/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Cleanup authority for newly created directories, never inferred from a path.
use super::super::runtime_files::{
    FLOWS_DIRECTORY_NAME, INSTANCES_DIRECTORY_NAME, SINK_DIRECTORY_NAME, SOURCE_DIRECTORY_NAME,
    create_directory_entry, create_private_directory, egress_queue_path,
    enforce_private_directory_permissions, flow_directory, instance_working_directory,
    loops_bell_path,
};
use crate::pipeline::reconfigure::PipelineReconfigureError;
use crate::pipeline::reconfigure::operation::{
    BlockingReconfigureJob, spawn_blocking_reconfigure_work,
};
use crate::pipeline::reconfigure::plan::{CompiledResourceChanges, FlowChange, ResourceMutation};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

pub(super) const CANDIDATE_DIRECTORY_NAME: &str = ".candidate";

/// A candidate either created the root or can delete only its own new children.
pub(super) enum ResourceDirectories {
    Root(OwnedDirectory),
    Additions {
        root: PathBuf,
        directories: Vec<OwnedDirectory>,
        candidate: Option<OwnedDirectory>,
    },
}

impl ResourceDirectories {
    /// Requires all retired Queue users and old processes to have actually joined.
    /// Cleanup authority stays with this owner until the caller joins the file job.
    #[allow(
        clippy::expect_used,
        reason = "the shared Queue layout always includes its Flow directory"
    )]
    pub(super) fn begin_install(
        &self,
        changes: &CompiledResourceChanges,
        current: &PipelineRevision,
    ) -> BlockingReconfigureJob<()> {
        let root = self.path().to_owned();
        let instances = root.join(INSTANCES_DIRECTORY_NAME);
        let old_sources: BTreeSet<_> = changes
            .flows
            .iter()
            .filter(|(_, change)| matches!(change, FlowChange::ReplaceQueues | FlowChange::Remove))
            .map(|(id, _)| {
                instance_working_directory(&instances, current.document().flows()[id].source())
                    .join(SOURCE_DIRECTORY_NAME)
            })
            .collect();
        let old_egress: Vec<_> = changes
            .egress_queues
            .iter()
            .filter(|queue| queue.mutation != ResourceMutation::Add)
            .map(|queue| {
                egress_queue_path(
                    &instance_working_directory(&instances, &queue.instance_id),
                    queue.flow_id.as_str(),
                    queue.channel_index,
                )
            })
            .collect();
        let removed_flow_directories: BTreeSet<_> = changes
            .egress_queues
            .iter()
            .filter(|queue| {
                queue.mutation == ResourceMutation::Remove
                    && !changes
                        .target
                        .document()
                        .flows()
                        .get(&queue.flow_id)
                        .is_some_and(|flow| flow.sinks().contains(&queue.instance_id))
            })
            .map(|queue| {
                egress_queue_path(
                    &instance_working_directory(&instances, &queue.instance_id),
                    queue.flow_id.as_str(),
                    queue.channel_index,
                )
                .parent()
                .expect("Queue has a parent")
                .to_owned()
            })
            .collect();
        let removed_sink_directories: Vec<_> = changes
            .sink_bells
            .iter()
            .filter(|(_, mutation)| **mutation == ResourceMutation::Remove)
            .map(|(id, _)| instance_working_directory(&instances, id).join(SINK_DIRECTORY_NAME))
            .collect();
        let removed_instances: Vec<_> = changes
            .processes
            .iter()
            .filter(|(_, mutation)| *mutation == ResourceMutation::Remove)
            .map(|(id, _)| instance_working_directory(&instances, id))
            .collect();
        let removed_shared_flow_directories: Vec<_> = changes
            .flow_bells
            .iter()
            .filter(|(_, mutation)| **mutation == ResourceMutation::Remove)
            .map(|(id, _)| flow_directory(&root, id.as_str()))
            .collect();
        let replaced_shared_flow_bells: Vec<_> = changes
            .flow_bells
            .iter()
            .filter(|(_, mutation)| **mutation == ResourceMutation::Replace)
            .map(|(id, _)| super::super::runtime_files::flow_channel_bell_path(&root, id.as_str()))
            .collect();
        let mut replacements: Vec<_> = changes
            .new_queue_flows()
            .map(|id| changes.target.document().flows()[id].source())
            .filter(|id| changes.process_mutation(id) != Some(ResourceMutation::Add))
            .map(|id| {
                (
                    instance_working_directory(&root.join(CANDIDATE_DIRECTORY_NAME), id)
                        .join(SOURCE_DIRECTORY_NAME),
                    instance_working_directory(&instances, id).join(SOURCE_DIRECTORY_NAME),
                )
            })
            .collect();
        let mut new_sink_directories = BTreeSet::new();
        let mut new_flow_directories = BTreeSet::new();
        for queue in &changes.egress_queues {
            if queue.mutation == ResourceMutation::Remove
                || changes.process_mutation(&queue.instance_id) == Some(ResourceMutation::Add)
            {
                continue;
            }
            let instance_directory = instance_working_directory(&instances, &queue.instance_id);
            let to = egress_queue_path(
                &instance_directory,
                queue.flow_id.as_str(),
                queue.channel_index,
            );
            let from = egress_queue_path(
                &instance_working_directory(
                    &root.join(CANDIDATE_DIRECTORY_NAME),
                    &queue.instance_id,
                ),
                queue.flow_id.as_str(),
                queue.channel_index,
            );
            if changes.sink_bells.get(&queue.instance_id) == Some(&ResourceMutation::Add) {
                new_sink_directories.insert(instance_directory.join(SINK_DIRECTORY_NAME));
            }
            if !current
                .document()
                .flows()
                .get(&queue.flow_id)
                .is_some_and(|flow| flow.sinks().contains(&queue.instance_id))
            {
                new_flow_directories
                    .insert(to.parent().expect("Queue has a Flow directory").to_owned());
            }
            replacements.push((from, to));
        }
        // Only a changed Channel count replaces a Flow Region. All users of
        // the old layout have joined before these files are installed.
        replacements.extend(
            changes
                .flow_bells
                .iter()
                .filter(|(_, mutation)| **mutation == ResourceMutation::Replace)
                .map(|(id, _)| {
                    (
                        super::super::runtime_files::flow_channel_bell_path(
                            &root.join(CANDIDATE_DIRECTORY_NAME),
                            id.as_str(),
                        ),
                        super::super::runtime_files::flow_channel_bell_path(&root, id.as_str()),
                    )
                }),
        );
        // An existing instance can gain a Sink interface. A process replacement
        // alone never replaces the Region used by retained Flow writers.
        replacements.extend(
            changes
                .sink_bells
                .iter()
                .filter(|(id, mutation)| {
                    **mutation == ResourceMutation::Add
                        && changes.process_mutation(id) != Some(ResourceMutation::Add)
                })
                .map(|(id, _)| {
                    (
                        loops_bell_path(
                            &instance_working_directory(&root.join(CANDIDATE_DIRECTORY_NAME), id)
                                .join(SINK_DIRECTORY_NAME),
                        ),
                        loops_bell_path(
                            &instance_working_directory(&instances, id).join(SINK_DIRECTORY_NAME),
                        ),
                    )
                }),
        );
        let candidate = match self {
            Self::Additions { candidate, .. } => candidate
                .as_ref()
                .map(|directory| directory.path().to_owned()),
            Self::Root(_) => unreachable!("Only a retained root has old interface files"),
        };
        spawn_blocking_reconfigure_work(None, move || {
            for path in old_egress.into_iter().chain(replaced_shared_flow_bells) {
                std::fs::remove_file(&path)
                    .map_err(|source| PipelineReconfigureError::QueueRemove { path, source })?;
            }
            for path in old_sources
                .into_iter()
                .chain(removed_flow_directories)
                .chain(removed_sink_directories)
                .chain(removed_instances)
                .chain(removed_shared_flow_directories)
            {
                remove_directory_tree(&path)?;
            }
            for path in new_sink_directories.into_iter().chain(new_flow_directories) {
                create_private_directory(&path)?;
            }
            for (from, to) in replacements {
                std::fs::rename(&from, &to)
                    .map_err(|source| PipelineReconfigureError::QueueRename { from, to, source })?;
            }
            if let Some(candidate) = candidate {
                remove_directory_tree(&candidate)?;
            }
            Ok(())
        })
    }

    /// The joined file job removed the now-empty candidate container successfully.
    pub(super) fn finish_install(&mut self) {
        if let Self::Additions { candidate, .. } = self
            && let Some(candidate) = candidate.take()
        {
            candidate.release();
        }
    }

    pub(super) fn create_root(path: &Path) -> Result<Self, PipelineReconfigureError> {
        let root = OwnedDirectory::create(path)?;
        create_private_directory(&path.join(INSTANCES_DIRECTORY_NAME))?;
        create_private_directory(&path.join(FLOWS_DIRECTORY_NAME))?;
        Ok(Self::Root(root))
    }

    pub(super) fn retain_root(path: &Path) -> Self {
        Self::Additions {
            root: path.to_owned(),
            directories: Vec::new(),
            candidate: None,
        }
    }

    pub(super) fn path(&self) -> &Path {
        match self {
            Self::Root(root) => root.path(),
            Self::Additions { root, .. } => root,
        }
    }

    pub(super) fn create_directory(&mut self, path: &Path) -> Result<(), PipelineReconfigureError> {
        let directory = OwnedDirectory::create(path)?;
        match self {
            Self::Root(_) => directory.release(),
            Self::Additions { directories, .. } => directories.push(directory),
        }
        Ok(())
    }

    /// Keeps replacement files outside every active Plugin working directory.
    pub(super) fn create_candidate(&mut self) -> Result<(), PipelineReconfigureError> {
        match self {
            Self::Additions {
                root, candidate, ..
            } => {
                assert!(
                    candidate.is_none(),
                    "One operation owns one candidate directory"
                );
                *candidate = Some(OwnedDirectory::create(
                    &root.join(CANDIDATE_DIRECTORY_NAME),
                )?);
                Ok(())
            }
            Self::Root(_) => unreachable!("Initial resources have no existing files to replace"),
        }
    }

    /// Transfers cleanup to the retained root after all workers have been adopted.
    pub(super) fn retain(&mut self, retained: Self) {
        match self {
            Self::Additions {
                directories,
                candidate,
                ..
            } => {
                assert!(
                    candidate.is_none(),
                    "Candidate interfaces must be installed before adoption"
                );
                for directory in directories.drain(..) {
                    directory.release();
                }
            }
            Self::Root(_) => unreachable!("a new root cannot coexist with a retained runtime"),
        }
        *self = retained;
    }

    pub(super) fn remove(self) -> Result<(), PipelineReconfigureError> {
        match self {
            Self::Root(root) => root.remove(),
            Self::Additions {
                directories,
                candidate,
                ..
            } => {
                for directory in directories {
                    directory.remove()?;
                }
                if let Some(candidate) = candidate {
                    candidate.remove()?;
                }
                Ok(())
            }
        }
    }
}

pub(super) struct OwnedDirectory {
    path: Option<PathBuf>,
}

impl OwnedDirectory {
    pub(super) fn create(path: &Path) -> Result<Self, PipelineReconfigureError> {
        Self::create_with_permissions(path, enforce_private_directory_permissions)
    }

    pub(super) fn create_with_permissions(
        path: &Path,
        enforce_permissions: impl FnOnce(&Path) -> Result<(), PipelineReconfigureError>,
    ) -> Result<Self, PipelineReconfigureError> {
        create_directory_entry(path)?;
        let working_directory = Self {
            path: Some(path.to_owned()),
        };
        enforce_permissions(path)?;
        Ok(working_directory)
    }

    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .unwrap_or_else(|| std::process::abort())
    }

    fn remove(mut self) -> Result<(), PipelineReconfigureError> {
        let path = self.path.take().unwrap_or_else(|| std::process::abort());
        remove_directory_tree(&path)
    }

    fn release(mut self) {
        self.path = None;
    }
}

impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        // Drop cannot return a filesystem failure. A stale candidate would
        // violate exclusive directory ownership and make retry ambiguous.
        if self
            .path
            .as_deref()
            .is_some_and(|path| remove_directory_tree(path).is_err())
        {
            std::process::abort();
        }
    }
}

fn remove_directory_tree(path: &Path) -> Result<(), PipelineReconfigureError> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(PipelineReconfigureError::DirectoryRemove {
            path: path.to_owned(),
            source,
        }),
    }
}
