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

//! Creates candidate Queue files and Bell Regions and binds their endpoints.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::contracts::core::PipelineEnvironment;
use crate::identifiers::{FlowId, PluginInstanceId, SinkContractId};
use crate::pipeline::channel::{
    FlowChannelBells, FlowChannelQueuePaths, PreparedEgressQueue, PreparedEgressRoutes,
};
use crate::pipeline::ingress_queue::{
    COMPLETION_MAX_PAYLOAD_SIZE, completion_capacity, submission_capacity,
};
use crate::pipeline::reconfigure::PipelineReconfigureError;
use crate::pipeline::reconfigure::plan::{CompiledResourceChanges, FlowChange, ResourceMutation};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::reconfigure::runtime_files::{
    INSTANCES_DIRECTORY_NAME, SINK_DIRECTORY_NAME, create_bell_region_file,
    create_private_directory, create_queue, egress_queue_path, ensure_private_directory,
    flow_channel_bell_path, flow_directory, instance_working_directory, loops_bell_path,
};
use crate::pipeline::runtime::ChannelRuntimeSpec;
use tenon_ipc::bell::{BellRegion, read_bell_region_epoch};
use tenon_ipc::queue::QueueWriter;

use super::directories::{CANDIDATE_DIRECTORY_NAME, ResourceDirectories};

/// The candidate Queue files and Bell Regions of one resource batch.
///
/// The Bell Regions never travel past this module: binding a Queue endpoint
/// consumes the region, and every later waiter or ringer reaches the same
/// mapping through the endpoint it already holds.
pub(super) struct PreparedQueueLayout {
    pub(super) channel_routes: BTreeMap<FlowId, Vec<PreparedEgressRoutes>>,
    /// The Channel doorbell region of every Flow whose Queues this batch creates.
    pub(super) flow_bells: BTreeMap<FlowId, Arc<BellRegion>>,
}

/// Creates candidate directories and Queue files, then returns every target route.
pub(super) fn prepare_routes(
    changes: &CompiledResourceChanges,
    environment: &PipelineEnvironment,
    working_directory: &mut ResourceDirectories,
) -> Result<PreparedQueueLayout, PipelineReconfigureError> {
    let root = working_directory.path().to_owned();
    stage_instance_directories(changes, working_directory, &root)?;
    let flow_bells = channel_bell_regions(changes, environment, &root)?;
    let sink_bells = create_sink_bell_regions(changes, &root)?;
    let channel_routes =
        prepare_egress_routes(changes, environment, &root, &flow_bells, &sink_bells)?;
    Ok(PreparedQueueLayout {
        channel_routes,
        flow_bells,
    })
}

pub(super) fn open_bell_region(path: &Path) -> Result<Arc<BellRegion>, PipelineReconfigureError> {
    BellRegion::open(path).map_err(|source| PipelineReconfigureError::BellRegionOpen {
        path: path.to_owned(),
        source,
    })
}

/// Opens retained Regions and creates only the Regions selected by the plan.
fn channel_bell_regions(
    changes: &CompiledResourceChanges,
    environment: &PipelineEnvironment,
    root: &Path,
) -> Result<BTreeMap<FlowId, Arc<BellRegion>>, PipelineReconfigureError> {
    let mut regions = BTreeMap::new();
    for flow_id in changes.new_queue_flows() {
        let live_path = flow_channel_bell_path(root, flow_id.as_str());
        let path = match changes.flow_bells.get(flow_id) {
            Some(mutation @ (ResourceMutation::Add | ResourceMutation::Replace)) => {
                let replacing = *mutation == ResourceMutation::Replace;
                let region_root = if replacing {
                    root.join(CANDIDATE_DIRECTORY_NAME)
                } else {
                    root.to_owned()
                };
                if replacing {
                    ensure_private_directory(
                        &region_root.join(super::super::runtime_files::FLOWS_DIRECTORY_NAME),
                    )?;
                    create_private_directory(&flow_directory(&region_root, flow_id.as_str()))?;
                }
                let path = flow_channel_bell_path(&region_root, flow_id.as_str());
                let slots = changes
                    .target
                    .document()
                    .channel_count(flow_id, environment.available_cpu_count());
                let epoch = if replacing {
                    read_bell_region_epoch(&live_path)
                        .unwrap_or(0)
                        .saturating_add(1)
                } else {
                    1
                };
                create_bell_region_file(&path, slots, epoch)?;
                path
            }
            None => live_path,
            Some(ResourceMutation::Remove) => unreachable!("a removed Flow has no new Queues"),
        };
        regions.insert(flow_id.clone(), open_bell_region(&path)?);
    }
    for queue in &changes.egress_queues {
        if queue.mutation == ResourceMutation::Remove || regions.contains_key(&queue.flow_id) {
            continue;
        }
        let path = flow_channel_bell_path(root, queue.flow_id.as_str());
        regions.insert(queue.flow_id.clone(), open_bell_region(&path)?);
    }
    Ok(regions)
}

/// Returns every Sink Instance this batch binds Egress to.
///
/// Routes exist for every Flow this batch does not remove, so this is exactly
/// the set whose own-loop region a retained Channel may have to ring.
pub(super) fn batch_sink_instances(
    changes: &CompiledResourceChanges,
) -> BTreeSet<PluginInstanceId> {
    changes
        .flows
        .iter()
        .filter(|(_, change)| **change != FlowChange::Remove)
        .flat_map(|(flow_id, _)| changes.target.document().flows()[flow_id].sinks())
        .cloned()
        .collect()
}

/// A process replacement keeps the Sink Region that its Flow writers still use.
fn create_sink_bell_regions(
    changes: &CompiledResourceChanges,
    root: &Path,
) -> Result<BTreeMap<PluginInstanceId, Arc<BellRegion>>, PipelineReconfigureError> {
    let mut regions = BTreeMap::new();
    for instance_id in batch_sink_instances(changes) {
        let path = match changes.sink_bells.get(&instance_id) {
            Some(ResourceMutation::Add) => {
                let directory = staged_instance_directory(changes, root, &instance_id)
                    .join(SINK_DIRECTORY_NAME);
                create_private_directory(&directory)?;
                let path = loops_bell_path(&directory);
                create_bell_region_file(&path, SINK_SLOT_COUNT, 1)?;
                path
            }
            None => loops_bell_path(
                &instance_working_directory(&root.join(INSTANCES_DIRECTORY_NAME), &instance_id)
                    .join(SINK_DIRECTORY_NAME),
            ),
            Some(ResourceMutation::Replace | ResourceMutation::Remove) => {
                unreachable!("a target Sink keeps its single-slot Region until removal")
            }
        };
        regions.insert(instance_id, open_bell_region(&path)?);
    }
    Ok(regions)
}

/// One Sink Instance serves every Egress Queue it reads from one loop, so its
/// own-loop region holds exactly the one doorbell that loop parks on.
const SINK_SLOT_COUNT: NonZeroU32 = NonZeroU32::MIN;

/// Creates the live instance directories, and per-instance candidate directories
/// for existing instances that gain new files. Cleanup authority stays in
/// `working_directory`.
fn stage_instance_directories(
    changes: &CompiledResourceChanges,
    working_directory: &mut ResourceDirectories,
    root: &Path,
) -> Result<(), PipelineReconfigureError> {
    let target = &changes.target;
    let instances_directory = root.join(INSTANCES_DIRECTORY_NAME);
    for instance_id in changes.added_instances() {
        working_directory.create_directory(&instance_working_directory(
            &instances_directory,
            instance_id,
        ))?;
    }
    for (flow_id, mutation) in &changes.flow_bells {
        if *mutation == ResourceMutation::Add {
            working_directory.create_directory(&flow_directory(root, flow_id.as_str()))?;
        }
    }
    // Existing instances keep candidate files outside their active directories.
    let new_file_instances: BTreeSet<_> = changes
        .new_queue_flows()
        .map(|id| target.document().flows()[id].source().clone())
        .chain(
            changes
                .egress_queues
                .iter()
                .filter(|queue| queue.mutation != ResourceMutation::Remove)
                .map(|queue| queue.instance_id.clone()),
        )
        .filter(|id| changes.process_mutation(id) != Some(ResourceMutation::Add))
        .collect();
    if !new_file_instances.is_empty()
        || changes
            .flow_bells
            .values()
            .any(|mutation| *mutation == ResourceMutation::Replace)
    {
        working_directory.create_candidate()?;
        for instance_id in new_file_instances {
            create_private_directory(&instance_working_directory(
                &root.join(CANDIDATE_DIRECTORY_NAME),
                &instance_id,
            ))?;
        }
    }
    Ok(())
}

/// Creates every new Egress Queue and assembles the per-Flow routes. New writers
/// move into the result; retained Queues remain unopened until their old writer exits.
#[allow(
    clippy::expect_used,
    reason = "the compiled plan and Queue layout establish these internal identities"
)]
fn prepare_egress_routes(
    changes: &CompiledResourceChanges,
    environment: &PipelineEnvironment,
    root: &Path,
    flow_bells: &BTreeMap<FlowId, Arc<BellRegion>>,
    sink_bells: &BTreeMap<PluginInstanceId, Arc<BellRegion>>,
) -> Result<BTreeMap<FlowId, Vec<PreparedEgressRoutes>>, PipelineReconfigureError> {
    let target = &changes.target;
    let instances_directory = root.join(INSTANCES_DIRECTORY_NAME);

    let mut egress_targets = BTreeMap::new();
    let mut created_directories = BTreeSet::new();
    for queue in &changes.egress_queues {
        if queue.mutation == ResourceMutation::Remove {
            continue;
        }
        let flow = &target.document().flows()[&queue.flow_id];
        let record_queue_capacity =
            submission_capacity(flow.max_pending_records(), flow.max_record_bytes())
                .expect("Runner verifies every Flow's Queue capacity");
        let instance_directory = staged_instance_directory(changes, root, &queue.instance_id);
        let queue_path = egress_queue_path(
            &instance_directory,
            queue.flow_id.as_str(),
            queue.channel_index,
        );
        let flow_directory = queue_path
            .parent()
            .expect("Queue layout has a Flow directory");
        for directory in [
            instance_directory.join(SINK_DIRECTORY_NAME),
            flow_directory.to_owned(),
        ] {
            if created_directories.insert(directory.clone()) {
                ensure_private_directory(&directory)?;
            }
        }
        create_queue(&queue_path, record_queue_capacity, flow.max_record_bytes())?;
        // The Channel's own slot carries no cause: every fact the Channel waits
        // on reaches this one address, and the retained Source that releases the
        // Completion of this Channel reads the same slot from its Queue header.
        let bell = flow_bells[&queue.flow_id]
            .loop_bell(queue.channel_index)
            .expect("one Channel slot exists per Channel of its Flow");
        let writer = QueueWriter::open(
            &queue_path,
            bell,
            Arc::clone(&sink_bells[&queue.instance_id]),
        )
        .map_err(|source| PipelineReconfigureError::QueueOpen {
            path: queue_path,
            source,
        })?;
        egress_targets.insert(
            (
                queue.instance_id.clone(),
                queue.flow_id.clone(),
                queue.channel_index,
            ),
            writer,
        );
    }

    let mut channel_routes = BTreeMap::new();
    for (flow_id, change) in &changes.flows {
        if *change == FlowChange::Remove {
            continue;
        }
        let bindings = flow_egress_bindings(target, flow_id);
        let mut channels = Vec::new();
        for channel_index in 0..target
            .document()
            .channel_count(flow_id, environment.available_cpu_count())
            .get()
        {
            let mut routes = HashMap::new();
            for (contract, instances) in &bindings {
                let mut targets = BTreeMap::new();
                for id in instances {
                    let material = match egress_targets.remove(&(
                        id.clone(),
                        flow_id.clone(),
                        channel_index,
                    )) {
                        Some(writer) => PreparedEgressQueue::New(writer),
                        // A replacement writer opens the retained Queue after its
                        // previous writer exits. Both use the retained Sink Region.
                        None => PreparedEgressQueue::Retained {
                            path: egress_queue_path(
                                &instance_working_directory(&instances_directory, id),
                                flow_id.as_str(),
                                channel_index,
                            ),
                            peer_region: Arc::clone(&sink_bells[id]),
                        },
                    };
                    targets.insert(id.clone(), material);
                }
                routes.insert(contract.clone(), targets);
            }
            channels.push(routes);
        }
        channel_routes.insert(flow_id.clone(), channels);
    }
    assert!(
        egress_targets.is_empty(),
        "every new Queue is assigned to its exact Channel"
    );
    Ok(channel_routes)
}

/// Creates one Source side's Queue pair per Channel and the doorbell region its
/// own loop parks on.
///
/// The Source's own region holds one slot per waiting loop, not per Channel:
/// every Submission Queue publishes the Submission loop's slot and every
/// Completion Queue publishes the Completion loop's, whatever the Channel count
/// is. `channel_region` is the Flow's Channel region; each Channel waits on its
/// own slot there, and this side only rings it.
#[expect(
    clippy::expect_used,
    reason = "the fixed Completion payload size is nonzero"
)]
pub(super) fn create_source_channels(
    source_directory: &Path,
    max_pending_records: NonZeroU64,
    max_record_bytes: NonZeroU64,
    routes: Vec<PreparedEgressRoutes>,
    channel_region: &Arc<BellRegion>,
) -> Result<Vec<ChannelRuntimeSpec>, PipelineReconfigureError> {
    ensure_private_directory(source_directory)?;
    let mut channels = Vec::new();
    channels
        .try_reserve_exact(routes.len())
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    let source_slot_count =
        NonZeroU32::new(SOURCE_LOOP_SLOT_COUNT).expect("the Source owns one slot per waiting loop");
    let source_bell_path = loops_bell_path(source_directory);
    // The Source Region and all its Queue pairs have the same lifetime.
    create_bell_region_file(&source_bell_path, source_slot_count, 1)?;
    let source_region = open_bell_region(&source_bell_path)?;
    let submission_capacity = submission_capacity(max_pending_records, max_record_bytes)
        .expect("Runner verifies every Flow's Queue capacity");
    let completion_capacity = completion_capacity(max_pending_records)
        .expect("Completion frames are smaller than the verified record frames");
    let completion_max_payload = NonZeroU64::new(COMPLETION_MAX_PAYLOAD_SIZE as u64)
        .expect("The fixed Completion payload size is nonzero");
    for (index, routes) in routes.into_iter().enumerate() {
        let submission = source_directory.join(format!("submission-{index}.queue"));
        let completion = source_directory.join(format!("completion-{index}.queue"));
        create_queue(&submission, submission_capacity, max_record_bytes)?;
        create_queue(&completion, completion_capacity, completion_max_payload)?;
        let channel_index =
            u32::try_from(index).map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
        let channel_bell = channel_region
            .loop_bell(channel_index)
            .expect("the Channel region holds one slot per Channel");
        channels.push(ChannelRuntimeSpec::new(
            FlowChannelQueuePaths::new(submission, completion),
            FlowChannelBells::new(
                // The Channel waits on its own slot of its Flow's Channel
                // region; both of its Ingress endpoints publish that slot into
                // their headers, so the Source rings this one address.
                channel_bell,
                // Committing a Completion or releasing a Submission frame rings
                // the slot the Source published in its own loop region. One slot
                // belongs to the Submission loop and one to the Completion loop,
                // so a ring always reaches the loop that waits on that role.
                Arc::clone(&source_region),
            ),
            routes,
        ));
    }
    Ok(channels)
}

/// The Source owns one slot per waiting loop, independent of the Channel count.
const SOURCE_LOOP_SLOT_COUNT: u32 = 2;

pub(super) fn flow_egress_bindings(
    target: &PipelineRevision,
    flow_id: &FlowId,
) -> HashMap<SinkContractId, Vec<PluginInstanceId>> {
    let mut bindings: HashMap<_, Vec<_>> = HashMap::new();
    for id in target.document().flows()[flow_id].sinks() {
        let instance = &target.document().plugin_instances()[id];
        let contract = SinkContractId::from_parts(
            instance.program_name().clone(),
            instance.exact_version().clone(),
        );
        bindings.entry(contract).or_default().push(id.clone());
    }
    for ids in bindings.values_mut() {
        ids.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    }
    bindings
}

pub(super) fn staged_instance_directory(
    changes: &CompiledResourceChanges,
    root: &Path,
    instance_id: &PluginInstanceId,
) -> PathBuf {
    let parent = if changes.process_mutation(instance_id) == Some(ResourceMutation::Add) {
        root.join(INSTANCES_DIRECTORY_NAME)
    } else {
        root.join(CANDIDATE_DIRECTORY_NAME)
    };
    instance_working_directory(&parent, instance_id)
}
