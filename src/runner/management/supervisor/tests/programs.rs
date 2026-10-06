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

//! Program ownership handoff, request cancellation, and fatal error ordering.

use super::*;
use crate::identifiers::{ExactVersion, ProgramName};
use crate::payload_contract::PluginInterface;
use crate::runner::extensions::RunnerHooks;
use crate::runner::management::jobs::ManagementMutationJob;
use crate::runner::management::{PluginDeleteFailure, PutDocumentPrecondition};
use crate::runner::plugin::package::tests::valid_program_package;
use crate::runner::plugin::store::PluginProgramStore;
use crate::runner::recovery::recover;
use std::fs;
use std::future::{Future as _, poll_fn};
use std::io::Cursor;
use std::task::Poll;

#[tokio::test(flavor = "current_thread")]
async fn program_mutation_retains_the_store_across_cancelled_waits_and_clients() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut supervisor, management) = program_supervisor(directory.path())?;
    let source = ProgramName::try_from("com.example.source").map_err(io::Error::other)?;
    let sink = ProgramName::try_from("com.example.sink").map_err(io::Error::other)?;
    let version = ExactVersion::try_from("1.0.0").map_err(io::Error::other)?;
    let (response, cancelled_client) = oneshot::channel();
    let job = supervisor
        .state
        .start_mutation(RunnerManagementMutation::DeleteProgram {
            program_name: source,
            exact_version: version.clone(),
            response,
        })
        .ok_or_else(|| io::Error::other("Program deletion did not take ownership"))?;
    let ManagementMutationJob::DeleteProgram { programs, .. } = &job else {
        return Err(io::Error::other(
            "Program deletion produced a different job",
        ));
    };
    let sibling = Arc::clone(
        programs
            .lookup(&sink, &version)
            .ok_or_else(|| io::Error::other("Sibling Program is missing"))?,
    );
    let release = hold_mutation_before_filesystem_work(&mut supervisor, job);
    drop(cancelled_client);
    let (response, mut query) = oneshot::channel();
    management
        .queries
        .send(RunnerManagementQuery::ListPrograms {
            interface: None,
            response,
        })
        .await
        .map_err(|_| io::Error::other("Program query was not queued"))?;

    {
        let event = supervisor.next_event(|_| Ok(()));
        tokio::pin!(event);
        poll_fn(|context| {
            assert!(event.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert!(matches!(
        query.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert!(!supervisor.state.owns_program_store());
    let _ = release.send(());
    let RunnerManagementEvent::CommitReady(commit) = supervisor.next_event(|_| Ok(())).await else {
        return Err(io::Error::other(
            "Cancelled client lost the owned Program deletion",
        ));
    };
    commit.publish_after(|directives| {
        assert!(directives.is_empty());
        Ok::<_, io::Error>(())
    })?;
    {
        let event = supervisor.next_event(|_| Ok(()));
        tokio::pin!(event);
        tokio::select! {
            _ = &mut event => return Err(io::Error::other("Query produced a mutation event")),
            result = &mut query => {
                let entries = result.map_err(io::Error::other)?
                    .map_err(|_| io::Error::other("Store was not returned after deletion"))?;
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].program_name, sink);
            }
        }
    }

    let (response, in_use) = oneshot::channel();
    let job = supervisor
        .state
        .start_mutation(RunnerManagementMutation::DeleteProgram {
            program_name: sink,
            exact_version: version,
            response,
        })
        .ok_or_else(|| io::Error::other("Sibling deletion did not reach its Store gate"))?;
    let ManagementMutationJob::DeleteProgram { programs, .. } = &job else {
        return Err(io::Error::other(
            "Sibling deletion produced a different job",
        ));
    };
    assert!(
        programs
            .programs()
            .any(|(_, _, entry)| Arc::ptr_eq(entry, &sibling))
    );
    supervisor.mutation_task = Some(task::spawn_blocking(move || job.run()));
    let RunnerManagementEvent::CommitReady(commit) = supervisor.next_event(|_| Ok(())).await else {
        return Err(io::Error::other(
            "Live Entry reference became a fatal failure",
        ));
    };
    commit.publish_after(|_| Ok::<_, io::Error>(()))?;
    assert!(matches!(
        in_use.await,
        Ok(Ok(Err(PluginDeleteFailure::InUse { .. })))
    ));
    assert!(supervisor.state.owns_program_store());
    supervisor.shutdown().await.map_err(io::Error::other)?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn program_store_failure_rejects_queued_work_in_running_and_draining_modes() -> io::Result<()>
{
    for publication in [PipelinePublication::Open, PipelinePublication::Closed] {
        let directory = tempfile::tempdir()?;
        let (mut supervisor, management) = program_supervisor(directory.path())?;
        supervisor.pipeline_publication = publication;
        let source = ProgramName::try_from("com.example.source").map_err(io::Error::other)?;
        let sink = ProgramName::try_from("com.example.sink").map_err(io::Error::other)?;
        let version = ExactVersion::try_from("1.0.0").map_err(io::Error::other)?;
        let (response, failed_request) = oneshot::channel();
        let job = supervisor
            .state
            .start_mutation(RunnerManagementMutation::DeleteProgram {
                program_name: source,
                exact_version: version.clone(),
                response,
            })
            .ok_or_else(|| io::Error::other("Program deletion did not start"))?;
        let release = hold_mutation_before_filesystem_work(&mut supervisor, job);
        let target = directory
            .path()
            .join("plugins/programs/com.example.source/.tenon-artifact-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305");
        fs::rename(&target, directory.path().join("externally-moved-program"))?;

        let (response, queued_mutation) = oneshot::channel();
        management
            .mutations
            .send(RunnerManagementMutation::DeleteProgram {
                program_name: sink,
                exact_version: version,
                response,
            })
            .await
            .map_err(|_| io::Error::other("Second mutation was not queued"))?;
        let (response, queued_query) = oneshot::channel();
        management
            .queries
            .send(RunnerManagementQuery::ListPrograms {
                interface: None,
                response,
            })
            .await
            .map_err(|_| io::Error::other("Program query was not queued"))?;
        let (response, prepared_document) = oneshot::channel();
        let preparation = supervisor.state.prepare_document(
            RunnerManagementDocumentPreparation::PutDocument {
                id: TenonDocumentId::try_from("must-not-commit").map_err(io::Error::other)?,
                precondition: PutDocumentPrecondition::Create,
                source: document("must-not-commit").into_bytes().into_boxed_slice(),
                response,
            },
            Arc::clone(&supervisor.verifier),
        );
        let (prepared, is_prepared) = oneshot::channel();
        supervisor
            .document_preparations
            .push_back(task::spawn_blocking(move || {
                let result = preparation.run();
                let _ = prepared.send(());
                result
            }));
        is_prepared.await.map_err(io::Error::other)?;

        let _ = release.send(());
        let RunnerManagementEvent::Failure(RunnerManagementSupervisorError::ProgramStore(error)) =
            supervisor.next_event(|_| Ok(())).await
        else {
            return Err(io::Error::other(
                "Store failure did not end management service",
            ));
        };
        assert!(
            matches!(error, PluginStoreError::FilesystemOperationFailed { path, source }
            if path == target && source.kind() == io::ErrorKind::NotFound)
        );
        supervisor.shutdown().await.map_err(io::Error::other)?;
        assert!(failed_request.await.is_err());
        assert!(matches!(queued_query.await, Ok(Err(RunnerUnavailable))));
        assert!(matches!(queued_mutation.await, Ok(Err(RunnerUnavailable))));
        assert!(matches!(
            prepared_document.await,
            Ok(Err(RunnerUnavailable))
        ));
        assert!(management.list_programs(None).await.is_err());
        assert!(
            management
                .delete_document(
                    TenonDocumentId::try_from("after-failure").map_err(io::Error::other)?,
                    DeleteDocumentPrecondition::Missing,
                )
                .await
                .is_err()
        );
        assert!(
            directory
                .path()
                .join("tenon-documents")
                .read_dir()?
                .next()
                .is_none()
        );
        assert!(
            directory
                .path()
                .join("plugins/programs/com.example.sink/.tenon-artifact-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305")
                .is_file()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_propagates_started_program_failure_and_joins_other_work() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut supervisor, management) = program_supervisor(directory.path())?;
    let (response, failed_request) = oneshot::channel();
    let job = supervisor
        .state
        .start_mutation(RunnerManagementMutation::DeleteProgram {
            program_name: ProgramName::try_from("com.example.source").map_err(io::Error::other)?,
            exact_version: ExactVersion::try_from("1.0.0").map_err(io::Error::other)?,
            response,
        })
        .ok_or_else(|| io::Error::other("Program deletion did not start"))?;
    let release = hold_mutation_before_filesystem_work(&mut supervisor, job);
    fs::rename(
        directory
            .path()
            .join("plugins/programs/com.example.source/.tenon-artifact-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305"),
        directory.path().join("externally-moved-program"),
    )?;
    let (response, query) = oneshot::channel();
    management
        .queries
        .send(RunnerManagementQuery::ListPrograms {
            interface: None,
            response,
        })
        .await
        .map_err(|_| io::Error::other("Program query was not queued"))?;
    let (release_preparation, preparation_released) = oneshot::channel();
    let (response, preparation_result) = oneshot::channel();
    let preparation = supervisor.state.prepare_document(
        RunnerManagementDocumentPreparation::PutDocument {
            id: TenonDocumentId::try_from("must-not-commit").map_err(io::Error::other)?,
            precondition: PutDocumentPrecondition::Create,
            source: document("must-not-commit").into_bytes().into_boxed_slice(),
            response,
        },
        Arc::clone(&supervisor.verifier),
    );
    supervisor
        .document_preparations
        .push_back(task::spawn_blocking(move || {
            let _ = preparation_released.blocking_recv();
            preparation.run()
        }));

    let shutdown = supervisor.shutdown();
    tokio::pin!(shutdown);
    tokio::select! {
        _ = &mut shutdown => return Err(io::Error::other("Shutdown abandoned a started mutation")),
        result = query => assert!(matches!(result, Ok(Err(RunnerUnavailable)))),
    }
    let _ = release.send(());
    assert!(failed_request.await.is_err());
    poll_fn(|context| {
        assert!(shutdown.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    let _ = release_preparation.send(());
    assert!(matches!(
        shutdown.await,
        Err(RunnerManagementSupervisorError::ProgramStore(
            PluginStoreError::FilesystemOperationFailed { .. }
        ))
    ));
    assert!(matches!(
        preparation_result.await,
        Ok(Err(RunnerUnavailable))
    ));
    assert!(management.list_programs(None).await.is_err());
    Ok(())
}

pub(super) fn program_supervisor(
    directory: &Path,
) -> io::Result<(RunnerManagementSupervisor, RunnerManagementClient)> {
    let layout = prepare_runner_state_directory(directory).map_err(io::Error::other)?;
    {
        let mut programs = PluginProgramStore::recover(
            layout.plugin_program_store_directory(),
            layout.plugin_program_store_directory(),
            std::sync::Arc::new(crate::runner::extensions::ByPass),
        )
        .map_err(io::Error::other)?;
        for interface in [PluginInterface::Source, PluginInterface::Sink] {
            programs
                .install(Cursor::new(valid_program_package(interface)?))
                .map_err(io::Error::other)?;
        }
    }
    let config = load_config(directory)?;
    let recovered = recover(
        &config,
        &layout,
        &RunnerHooks::default().artifact_protection,
        &layout.pipeline_runtime_directory(),
    )
    .map_err(io::Error::other)?;
    let (state, directives) = RunnerManagementState::recover(
        directory,
        config.script_vm_limits(),
        recovered,
        RunnerHooks::default().artifact_protection,
        None,
        |_| Ok(()),
    )?;
    assert!(directives.is_empty());
    Ok(test_support::supervisor(
        state,
        config.tenon_document_verifier().map_err(io::Error::other)?,
    ))
}

pub(super) fn hold_mutation_before_filesystem_work(
    supervisor: &mut RunnerManagementSupervisor,
    job: ManagementMutationJob,
) -> oneshot::Sender<()> {
    let (release, released) = oneshot::channel();
    supervisor.mutation_task = Some(task::spawn_blocking(move || {
        let _ = released.blocking_recv();
        job.run()
    }));
    release
}
