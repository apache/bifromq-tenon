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

//! Lifecycle owner for all Runner management work.
//!
//! The supervisor is the only scheduler: bounded side-effect-free CPU jobs
//! can run together, and durable mutations commit one at a time. Reads wait
//! in their bounded queue while a Program mutation owns the Store. Shutdown
//! rejects work that never started and joins every started blocking job.

use super::jobs::{ManagementDocumentPreparationResult, ManagementMutationResult};
use super::state::{RunnerManagementCommit, RunnerManagementState};
use super::{
    PipelineDirective, PipelineLifecycleRole, PipelinePublication, PipelineStateUpdate,
    RunnerManagementClient, RunnerManagementDocumentPreparation, RunnerManagementMutation,
    RunnerManagementQuery,
};
use crate::config::ScriptVmLimits;
use crate::runner::extensions::{ArtifactProtection, ExecutionDenied, ExecutionScope};
use crate::runner::plugin::store::PluginStoreError;
use crate::runner::recovery::RecoveredRunnerState;
use crate::tenon_document::verified::TenonDocumentVerifier;
use opentelemetry::metrics::Meter;
use std::collections::VecDeque;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::{fmt, io};
use tokio::sync::mpsc;
use tokio::task::{self, JoinError, JoinHandle};

const MAX_DOCUMENT_PREPARATIONS: usize = 8;
const MANAGEMENT_QUEUE_CAPACITY: usize = 32;

/// The single command receiver consumed by the management supervisor.
pub(crate) struct RunnerManagementReceiver {
    queries: mpsc::Receiver<RunnerManagementQuery>,
    document_preparations: mpsc::Receiver<RunnerManagementDocumentPreparation>,
    mutations: mpsc::Receiver<RunnerManagementMutation>,
}

/// Runtime-Integrity-checked facts that have not opened a management interface.
pub(crate) struct RecoveredRunnerManagement {
    state: RunnerManagementState,
    initial_pipeline_directives: Box<[PipelineDirective]>,
}

impl RecoveredRunnerManagement {
    pub(crate) fn check_execution(
        &self,
        authorize: impl FnOnce(ExecutionScope<'_>) -> Result<(), ExecutionDenied>,
    ) -> Result<(), ExecutionDenied> {
        self.state.check_execution(authorize)
    }
}

/// Owns all scheduling around the Runner's mutable management facts.
pub(crate) struct RunnerManagementSupervisor {
    state: RunnerManagementState,
    receiver: RunnerManagementReceiver,
    verifier: Arc<TenonDocumentVerifier>,
    mutation_task: Option<JoinHandle<Result<ManagementMutationResult, PluginStoreError>>>,
    document_preparations: VecDeque<JoinHandle<ManagementDocumentPreparationResult>>,
    pipeline_publication: PipelinePublication,
    // A policy notification arrived during this mutation. Clear after completion.
    recheck_after_mutation: bool,
}

impl RunnerManagementSupervisor {
    pub(crate) fn check_execution(
        &mut self,
        authorize: impl FnOnce(ExecutionScope<'_>) -> Result<(), ExecutionDenied>,
    ) -> Result<(), ExecutionDenied> {
        self.recheck_after_mutation |= self.mutation_task.is_some();
        self.state.check_execution(authorize)
    }
    /// Restores management facts without opening their command interface.
    pub(crate) fn recover(
        state_directory: &Path,
        script_vm_limits: ScriptVmLimits,
        recovered: RecoveredRunnerState,
        artifact_protection: Arc<dyn ArtifactProtection>,
        metrics: Option<&Meter>,
        authorize: impl FnMut(ExecutionScope<'_>) -> Result<(), ExecutionDenied>,
    ) -> io::Result<RecoveredRunnerManagement> {
        let (state, initial_directives) = RunnerManagementState::recover(
            state_directory,
            script_vm_limits,
            recovered,
            artifact_protection,
            metrics,
            authorize,
        )?;
        Ok(RecoveredRunnerManagement {
            state,
            initial_pipeline_directives: initial_directives,
        })
    }

    /// Opens the only command interface after the private Control Server exists.
    pub(crate) fn start(
        recovered: RecoveredRunnerManagement,
        verifier: TenonDocumentVerifier,
    ) -> (Self, RunnerManagementClient, Box<[PipelineDirective]>) {
        let (management, receiver) = management_channel();
        (
            Self {
                state: recovered.state,
                receiver,
                verifier: Arc::new(verifier),
                mutation_task: None,
                document_preparations: VecDeque::new(),
                pipeline_publication: PipelinePublication::Open,
                recheck_after_mutation: false,
            },
            management,
            recovered.initial_pipeline_directives,
        )
    }

    /// Waits until one complete mutation transition or terminal failure is ready.
    ///
    /// A queued mutation starts only when this method is polled after the
    /// previous transition was returned to the caller. A terminal failure
    /// hands control to Runner shutdown; the caller must not resume service.
    pub(crate) async fn next_event(
        &mut self,
        mut authorize: impl FnMut(ExecutionScope<'_>) -> Result<(), ExecutionDenied>,
    ) -> RunnerManagementEvent {
        loop {
            tokio::select! {
                result = wait_for_mutation(&mut self.mutation_task), if self.mutation_task.is_some() => {
                    self.mutation_task = None;
                    let recheck = std::mem::take(&mut self.recheck_after_mutation)
                        && self.pipeline_publication == PipelinePublication::Open
                        && matches!(&result, Ok(Ok(ManagementMutationResult::PutDocument { result: Ok(_), .. })));
                    match self.complete_mutation(result) {
                        Ok(Some(commit)) => {
                            if recheck && let Err(source) = self.state.check_execution(&mut authorize) {
                                return RunnerManagementEvent::Failure(RunnerManagementSupervisorError::ExecutionDenied(source));
                            }
                            return RunnerManagementEvent::CommitReady(commit);
                        }
                        Ok(None) => {}
                        Err(source) => return RunnerManagementEvent::Failure(source),
                    }
                }
                result = wait_for_document_preparation(&mut self.document_preparations),
                    if self.mutation_task.is_none() && !self.document_preparations.is_empty() =>
                {
                    match result {
                        Ok(prepared) => {
                            self.mutation_task = self.state.start_document_commit(prepared, &mut authorize)
                                .map(|job| task::spawn_blocking(move || job.run()));
                        }
                        Err(source) => return RunnerManagementEvent::Failure(
                            RunnerManagementSupervisorError::Task(source),
                        ),
                    }
                }
                command = self.receiver.queries.recv(), if self.state.owns_program_store() => {
                    let Some(command) = command else {
                        return RunnerManagementEvent::Failure(
                            RunnerManagementSupervisorError::InterfaceClosed,
                        );
                    };
                    self.state.handle_query(command);
                }
                command = self.receiver.document_preparations.recv(),
                    if self.document_preparations.len() < MAX_DOCUMENT_PREPARATIONS =>
                {
                    let Some(command) = command else {
                        return RunnerManagementEvent::Failure(
                            RunnerManagementSupervisorError::InterfaceClosed,
                        );
                    };
                    let job = self.state.prepare_document(
                        command,
                        Arc::clone(&self.verifier),
                    );
                    self.document_preparations
                        .push_back(task::spawn_blocking(move || job.run()));
                }
                command = self.receiver.mutations.recv(), if self.mutation_task.is_none() => {
                    let Some(command) = command else {
                        return RunnerManagementEvent::Failure(
                            RunnerManagementSupervisorError::InterfaceClosed,
                        );
                    };
                    self.mutation_task = self
                        .state
                        .start_mutation(command)
                        .map(|job| task::spawn_blocking(move || job.run()));
                }
            }
        }
    }

    /// Closes admission, rejects work that has not started, and finishes the
    /// one Store operation that may already own an external side effect.
    pub(crate) async fn shutdown(&mut self) -> Result<(), RunnerManagementSupervisorError> {
        self.close_pipeline_publication();
        self.close_admission();
        self.reject_queued();
        let mutation_failure = self.join_mutation().await;
        let preparation_failure = self.join_preparations().await;
        match mutation_failure.or(preparation_failure) {
            Some(source) => Err(source),
            None => Ok(()),
        }
    }

    /// Stops accepting new work on every management queue.
    fn close_admission(&mut self) {
        self.receiver.queries.close();
        self.receiver.document_preparations.close();
        self.receiver.mutations.close();
    }

    /// Rejects requests that were queued but never started a blocking job.
    fn reject_queued(&mut self) {
        while let Ok(request) = self.receiver.queries.try_recv() {
            request.reject_shutting_down();
        }
        while let Ok(request) = self.receiver.mutations.try_recv() {
            request.reject_shutting_down();
        }
        while let Ok(request) = self.receiver.document_preparations.try_recv() {
            request.reject_shutting_down();
        }
    }

    /// Finishes the one mutation that may already own an external side effect.
    async fn join_mutation(&mut self) -> Option<RunnerManagementSupervisorError> {
        self.mutation_task.as_ref()?;
        let result = wait_for_mutation(&mut self.mutation_task).await;
        self.mutation_task = None;
        self.recheck_after_mutation = false;
        self.complete_mutation(result).err()
    }

    /// Joins every started preparation, rejecting its queued response.
    async fn join_preparations(&mut self) -> Option<RunnerManagementSupervisorError> {
        let mut first_failure = None;
        while let Some(task) = self.document_preparations.pop_front() {
            match task.await {
                Ok(result) => result.reject_shutting_down(),
                Err(source) if first_failure.is_none() => {
                    first_failure = Some(RunnerManagementSupervisorError::Task(source));
                }
                Err(_) => {}
            }
        }
        first_failure
    }

    pub(crate) fn record_pipeline_state(
        &mut self,
        update: PipelineStateUpdate,
        role: PipelineLifecycleRole,
    ) {
        self.state.record_pipeline_state(update, role);
    }

    /// Prevents durable mutations from publishing new Pipeline work.
    pub(crate) fn close_pipeline_publication(&mut self) {
        self.pipeline_publication = PipelinePublication::Closed;
    }

    fn complete_mutation(
        &mut self,
        result: Result<Result<ManagementMutationResult, PluginStoreError>, JoinError>,
    ) -> Result<Option<RunnerManagementCommit>, RunnerManagementSupervisorError> {
        let mutation = result
            .map_err(RunnerManagementSupervisorError::Task)?
            .map_err(RunnerManagementSupervisorError::ProgramStore)?;
        Ok(self
            .state
            .complete_mutation(mutation, self.pipeline_publication))
    }
}

fn management_channel() -> (RunnerManagementClient, RunnerManagementReceiver) {
    let (queries, query_receiver) = mpsc::channel(MANAGEMENT_QUEUE_CAPACITY);
    let (document_preparations, document_preparation_receiver) =
        mpsc::channel(MANAGEMENT_QUEUE_CAPACITY);
    let (mutations, mutation_receiver) = mpsc::channel(MANAGEMENT_QUEUE_CAPACITY);
    (
        RunnerManagementClient {
            queries,
            document_preparations,
            mutations,
        },
        RunnerManagementReceiver {
            queries: query_receiver,
            document_preparations: document_preparation_receiver,
            mutations: mutation_receiver,
        },
    )
}

#[allow(
    clippy::expect_used,
    reason = "the select guard polls this future only while a mutation task exists"
)]
async fn wait_for_mutation(
    task: &mut Option<JoinHandle<Result<ManagementMutationResult, PluginStoreError>>>,
) -> Result<Result<ManagementMutationResult, PluginStoreError>, JoinError> {
    task.as_mut()
        .expect("an active mutation wait must retain its task")
        .await
}

#[allow(
    clippy::expect_used,
    reason = "the select guard polls only a nonempty ordered preparation queue"
)]
async fn wait_for_document_preparation(
    tasks: &mut VecDeque<JoinHandle<ManagementDocumentPreparationResult>>,
) -> Result<ManagementDocumentPreparationResult, JoinError> {
    let result = tasks
        .front_mut()
        .expect("an active document preparation wait must retain its task")
        .await;
    drop(
        tasks
            .pop_front()
            .expect("completed document preparation must remain queued"),
    );
    result
}

pub(crate) enum RunnerManagementEvent {
    CommitReady(RunnerManagementCommit),
    Failure(RunnerManagementSupervisorError),
}

#[derive(Debug)]
pub(crate) enum RunnerManagementSupervisorError {
    Task(JoinError),
    /// Preserves the Store failure that requires the whole Runner to stop.
    ProgramStore(PluginStoreError),
    ExecutionDenied(ExecutionDenied),
    InterfaceClosed,
}

impl fmt::Display for RunnerManagementSupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionDenied(_) => formatter.write_str("Runner execution is not permitted"),
            Self::Task(_) => formatter.write_str("Runner management task failed"),
            Self::ProgramStore(_) => formatter.write_str("Runner Plugin Program Store failed"),
            Self::InterfaceClosed => formatter.write_str("Runner management interface closed"),
        }
    }
}

impl Error for RunnerManagementSupervisorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ExecutionDenied(source) => Some(source),
            Self::Task(source) => Some(source),
            Self::ProgramStore(source) => Some(source),
            Self::InterfaceClosed => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn interface() -> (RunnerManagementClient, RunnerManagementReceiver) {
        management_channel()
    }

    pub(crate) fn supervisor(
        state: RunnerManagementState,
        verifier: TenonDocumentVerifier,
    ) -> (RunnerManagementSupervisor, RunnerManagementClient) {
        let (management, receiver) = management_channel();
        (
            RunnerManagementSupervisor {
                state,
                receiver,
                verifier: Arc::new(verifier),
                mutation_task: None,
                document_preparations: VecDeque::new(),
                pipeline_publication: PipelinePublication::Open,
                recheck_after_mutation: false,
            },
            management,
        )
    }
}

#[cfg(test)]
mod tests {
    mod policy_changes;
    mod programs;
    mod uploads;

    use super::*;
    use crate::identifiers::TenonDocumentId;
    use crate::runner::management::test_support::empty_state;
    use crate::runner::management::{
        DeleteDocumentPrecondition, PutDocumentPrecondition, RunnerManagementMutation,
        RunnerUnavailable,
    };
    use crate::runner::state_directory::prepare_runner_state_directory;
    use crate::runner::test_support::{document, load_config};
    use std::io;
    use std::sync::mpsc as std_mpsc;
    use tokio::sync::oneshot;

    #[tokio::test(flavor = "current_thread")]
    async fn next_mutation_starts_only_after_the_previous_commit_returns() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let config = load_config(directory.path())?;
        let (management, receiver) = management_channel();
        let state = empty_state(config.state_directory(), config.script_vm_limits());
        let (finish, finish_requested) = oneshot::channel();
        let first_id = TenonDocumentId::try_from("first-mutation").map_err(io::Error::other)?;
        let (first_response, _first_result) = oneshot::channel();
        let mutation_task = tokio::spawn(async move {
            let _ = finish_requested.await;
            Ok(ManagementMutationResult::DeleteDocument {
                id: first_id,
                response: first_response,
                result: Ok(()),
            })
        });
        let second_id = TenonDocumentId::try_from("second-mutation").map_err(io::Error::other)?;
        let (second_response, mut second_result) = oneshot::channel();
        management
            .mutations
            .send(RunnerManagementMutation::DeleteDocument {
                id: second_id,
                precondition: DeleteDocumentPrecondition::Missing,
                response: second_response,
            })
            .await
            .map_err(|_| io::Error::other("Second mutation was not queued"))?;
        let mut supervisor = RunnerManagementSupervisor {
            state,
            receiver,
            verifier: Arc::new(config.tenon_document_verifier().map_err(io::Error::other)?),
            mutation_task: Some(mutation_task),
            document_preparations: VecDeque::new(),
            pipeline_publication: PipelinePublication::Open,
            recheck_after_mutation: false,
        };
        let _ = finish.send(());
        let RunnerManagementEvent::CommitReady(commit) = supervisor.next_event(|_| Ok(())).await
        else {
            return Err(io::Error::other("First management commit was not returned"));
        };
        assert!(matches!(
            second_result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        commit.publish_after(|_| Ok::<_, io::Error>(()))?;

        let later_event = supervisor.next_event(|_| Ok(()));
        tokio::pin!(later_event);
        tokio::select! {
            _ = &mut later_event => {
                return Err(io::Error::other(
                    "Management returned an event before answering the queued mutation",
                ));
            }
            result = &mut second_result => assert!(matches!(result, Ok(Ok(Ok(()))))),
        }
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_finishes_started_store_work_without_publishing_runtime_work() -> io::Result<()>
    {
        let directory = tempfile::tempdir()?;
        let config = load_config(directory.path())?;
        let (management, receiver) = management_channel();
        let state = empty_state(config.state_directory(), config.script_vm_limits());
        let (finish, finish_requested) = oneshot::channel();
        let started_id = TenonDocumentId::try_from("started-mutation").map_err(io::Error::other)?;
        let (started_response, started_result) = oneshot::channel();
        let mutation_task = tokio::spawn(async move {
            let _ = finish_requested.await;
            Ok(ManagementMutationResult::DeleteDocument {
                id: started_id,
                response: started_response,
                result: Ok(()),
            })
        });
        let queued_id = TenonDocumentId::try_from("queued-mutation").map_err(io::Error::other)?;
        let (queued_response, queued_result) = oneshot::channel();
        management
            .mutations
            .send(RunnerManagementMutation::DeleteDocument {
                id: queued_id,
                precondition: DeleteDocumentPrecondition::Missing,
                response: queued_response,
            })
            .await
            .map_err(|_| io::Error::other("Queued mutation was not accepted"))?;
        let mut supervisor = RunnerManagementSupervisor {
            state,
            receiver,
            verifier: Arc::new(config.tenon_document_verifier().map_err(io::Error::other)?),
            mutation_task: Some(mutation_task),
            document_preparations: VecDeque::new(),
            pipeline_publication: PipelinePublication::Open,
            recheck_after_mutation: false,
        };

        let shutdown = supervisor.shutdown();
        tokio::pin!(shutdown);
        tokio::select! {
            result = &mut shutdown => {
                return Err(io::Error::other(format!(
                    "Management shutdown finished before started Store work: {result:?}",
                )));
            }
            result = queued_result => assert!(matches!(result, Ok(Err(RunnerUnavailable)))),
        }

        let _ = finish.send(());
        shutdown.await.map_err(io::Error::other)?;
        assert!(management.list_documents().await.is_err());
        assert!(matches!(started_result.await, Ok(Ok(Ok(())))));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mutation_backpressure_is_not_bypassed_by_an_internal_queue() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let config = load_config(directory.path())?;
        let (management, receiver) = management_channel();
        let state = empty_state(config.state_directory(), config.script_vm_limits());
        let (release, released) = oneshot::channel();
        let active_id = TenonDocumentId::try_from("active-mutation").map_err(io::Error::other)?;
        let mutation_task = tokio::spawn(async move {
            let _ = released.await;
            let (response, _result) = oneshot::channel();
            Ok(ManagementMutationResult::DeleteDocument {
                id: active_id,
                response,
                result: Ok(()),
            })
        });
        let mut supervisor = RunnerManagementSupervisor {
            state,
            receiver,
            verifier: Arc::new(config.tenon_document_verifier().map_err(io::Error::other)?),
            mutation_task: Some(mutation_task),
            document_preparations: VecDeque::new(),
            pipeline_publication: PipelinePublication::Open,
            recheck_after_mutation: false,
        };

        let fill_queue = async move {
            for index in 0..MANAGEMENT_QUEUE_CAPACITY {
                let (response, _result) = oneshot::channel();
                management
                    .mutations
                    .try_send(RunnerManagementMutation::DeleteDocument {
                        id: TenonDocumentId::try_from(format!("queued-{index}"))
                            .map_err(io::Error::other)?,
                        precondition: DeleteDocumentPrecondition::Missing,
                        response,
                    })
                    .map_err(|_| io::Error::other("Supervisor drained a mutation shadow queue"))?;
                task::yield_now().await;
            }
            let (response, _result) = oneshot::channel();
            let overflow =
                management
                    .mutations
                    .try_send(RunnerManagementMutation::DeleteDocument {
                        id: TenonDocumentId::try_from("queued-overflow")
                            .map_err(io::Error::other)?,
                        precondition: DeleteDocumentPrecondition::Missing,
                        response,
                    });

            assert!(matches!(overflow, Err(mpsc::error::TrySendError::Full(_))));
            let _ = release.send(());
            Ok::<_, io::Error>(())
        };
        let (event, fill_result) = tokio::join!(supervisor.next_event(|_| Ok(())), fill_queue);
        fill_result?;
        let RunnerManagementEvent::CommitReady(commit) = event else {
            return Err(io::Error::other(
                "Active mutation did not complete after release",
            ));
        };
        commit.publish_after(|_| Ok::<_, io::Error>(()))?;
        supervisor.shutdown().await.map_err(io::Error::other)?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completed_document_preparations_commit_in_acceptance_order() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        prepare_runner_state_directory(directory.path()).map_err(io::Error::other)?;
        let config = load_config(directory.path())?;
        let verifier = Arc::new(config.tenon_document_verifier().map_err(io::Error::other)?);
        let (_management, receiver) = management_channel();
        let state = empty_state(config.state_directory(), config.script_vm_limits());
        let command = |id: &str| -> io::Result<_> {
            let source = document(id).into_bytes().into_boxed_slice();
            let (response, result) = oneshot::channel();
            let (started, was_started) = std_mpsc::channel();
            let (release, released) = std_mpsc::channel();
            Ok((
                RunnerManagementDocumentPreparation::PutDocument {
                    id: TenonDocumentId::try_from(id).map_err(io::Error::other)?,
                    precondition: PutDocumentPrecondition::Create,
                    source,
                    response,
                },
                result,
                started,
                was_started,
                release,
                released,
            ))
        };
        let (first, first_response, first_started, first_was_started, release_first, first_release) =
            command("first-prepared")?;
        let (
            second,
            mut second_response,
            second_started,
            second_was_started,
            release_second,
            second_release,
        ) = command("second-prepared")?;
        let first_job = state.prepare_document(first, Arc::clone(&verifier));
        let second_job = state.prepare_document(second, Arc::clone(&verifier));
        let first_task = task::spawn_blocking(move || {
            let _ = first_started.send(());
            let _ = first_release.recv();
            first_job.run()
        });
        let second_task = task::spawn_blocking(move || {
            let _ = second_started.send(());
            let _ = second_release.recv();
            second_job.run()
        });
        let mut supervisor = RunnerManagementSupervisor {
            state,
            receiver,
            verifier,
            mutation_task: None,
            document_preparations: VecDeque::from([first_task, second_task]),
            pipeline_publication: PipelinePublication::Open,
            recheck_after_mutation: false,
        };

        let release_preparations = async move {
            let mut first_started = false;
            let mut second_started = false;
            while !first_started || !second_started {
                first_started |= first_was_started.try_recv().is_ok();
                second_started |= second_was_started.try_recv().is_ok();
                if !first_started || !second_started {
                    task::yield_now().await;
                }
            }
            let _ = release_second.send(());
            task::yield_now().await;
            assert!(matches!(
                second_response.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            let _ = release_first.send(());
            Ok::<_, io::Error>(second_response)
        };
        let (first_event, release_result) =
            tokio::join!(supervisor.next_event(|_| Ok(())), release_preparations);
        let second_response = release_result?;
        let RunnerManagementEvent::CommitReady(first_commit) = first_event else {
            return Err(io::Error::other("First prepared document did not commit"));
        };
        first_commit.publish_after(|_| Ok::<_, io::Error>(()))?;

        let RunnerManagementEvent::CommitReady(second_commit) =
            supervisor.next_event(|_| Ok(())).await
        else {
            return Err(io::Error::other("Second prepared document did not commit"));
        };
        second_commit.publish_after(|_| Ok::<_, io::Error>(()))?;
        assert!(matches!(first_response.await, Ok(Ok(Ok(_)))));
        assert!(matches!(second_response.await, Ok(Ok(Ok(_)))));
        Ok(())
    }
}
