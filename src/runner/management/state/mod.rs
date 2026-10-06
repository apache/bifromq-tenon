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

//! Serialized state machine behind the Runner management interface.
//!
//! This module owns the current Tenon Document set, the Program Store, query
//! projections, and runtime reconciliation decisions. The management
//! supervisor owns mutation ordering and blocking jobs. No transport adapter
//! can access the state directly.

mod plugins;

use super::jobs::{
    ManagementDocumentPreparationJob, ManagementDocumentPreparationResult, ManagementMutationJob,
    ManagementMutationResult,
};
use super::metrics::ManagementMetrics;
use super::{
    DeleteDocumentPrecondition, DocumentContent, DocumentSummary, DocumentWriteFailure,
    PipelineConvergence, PipelineDetails, PipelineLifecycleRole, PipelineLifecycleState,
    PipelinePublication, PipelineStateUpdate, PipelineStatus, PluginDeleteFailure,
    PluginInstanceView, PluginOperationFailure, ProcessErrorView, PutDocumentFailure,
    PutDocumentOutcome, PutDocumentPrecondition, RunnerManagementDocumentPreparation,
    RunnerManagementMutation, RunnerManagementQuery,
};
use crate::config::ScriptVmLimits;
use crate::contracts::core::{PipelineStatusSnapshot, PluginInstanceState};
use crate::identifiers::TenonDocumentId;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::extensions::{ArtifactProtection, ExecutionDenied, ExecutionScope};
use crate::runner::pipeline::{
    PipelineLifecycleTarget, RuntimeResolution, RuntimeResolutionIssue, RuntimeResolver,
};
use crate::runner::plugin::store::{PluginProgramInstallResult, PluginProgramStore};
use crate::runner::process_resources;
use crate::runner::recovery::RecoveredRunnerState;
use crate::runner::state_directory::TENON_DOCUMENT_STORE_DIRECTORY_NAME;
use crate::tenon_document::verified::{TenonDocumentVerifier, VerifiedTenonDocument};
use opentelemetry::metrics::Meter;
use process_resources::applied_limits;
use std::collections::HashMap;
use std::io;
use std::iter;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

/// One lifecycle action derived from a committed management fact.
pub(crate) enum PipelineDirective {
    SetTarget {
        document_id: TenonDocumentId,
        target: Option<Arc<PipelineLifecycleTarget>>,
    },
    Stop {
        document_id: TenonDocumentId,
    },
}

/// The only owner of Runner management facts and business decisions.
pub(crate) struct RunnerManagementState {
    state_directory: PathBuf,
    artifact_protection: Arc<dyn ArtifactProtection>,
    script_vm_limits: ScriptVmLimits,
    available_cpu_count: NonZeroUsize,
    documents: HashMap<TenonDocumentId, RunnerDocumentState>,
    // The Store moves into the supervised job and returns on nonfatal completion.
    // Queries wait during the handoff; fatal failure ends service, not lookup.
    programs: Option<PluginProgramStore>,
    metrics: Option<ManagementMetrics>,
}

impl RunnerManagementState {
    // ===== Recovery and lifecycle intake =====

    pub(crate) fn check_execution(
        &self,
        authorize: impl FnOnce(ExecutionScope<'_>) -> Result<(), ExecutionDenied>,
    ) -> Result<(), ExecutionDenied> {
        let documents = self
            .documents
            .values()
            .filter(|state| state.is_accepted())
            .map(|state| state.document.as_ref())
            .collect::<Vec<_>>();
        authorize(ExecutionScope {
            documents: &documents,
        })
    }

    /// Restores the complete management state and derives initial targets.
    pub(crate) fn recover(
        state_directory: &Path,
        script_vm_limits: ScriptVmLimits,
        recovered: RecoveredRunnerState,
        artifact_protection: Arc<dyn ArtifactProtection>,
        metrics: Option<&Meter>,
        mut authorize: impl FnMut(ExecutionScope<'_>) -> Result<(), ExecutionDenied>,
    ) -> io::Result<(Self, Box<[PipelineDirective]>)> {
        let (documents, programs) = recovered.into_parts();
        let mut owner = Self {
            state_directory: state_directory.to_path_buf(),
            artifact_protection,
            script_vm_limits,
            available_cpu_count: process_resources::available_cpu_count()?,
            documents: HashMap::new(),
            programs: Some(programs),
            metrics: metrics.map(ManagementMetrics::new),
        };
        let mut directives = Vec::new();
        for recovered in documents {
            let (source, document) = recovered.into_parts();
            let documents = owner.proposed_documents(&document);
            let execution = match authorize(ExecutionScope {
                documents: &documents,
            }) {
                Ok(()) => DocumentExecution::Accepted(None),
                Err(error) => {
                    eprintln!(
                        "{}: Document {} was not accepted: {}",
                        error.code(),
                        document.id(),
                        error
                    );
                    DocumentExecution::NotAccepted
                }
            };
            let document_id = document.id().clone();
            owner.documents.insert(
                document_id.clone(),
                RunnerDocumentState {
                    source,
                    document,
                    execution,
                },
            );
            directives.extend(owner.reconcile_document(&document_id).into_vec());
        }
        Ok((owner, directives.into_boxed_slice()))
    }

    /// Records a complete state snapshot from a live Pipeline owner.
    #[allow(
        clippy::expect_used,
        reason = "Pipeline updates are accepted only from the current lifecycle owner"
    )]
    pub(crate) fn record_pipeline_state(
        &mut self,
        update: PipelineStateUpdate,
        role: PipelineLifecycleRole,
    ) {
        if matches!(role, PipelineLifecycleRole::Retiring) {
            return;
        }
        let state = self
            .documents
            .get_mut(&update.document_id)
            .expect("a current Pipeline update must retain its management document");
        let DocumentExecution::Accepted(Some(lifecycle)) = &mut state.execution else {
            unreachable!("a current Pipeline update must retain an accepted lifecycle");
        };
        *lifecycle = update.state;
        if let Some(metrics) = &self.metrics {
            metrics.refresh(&update.document_id, |is_unready| {
                let runtime_ready = !is_unready;
                pipeline_status(&update.document_id, state, runtime_ready)
            });
        }
    }

    // ===== Mutation scheduling: check preconditions and produce jobs =====

    #[allow(
        clippy::expect_used,
        reason = "the supervisor starts a mutation only after the previous job returned its Store"
    )]
    pub(super) fn start_mutation(
        &mut self,
        command: RunnerManagementMutation,
    ) -> Option<ManagementMutationJob> {
        match command {
            RunnerManagementMutation::DeleteDocument {
                id,
                precondition,
                response,
            } => {
                let Some(state) = self.documents.get(&id) else {
                    let _ = response.send(Ok(Ok(())));
                    return None;
                };
                match precondition {
                    DeleteDocumentPrecondition::Match(condition) if condition == state.etag() => {}
                    DeleteDocumentPrecondition::Missing => {
                        let _ = response.send(Ok(Err(DocumentWriteFailure::PreconditionRequired)));
                        return None;
                    }
                    DeleteDocumentPrecondition::Match(_) => {
                        let _ = response.send(Ok(Err(DocumentWriteFailure::PreconditionFailed)));
                        return None;
                    }
                }
                Some(ManagementMutationJob::DeleteDocument {
                    id,
                    response,
                    document_store_directory: self.document_store_directory(),
                })
            }
            RunnerManagementMutation::InstallProgram { package, response } => {
                Some(ManagementMutationJob::InstallProgram {
                    programs: self
                        .programs
                        .take()
                        .expect("serial mutation must own the Store"),
                    package,
                    response,
                })
            }
            RunnerManagementMutation::DeleteProgram {
                program_name,
                exact_version,
                response,
            } => {
                let references = self.documents_referencing_program(&program_name, &exact_version);
                if !references.is_empty() {
                    let _ = response.send(Ok(Err(PluginDeleteFailure::InUse {
                        referenced_by: references.into_boxed_slice(),
                    })));
                    return None;
                }
                Some(ManagementMutationJob::DeleteProgram {
                    programs: self
                        .programs
                        .take()
                        .expect("serial mutation must own the Store"),
                    program_name,
                    exact_version,
                    response,
                })
            }
        }
    }

    pub(super) fn prepare_document(
        &self,
        command: RunnerManagementDocumentPreparation,
        verifier: Arc<TenonDocumentVerifier>,
    ) -> ManagementDocumentPreparationJob {
        match command {
            RunnerManagementDocumentPreparation::PutDocument {
                id,
                precondition,
                source,
                response,
            } => ManagementDocumentPreparationJob {
                id,
                precondition,
                source,
                response,
                verifier,
            },
        }
    }

    pub(super) fn start_document_commit(
        &self,
        prepared: ManagementDocumentPreparationResult,
        authorize: impl FnOnce(ExecutionScope<'_>) -> Result<(), ExecutionDenied>,
    ) -> Option<ManagementMutationJob> {
        let ManagementDocumentPreparationResult {
            precondition,
            source,
            response,
            result,
        } = prepared;
        let document = match result {
            Ok(document) => document,
            Err(error) => {
                let _ = response.send(Ok(Err(error)));
                return None;
            }
        };
        let existing = self.documents.get(document.id());
        let outcome = match (existing, &precondition) {
            (None, PutDocumentPrecondition::Create) => PutDocumentOutcome::Created,
            (Some(state), PutDocumentPrecondition::Replace(etag)) if *etag == state.etag() => {
                PutDocumentOutcome::Replaced
            }
            (None, PutDocumentPrecondition::Missing)
            | (Some(_), PutDocumentPrecondition::Missing) => {
                let _ = response.send(Ok(Err(PutDocumentFailure::Write(
                    DocumentWriteFailure::PreconditionRequired,
                ))));
                return None;
            }
            (None, PutDocumentPrecondition::Replace(_))
            | (Some(_), PutDocumentPrecondition::Create)
            | (Some(_), PutDocumentPrecondition::Replace(_)) => {
                let _ = response.send(Ok(Err(PutDocumentFailure::Write(
                    DocumentWriteFailure::PreconditionFailed,
                ))));
                return None;
            }
        };
        if existing
            .is_some_and(|state| state.is_accepted() && state.source.as_ref() == source.as_ref())
        {
            let _ = response.send(Ok(Ok(PutDocumentOutcome::Unchanged)));
            return None;
        }
        let documents = self.proposed_documents(&document);
        match authorize(ExecutionScope {
            documents: &documents,
        }) {
            Ok(()) => {}
            Err(error) => {
                let _ = response.send(Ok(Err(PutDocumentFailure::ExecutionDenied(error))));
                return None;
            }
        };
        Some(ManagementMutationJob::PutDocument {
            source,
            document,
            outcome,
            response,
            document_store_directory: self.document_store_directory(),
            protection: Arc::clone(&self.artifact_protection),
        })
    }

    // ===== Mutation completion: apply committed facts and reconcile =====

    /// Publishes one finished Store operation into the management facts.
    pub(super) fn complete_mutation(
        &mut self,
        mutation: ManagementMutationResult,
        pipeline_publication: PipelinePublication,
    ) -> Option<RunnerManagementCommit> {
        let commit = self.apply_mutation(mutation, pipeline_publication);
        match pipeline_publication {
            PipelinePublication::Open => Some(commit),
            PipelinePublication::Closed => {
                commit.finish_without_directives();
                None
            }
        }
    }

    fn apply_mutation(
        &mut self,
        mutation: ManagementMutationResult,
        pipeline_publication: PipelinePublication,
    ) -> RunnerManagementCommit {
        match mutation {
            ManagementMutationResult::PutDocument {
                source,
                outcome,
                response,
                result,
            } => {
                let document = match result {
                    Ok(result) => result,
                    Err(error) => {
                        return RunnerManagementCommit::put_document(
                            Box::new([]),
                            response,
                            Err(error),
                        );
                    }
                };
                let id = document.id().clone();
                let previous = self.documents.remove(&id);
                let previous_lifecycle = previous.and_then(|state| match state.execution {
                    DocumentExecution::Accepted(lifecycle) => lifecycle,
                    DocumentExecution::NotAccepted => None,
                });
                self.documents.insert(
                    id.clone(),
                    RunnerDocumentState {
                        source: Zeroizing::new(source),
                        document,
                        execution: DocumentExecution::Accepted(previous_lifecycle),
                    },
                );
                let directives = match pipeline_publication {
                    PipelinePublication::Open => self.reconcile_document(&id),
                    PipelinePublication::Closed => Box::new([]),
                };
                RunnerManagementCommit::put_document(directives, response, Ok(outcome))
            }
            ManagementMutationResult::DeleteDocument {
                id,
                response,
                result,
            } => {
                if result.is_err() {
                    return RunnerManagementCommit::delete_document(
                        Box::new([]),
                        response,
                        Err(DocumentWriteFailure::Store),
                    );
                }
                if let Some(metrics) = &self.metrics {
                    metrics.remove(&id);
                }
                let stopped = self
                    .documents
                    .remove(&id)
                    .is_some_and(|state| state.lifecycle().is_some());
                let directives = if stopped && pipeline_publication == PipelinePublication::Open {
                    vec![PipelineDirective::Stop { document_id: id }].into_boxed_slice()
                } else {
                    Box::new([])
                };
                RunnerManagementCommit::delete_document(directives, response, Ok(()))
            }
            ManagementMutationResult::InstallProgram {
                programs,
                response,
                result,
            } => {
                self.programs = Some(programs);
                let directives =
                    if result.is_ok() && pipeline_publication == PipelinePublication::Open {
                        self.reconcile_all_documents()
                    } else {
                        Box::new([])
                    };
                RunnerManagementCommit {
                    directives,
                    response: MutationResponse::InstallProgram(response, result),
                }
            }
            ManagementMutationResult::DeleteProgram {
                programs,
                response,
                result,
            } => {
                self.programs = Some(programs);
                // No Document can reference a successfully removed Program.
                RunnerManagementCommit::delete_program(Box::new([]), response, result)
            }
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "reconciliation iterates identities selected from the owned document map"
    )]
    fn reconcile_document(&mut self, id: &TenonDocumentId) -> Box<[PipelineDirective]> {
        let programs = self
            .programs
            .as_ref()
            .expect("reconciliation must own the Store");
        let state = self
            .documents
            .get_mut(id)
            .expect("document reconciliation must retain its management document");
        if !state.is_accepted() {
            return Box::new([]);
        }
        let resolution = RuntimeResolver::new(self.script_vm_limits, self.available_cpu_count)
            .resolve(&state.document, programs);
        let runtime_ready = matches!(resolution, RuntimeResolution::Ready(_));
        match resolution {
            RuntimeResolution::Ready(plan) => {
                let target = Arc::new(PipelineLifecycleTarget::new(plan, state.etag()));
                if state.lifecycle().is_none() {
                    state.execution =
                        DocumentExecution::Accepted(Some(PipelineLifecycleState::Starting));
                }
                if let Some(metrics) = &self.metrics {
                    metrics.record(pipeline_status(id, state, runtime_ready));
                }
                vec![PipelineDirective::SetTarget {
                    document_id: id.clone(),
                    target: Some(target),
                }]
                .into_boxed_slice()
            }
            RuntimeResolution::Unready(issues) => {
                debug_assert!(!issues.is_empty());
                if let Some(metrics) = &self.metrics {
                    metrics.record(pipeline_status(id, state, runtime_ready));
                }
                vec![PipelineDirective::SetTarget {
                    document_id: id.clone(),
                    target: None,
                }]
                .into_boxed_slice()
            }
        }
    }

    fn reconcile_all_documents(&mut self) -> Box<[PipelineDirective]> {
        let ids = self.documents.keys().cloned().collect::<Vec<_>>();
        let mut directives = Vec::new();
        for id in ids {
            directives.extend(self.reconcile_document(&id).into_vec());
        }
        directives.into_boxed_slice()
    }

    // ===== Query projections: read models =====

    pub(super) fn handle_query(&self, command: RunnerManagementQuery) {
        self.answer_query(command);
    }

    fn answer_query(&self, command: RunnerManagementQuery) {
        match command {
            RunnerManagementQuery::ListDocuments(response) => {
                let documents = self
                    .documents
                    .iter()
                    .map(|(id, state)| DocumentSummary {
                        id: id.clone(),
                        etag: state.etag(),
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let _ = response.send(Ok(documents));
            }
            RunnerManagementQuery::GetDocument(id, response) => {
                let document = self.documents.get(&id).map(|state| DocumentContent {
                    source: state.source.to_vec().into_boxed_slice(),
                    etag: state.etag(),
                });
                let _ = response.send(Ok(document));
            }
            RunnerManagementQuery::ListPipelines(response) => {
                let pipelines = self
                    .documents
                    .iter()
                    .filter(|(_, state)| state.is_accepted())
                    .map(|(id, state)| {
                        let runtime_ready = self.pipeline_runtime_issues(state).is_empty();
                        pipeline_status(id, state, runtime_ready)
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice();
                let _ = response.send(Ok(pipelines));
            }
            RunnerManagementQuery::GetPipeline(id, response) => {
                let pipeline = self
                    .documents
                    .get(&id)
                    .filter(|state| state.is_accepted())
                    .map(|state| self.pipeline_details(&id, state));
                let _ = response.send(Ok(pipeline));
            }
            RunnerManagementQuery::ListPrograms {
                interface,
                response,
            } => {
                let _ = response.send(Ok(self.program_entries(interface)));
            }
            RunnerManagementQuery::GetProgram {
                program_name,
                exact_version,
                resource,
                response,
            } => {
                let resource = self.program_resource(&program_name, &exact_version, resource);
                let _ = response.send(Ok(resource));
            }
        }
    }

    fn pipeline_details(
        &self,
        id: &TenonDocumentId,
        state: &RunnerDocumentState,
    ) -> PipelineDetails {
        let runtime_issues = self.pipeline_runtime_issues(state);
        let runtime_ready = runtime_issues.is_empty();
        let status = pipeline_status(id, state, runtime_ready);
        let plugin_instances = match state.lifecycle() {
            Some(PipelineLifecycleState::Running(running)) => {
                plugin_instance_views(running.document(), running.snapshot())
            }
            _ => Box::default(),
        };
        PipelineDetails {
            status,
            runtime_issues,
            plugin_instances,
            resource_limits: match state.lifecycle() {
                Some(PipelineLifecycleState::Running(running)) => {
                    applied_limits(running.document())
                }
                _ => None,
            },
            last_error: match state.lifecycle() {
                Some(PipelineLifecycleState::RestartBackoff(error)) => Some(error.clone()),
                _ => None,
            },
        }
    }

    fn pipeline_runtime_issues(
        &self,
        state: &RunnerDocumentState,
    ) -> Box<[RuntimeResolutionIssue]> {
        match RuntimeResolver::new(self.script_vm_limits, self.available_cpu_count)
            .resolve(&state.document, self.program_store())
        {
            RuntimeResolution::Ready(_) => Box::default(),
            RuntimeResolution::Unready(issues) => issues,
        }
    }

    // ===== Store ownership and shared helpers =====

    /// Reports whether queries can borrow the Store outside a mutation job.
    #[must_use]
    pub(super) const fn owns_program_store(&self) -> bool {
        self.programs.is_some()
    }

    #[allow(
        clippy::expect_used,
        reason = "the supervisor pauses queries while a mutation owns the Store"
    )]
    fn program_store(&self) -> &PluginProgramStore {
        self.programs
            .as_ref()
            .expect("queries must borrow the owned Store")
    }

    fn document_store_directory(&self) -> PathBuf {
        self.state_directory
            .join(TENON_DOCUMENT_STORE_DIRECTORY_NAME)
    }

    fn proposed_documents<'a>(
        &'a self,
        candidate: &'a VerifiedTenonDocument,
    ) -> Vec<&'a VerifiedTenonDocument> {
        self.documents
            .values()
            .filter(|state| state.is_accepted() && state.document.id() != candidate.id())
            .map(|state| state.document.as_ref())
            .chain(iter::once(candidate))
            .collect()
    }
}

pub(crate) struct RunnerManagementCommit {
    directives: Box<[PipelineDirective]>,
    response: MutationResponse,
}

impl RunnerManagementCommit {
    /// Publishes the adapter response only after lifecycle directives are accepted.
    pub(crate) fn publish_after<E>(
        self,
        apply: impl FnOnce(Box<[PipelineDirective]>) -> Result<(), E>,
    ) -> Result<(), E> {
        apply(self.directives)?;
        self.response.send();
        Ok(())
    }

    fn finish_without_directives(self) {
        self.response.send();
    }

    fn put_document(
        directives: Box<[PipelineDirective]>,
        response: super::ManagementReply<Result<PutDocumentOutcome, PutDocumentFailure>>,
        result: Result<PutDocumentOutcome, PutDocumentFailure>,
    ) -> Self {
        Self {
            directives,
            response: MutationResponse::PutDocument(response, result),
        }
    }

    fn delete_document(
        directives: Box<[PipelineDirective]>,
        response: super::ManagementReply<Result<(), DocumentWriteFailure>>,
        result: Result<(), DocumentWriteFailure>,
    ) -> Self {
        Self {
            directives,
            response: MutationResponse::DeleteDocument(response, result),
        }
    }

    fn delete_program(
        directives: Box<[PipelineDirective]>,
        response: super::ManagementReply<Result<(), PluginDeleteFailure>>,
        result: Result<(), PluginDeleteFailure>,
    ) -> Self {
        Self {
            directives,
            response: MutationResponse::DeleteProgram(response, result),
        }
    }
}

enum MutationResponse {
    PutDocument(
        super::ManagementReply<Result<PutDocumentOutcome, PutDocumentFailure>>,
        Result<PutDocumentOutcome, PutDocumentFailure>,
    ),
    DeleteDocument(
        super::ManagementReply<Result<(), DocumentWriteFailure>>,
        Result<(), DocumentWriteFailure>,
    ),
    InstallProgram(
        super::ManagementReply<Result<PluginProgramInstallResult, PluginOperationFailure>>,
        Result<PluginProgramInstallResult, PluginOperationFailure>,
    ),
    DeleteProgram(
        super::ManagementReply<Result<(), PluginDeleteFailure>>,
        Result<(), PluginDeleteFailure>,
    ),
}

impl MutationResponse {
    fn send(self) {
        match self {
            Self::PutDocument(response, result) => {
                let _ = response.send(Ok(result));
            }
            Self::DeleteDocument(response, result) => {
                let _ = response.send(Ok(result));
            }
            Self::InstallProgram(response, result) => {
                let _ = response.send(Ok(result));
            }
            Self::DeleteProgram(response, result) => {
                let _ = response.send(Ok(result));
            }
        }
    }
}

struct RunnerDocumentState {
    // The exact durable source is returned by the API and defines its ETag;
    // the verified model cannot reproduce the caller's original JSONC bytes.
    source: Zeroizing<Box<[u8]>>,
    document: Arc<VerifiedTenonDocument>,
    execution: DocumentExecution,
}

impl RunnerDocumentState {
    fn is_accepted(&self) -> bool {
        matches!(self.execution, DocumentExecution::Accepted(_))
    }

    fn lifecycle(&self) -> Option<&PipelineLifecycleState> {
        match &self.execution {
            DocumentExecution::Accepted(lifecycle) => lifecycle.as_ref(),
            DocumentExecution::NotAccepted => None,
        }
    }

    fn etag(&self) -> TenonDocumentEtag {
        TenonDocumentEtag::for_source(&self.source)
    }
}

enum DocumentExecution {
    NotAccepted,
    Accepted(Option<PipelineLifecycleState>),
}

fn pipeline_status(
    id: &TenonDocumentId,
    state: &RunnerDocumentState,
    runtime_ready: bool,
) -> PipelineStatus {
    let document_etag = state.etag();
    let convergence = if runtime_ready {
        match state.lifecycle() {
            Some(PipelineLifecycleState::Starting) | None => PipelineConvergence::Starting,
            Some(PipelineLifecycleState::RestartBackoff(_)) => PipelineConvergence::RestartBackoff,
            Some(PipelineLifecycleState::Running(running)) => {
                let applied = running.document_etag();
                if applied == document_etag {
                    PipelineConvergence::Running { applied }
                } else {
                    PipelineConvergence::Updating { applied }
                }
            }
        }
    } else {
        let applied = match state.lifecycle() {
            Some(PipelineLifecycleState::Running(running)) => Some(running.document_etag()),
            _ => None,
        };
        PipelineConvergence::Unready { applied }
    };
    PipelineStatus {
        id: id.clone(),
        document_etag,
        convergence,
    }
}

#[allow(
    clippy::expect_used,
    reason = "the applied snapshot was validated against its retained Document"
)]
fn plugin_instance_views(
    document: &VerifiedTenonDocument,
    snapshot: &PipelineStatusSnapshot,
) -> Box<[PluginInstanceView]> {
    snapshot
        .plugin_instances
        .iter()
        .zip(document.plugin_instances())
        .map(|(status, (id, instance))| PluginInstanceView {
            id: id.clone(),
            program_name: instance.program_name().clone(),
            exact_version: instance.exact_version().clone(),
            state: PluginInstanceState::try_from(status.state)
                .expect("validated applied state retains a known Instance state"),
            last_error: status.last_error.as_ref().map(|error| ProcessErrorView {
                code: error.code.clone().into_boxed_str(),
                message: error.message.clone().into_boxed_str(),
            }),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::runner::extensions::ByPass;
    use crate::runner::plugin::store::test_support::empty_store;

    #[allow(
        clippy::expect_used,
        reason = "the test fixture has a constant non-zero record limit"
    )]
    pub(crate) fn empty_state(
        state_directory: &Path,
        script_vm_limits: ScriptVmLimits,
    ) -> RunnerManagementState {
        RunnerManagementState {
            state_directory: state_directory.to_path_buf(),
            artifact_protection: Arc::new(ByPass),
            script_vm_limits,
            available_cpu_count: process_resources::available_cpu_count()
                .expect("Runner must determine available CPU count"),
            documents: HashMap::new(),
            programs: Some(empty_store(state_directory.join("plugins/programs"))),
            metrics: None,
        }
    }

    pub(crate) fn insert_document(
        state: &mut RunnerManagementState,
        source: Box<[u8]>,
        document: Arc<VerifiedTenonDocument>,
        lifecycle: PipelineLifecycleState,
    ) {
        state.documents.insert(
            document.id().clone(),
            RunnerDocumentState {
                source: Zeroizing::new(source),
                document,
                execution: DocumentExecution::Accepted(Some(lifecycle)),
            },
        );
    }

    pub(super) fn document_state<'a>(
        state: &'a RunnerManagementState,
        id: &TenonDocumentId,
    ) -> Option<(TenonDocumentEtag, &'a PipelineLifecycleState)> {
        state.documents.get(id).and_then(|document| {
            document
                .lifecycle()
                .map(|lifecycle| (document.etag(), lifecycle))
        })
    }

    pub(crate) fn pipeline(
        state: &RunnerManagementState,
        id: &TenonDocumentId,
    ) -> Option<PipelineDetails> {
        state
            .documents
            .get(id)
            .map(|document| state.pipeline_details(id, document))
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{document_state, empty_state, insert_document};
    use super::*;
    use crate::runner::extensions::RunnerHooks;
    use crate::runner::recovery::recover;
    use crate::runner::state_directory::prepare_runner_state_directory;
    use crate::runner::test_support::{document, load_config, write_document};
    use crate::tenon_document::UnverifiedTenonDocument;
    use std::io;

    #[test]
    fn unready_reconciliation_replaces_the_restart_target() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let config = load_config(directory.path())?;
        write_document(
            directory.path(),
            "unready-restart",
            document("unready-restart"),
        )?;
        let layout = prepare_runner_state_directory(directory.path()).map_err(io::Error::other)?;
        let recovered = recover(
            &config,
            &layout,
            &RunnerHooks::default().artifact_protection,
            &layout.pipeline_runtime_directory(),
        )
        .map_err(io::Error::other)?;
        let (mut state, initial) = RunnerManagementState::recover(
            directory.path(),
            config.script_vm_limits(),
            recovered,
            RunnerHooks::default().artifact_protection,
            None,
            |_| Ok(()),
        )?;
        assert!(matches!(
            initial.as_ref(),
            [PipelineDirective::SetTarget { target: None, .. }]
        ));
        let id = TenonDocumentId::try_from("unready-restart").map_err(io::Error::other)?;

        let directives = state.reconcile_document(&id);

        assert!(matches!(
            directives.as_ref(),
            [PipelineDirective::SetTarget { document_id, target: None }] if document_id == &id
        ));
        Ok(())
    }

    #[test]
    fn state_update_preserves_the_desired_document_identity() -> io::Result<()> {
        let document_id = TenonDocumentId::try_from("state-owner").map_err(io::Error::other)?;
        let (mut management, document_etag) = test_management_state("state-owner")?;

        management.record_pipeline_state(
            PipelineStateUpdate {
                document_id: document_id.clone(),
                state: PipelineLifecycleState::Starting,
            },
            PipelineLifecycleRole::Current,
        );

        let Some((retained, lifecycle)) = document_state(&management, &document_id) else {
            return Err(io::Error::other("Document state disappeared"));
        };
        assert_eq!(retained, document_etag);
        assert!(matches!(lifecycle, PipelineLifecycleState::Starting));
        Ok(())
    }

    #[test]
    fn retiring_state_update_is_discarded_after_its_owner_is_removed() -> io::Result<()> {
        let document_id = TenonDocumentId::try_from("retiring-owner").map_err(io::Error::other)?;
        let directory = tempfile::tempdir()?;
        let config = load_config(directory.path())?;
        let mut management = empty_state(config.state_directory(), config.script_vm_limits());

        management.record_pipeline_state(
            PipelineStateUpdate {
                document_id,
                state: PipelineLifecycleState::Starting,
            },
            PipelineLifecycleRole::Retiring,
        );
        Ok(())
    }

    fn test_management_state(id: &str) -> io::Result<(RunnerManagementState, TenonDocumentEtag)> {
        let directory = tempfile::tempdir()?;
        let config = load_config(directory.path())?;
        let source = document(id).into_bytes().into_boxed_slice();
        let document_etag = TenonDocumentEtag::for_source(&source);
        let parsed = UnverifiedTenonDocument::parse(&source).map_err(io::Error::other)?;
        let verified = config
            .tenon_document_verifier()
            .map_err(io::Error::other)?
            .verify(parsed)
            .map_err(io::Error::other)?;
        let mut management = empty_state(config.state_directory(), config.script_vm_limits());
        insert_document(
            &mut management,
            source,
            Arc::new(verified),
            PipelineLifecycleState::Starting,
        );
        Ok((management, document_etag))
    }
}
