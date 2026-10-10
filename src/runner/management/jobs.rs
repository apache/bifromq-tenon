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

//! Concrete blocking jobs owned and joined by the management supervisor.
//!
//! Jobs contain only the owned inputs needed off the async main thread. Their
//! results return complete facts to the serialized state owner. A unified
//! Program mutation takes and returns the original Store, committing its disk
//! and index together. Jobs never publish Pipeline directives themselves.

use super::plugin_upload::PluginUploadReader;
use super::{
    DocumentValidationFailure, DocumentWriteFailure, ManagementReply, PluginDeleteFailure,
    PluginOperationFailure, PutDocumentFailure, PutDocumentOutcome, PutDocumentPrecondition,
};
use crate::identifiers::{ExactVersion, ProgramName, TenonDocumentId};
use crate::runner::document_store::{TenonDocumentStore, TenonDocumentStoreError};
use crate::runner::extensions::ArtifactProtection;
use crate::runner::plugin::package::PluginPackageError;
use crate::runner::plugin::store::{
    PluginProgramInstallResult, PluginProgramStore, PluginStoreError,
};
use crate::tenon_document::UnverifiedTenonDocument;
use crate::tenon_document::verified::{TenonDocumentVerifier, VerifiedTenonDocument};
use std::path::PathBuf;
use std::sync::Arc;

pub(super) enum ManagementMutationResult {
    PutDocument {
        source: Box<[u8]>,
        outcome: PutDocumentOutcome,
        response: ManagementReply<Result<PutDocumentOutcome, PutDocumentFailure>>,
        result: Result<Arc<VerifiedTenonDocument>, PutDocumentFailure>,
    },
    DeleteDocument {
        id: TenonDocumentId,
        response: ManagementReply<Result<(), DocumentWriteFailure>>,
        result: Result<(), TenonDocumentStoreError>,
    },
    InstallProgram {
        programs: PluginProgramStore,
        response: ManagementReply<Result<PluginProgramInstallResult, PluginOperationFailure>>,
        result: Result<PluginProgramInstallResult, PluginOperationFailure>,
    },
    /// Returns the original Store after a successful delete or an in-use conflict.
    DeleteProgram {
        programs: PluginProgramStore,
        response: ManagementReply<Result<(), PluginDeleteFailure>>,
        result: Result<(), PluginDeleteFailure>,
    },
}

pub(super) enum ManagementMutationJob {
    PutDocument {
        source: Box<[u8]>,
        document: Arc<VerifiedTenonDocument>,
        outcome: PutDocumentOutcome,
        response: ManagementReply<Result<PutDocumentOutcome, PutDocumentFailure>>,
        document_store_directory: PathBuf,
        protection: Arc<dyn ArtifactProtection>,
    },
    DeleteDocument {
        id: TenonDocumentId,
        response: ManagementReply<Result<(), DocumentWriteFailure>>,
        document_store_directory: PathBuf,
    },
    InstallProgram {
        programs: PluginProgramStore,
        package: PluginUploadReader,
        response: ManagementReply<Result<PluginProgramInstallResult, PluginOperationFailure>>,
    },
    /// Owns the only Store while its disk and index are mutated together.
    DeleteProgram {
        programs: PluginProgramStore,
        program_name: ProgramName,
        exact_version: ExactVersion,
        response: ManagementReply<Result<(), PluginDeleteFailure>>,
    },
}

impl ManagementMutationJob {
    /// Completes one owned mutation, preserving fatal Program Store errors.
    ///
    /// # Errors
    ///
    /// Returns unified Store integrity or persistence failures to the Runner's
    /// fatal shutdown path. Ordinary operation failures remain in the result.
    pub(super) fn run(self) -> Result<ManagementMutationResult, PluginStoreError> {
        Ok(match self {
            Self::PutDocument {
                source,
                document,
                outcome,
                response,
                document_store_directory,
                protection,
            } => {
                let result = TenonDocumentStore::new(document_store_directory)
                    .commit(document.id(), &source, protection.as_ref())
                    .map(|_| document)
                    .map_err(|_| PutDocumentFailure::Write(DocumentWriteFailure::Store));
                ManagementMutationResult::PutDocument {
                    source,
                    outcome,
                    response,
                    result,
                }
            }
            Self::DeleteDocument {
                id,
                response,
                document_store_directory,
            } => ManagementMutationResult::DeleteDocument {
                result: TenonDocumentStore::new(document_store_directory).delete(&id),
                id,
                response,
            },
            Self::InstallProgram {
                mut programs,
                package,
                response,
            } => {
                let result = match programs.install(package) {
                    Ok(result) => Ok(result),
                    Err(error) => Err(program_install_failure(error)?),
                };
                ManagementMutationResult::InstallProgram {
                    programs,
                    response,
                    result,
                }
            }
            Self::DeleteProgram {
                mut programs,
                program_name,
                exact_version,
                response,
            } => {
                let result = match programs.uninstall(&program_name, &exact_version) {
                    Ok(_) => Ok(()),
                    Err(PluginStoreError::ProgramInUse) => Err(PluginDeleteFailure::InUse {
                        referenced_by: Box::new([]),
                    }),
                    Err(source) => return Err(source),
                };
                ManagementMutationResult::DeleteProgram {
                    programs,
                    response,
                    result,
                }
            }
        })
    }
}

pub(super) struct ManagementDocumentPreparationResult {
    pub(super) precondition: PutDocumentPrecondition,
    pub(super) source: Box<[u8]>,
    pub(super) response: ManagementReply<Result<PutDocumentOutcome, PutDocumentFailure>>,
    pub(super) result: Result<Arc<VerifiedTenonDocument>, PutDocumentFailure>,
}

impl ManagementDocumentPreparationResult {
    pub(super) fn reject_shutting_down(self) {
        let _ = self.response.send(Err(super::RunnerUnavailable));
    }
}

pub(super) struct ManagementDocumentPreparationJob {
    pub(super) id: TenonDocumentId,
    pub(super) precondition: PutDocumentPrecondition,
    pub(super) source: Box<[u8]>,
    pub(super) response: ManagementReply<Result<PutDocumentOutcome, PutDocumentFailure>>,
    pub(super) verifier: Arc<TenonDocumentVerifier>,
}

impl ManagementDocumentPreparationJob {
    pub(super) fn run(self) -> ManagementDocumentPreparationResult {
        let result = UnverifiedTenonDocument::parse(&self.source)
            .map_err(DocumentValidationFailure::from)
            .and_then(|document| {
                self.verifier
                    .verify(document)
                    .map_err(DocumentValidationFailure::from)
            })
            .map_err(PutDocumentFailure::Validation)
            .and_then(|document| {
                if document.id() == &self.id {
                    Ok(Arc::new(document))
                } else {
                    Err(PutDocumentFailure::IdMismatch)
                }
            });
        ManagementDocumentPreparationResult {
            precondition: self.precondition,
            source: self.source,
            response: self.response,
            result,
        }
    }
}

fn program_install_failure(
    error: PluginStoreError,
) -> Result<PluginOperationFailure, PluginStoreError> {
    match error {
        PluginStoreError::PackageAccessRejected => {
            Ok(PluginOperationFailure::Invalid { code: error.code() })
        }
        PluginStoreError::PlatformMismatch { platforms } => {
            Ok(PluginOperationFailure::PlatformMismatch { platforms })
        }
        PluginStoreError::VersionConflict => {
            Ok(PluginOperationFailure::Conflict { code: error.code() })
        }
        PluginStoreError::PackageInvalid {
            source: PluginPackageError::PackageTooLarge,
        } => Ok(PluginOperationFailure::TooLarge),
        PluginStoreError::PackageInvalid {
            source:
                PluginPackageError::PackageInvalid
                | PluginPackageError::ArchiveInvalid { .. }
                | PluginPackageError::ManifestJsonInvalid { .. }
                | PluginPackageError::ManifestSchemaInvalid { .. }
                | PluginPackageError::ConfigSchemaInvalid { .. }
                | PluginPackageError::PayloadContractInvalid { .. },
        } => Ok(PluginOperationFailure::Invalid { code: error.code() }),
        _ => Err(error),
    }
}
