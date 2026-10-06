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

//! Restores Runner desired state from the private Stores.
//!
//! Startup validates saved Documents, then restores the Program Store.
//! Invalid Documents or Store root and cleanup failures stop startup.
//! Package access or validation failure stops startup and keeps the saved input.
//! Runtime files are reconstructed from the saved packages.
//! No new UDS or child process is created before recovery completes.

use crate::config::RunnerConfig;
use crate::identifiers::TenonDocumentId;
use crate::runner::document_store::{TenonDocumentStore, TenonDocumentStoreError};
use crate::runner::extensions::ArtifactProtection;
use crate::runner::plugin::store::{PluginProgramStore, PluginStoreError};
use crate::runner::state_directory::RunnerStateLayout;
use crate::tenon_document::verified::VerifiedTenonDocument;
use crate::tenon_document::{
    TenonDocumentSyntaxError, TenonDocumentVerificationError,
    TenonDocumentVerifierInitializationError, UnverifiedTenonDocument,
};
use sha2::{Digest as _, Sha256};
use std::error::Error;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use zeroize::Zeroizing;

/// One complete startup view of desired Tenon Documents.
#[derive(Debug)]
pub(crate) struct RecoveredRunnerState {
    documents: Box<[RecoveredTenonDocument]>,
    programs: PluginProgramStore,
}

impl RecoveredRunnerState {
    #[must_use]
    pub(crate) fn into_parts(self) -> (Box<[RecoveredTenonDocument]>, PluginProgramStore) {
        (self.documents, self.programs)
    }
}

/// One statically verified desired document restored from durable state.
#[derive(Debug)]
pub(crate) struct RecoveredTenonDocument {
    // The exact durable source is an API/ETag artifact and cannot be rebuilt
    // from the separately owned verified semantic model.
    source: Zeroizing<Box<[u8]>>,
    document: Arc<VerifiedTenonDocument>,
}

impl RecoveredTenonDocument {
    /// Transfers the original source and verified model.
    #[must_use]
    pub(crate) fn into_parts(self) -> (Zeroizing<Box<[u8]>>, Arc<VerifiedTenonDocument>) {
        (self.source, self.document)
    }
}

/// Restores the complete immutable startup facts before any child is launched.
///
/// # Errors
///
/// Returns [`RunnerRecoveryError`] when a root Store operation fails, a
/// committed Document cannot be parsed or verified, its filename identity is
/// inconsistent, or Program recovery or cleanup cannot complete.
/// Package access or validation failure keeps the saved input.
pub(crate) fn recover(
    config: &RunnerConfig,
    state_layout: &RunnerStateLayout,
    protection: &Arc<dyn ArtifactProtection>,
    runtime_directory: &Path,
) -> Result<RecoveredRunnerState, RunnerRecoveryError> {
    let verifier = config
        .tenon_document_verifier()
        .map_err(RunnerRecoveryError::VerifierInitialization)?;
    let store = TenonDocumentStore::new(state_layout.tenon_document_store_directory());
    store
        .recover_stale_temporary_files()
        .map_err(RunnerRecoveryError::TenonDocumentStore)?;

    let mut verified = Vec::new();
    for source in store
        .sources(Arc::clone(protection))
        .map_err(RunnerRecoveryError::TenonDocumentStore)?
    {
        let source = source.map_err(RunnerRecoveryError::TenonDocumentStore)?;
        let entry = source.committed_file_name().into_boxed_str();
        let document = match UnverifiedTenonDocument::parse(source.source()) {
            Ok(document) => document,
            Err(source) => {
                return Err(RunnerRecoveryError::TenonDocumentSyntax { entry, source });
            }
        };
        let document = match verifier.verify(document) {
            Ok(document) => document,
            Err(source) => {
                return Err(RunnerRecoveryError::TenonDocumentVerification { entry, source });
            }
        };
        let id_sha256: [u8; 32] = Sha256::digest(document.id().as_str().as_bytes()).into();
        if source.expected_id_sha256() != &id_sha256 {
            return Err(RunnerRecoveryError::TenonDocumentIdentityMismatch {
                entry,
                document_id: document.id().clone(),
            });
        }
        verified.push((source.into_source(), Arc::new(document)));
    }
    verified.sort_unstable_by(|left, right| left.1.id().as_str().cmp(right.1.id().as_str()));

    let programs = PluginProgramStore::recover(
        state_layout.plugin_program_store_directory(),
        runtime_directory.to_path_buf(),
        Arc::clone(protection),
    )
    .map_err(RunnerRecoveryError::PluginStore)?;
    let documents = verified
        .into_iter()
        .map(|(source, document)| RecoveredTenonDocument { source, document })
        .collect::<Vec<_>>()
        .into_boxed_slice();

    Ok(RecoveredRunnerState {
        documents,
        programs,
    })
}

/// A fatal private-state failure discovered before the Runner starts serving.
#[derive(Debug)]
pub(crate) enum RunnerRecoveryError {
    /// The static Tenon Document verifier could not be constructed.
    VerifierInitialization(TenonDocumentVerifierInitializationError),
    /// Tenon Document Store recovery or iteration failed.
    TenonDocumentStore(TenonDocumentStoreError),
    /// One committed entry is not valid Tenon Document syntax.
    TenonDocumentSyntax {
        /// The exact committed filename being restored.
        entry: Box<str>,
        /// The underlying syntax failure.
        source: TenonDocumentSyntaxError,
    },
    /// One committed entry failed static verification.
    TenonDocumentVerification {
        /// The exact committed filename being restored.
        entry: Box<str>,
        /// The underlying static verification failure.
        source: TenonDocumentVerificationError,
    },
    /// A committed filename does not identify the Document it contains.
    TenonDocumentIdentityMismatch {
        /// The exact committed filename being restored.
        entry: Box<str>,
        /// The parsed Document identity found in that entry.
        document_id: TenonDocumentId,
    },
    /// Installed Plugin enumeration or package validation failed.
    PluginStore(PluginStoreError),
}

impl RunnerRecoveryError {
    /// Returns the stable process diagnostic code for this startup failure.
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::VerifierInitialization(_) => "runner.tenon_document_verifier_invalid",
            Self::TenonDocumentStore(error) => error.code(),
            Self::TenonDocumentSyntax { .. } => "runner.tenon_document_syntax_invalid",
            Self::TenonDocumentVerification { .. } => "runner.tenon_document_verification_failed",
            Self::TenonDocumentIdentityMismatch { .. } => "runner.tenon_document_identity_mismatch",
            Self::PluginStore(error) => error.code(),
        }
    }
}

impl fmt::Display for RunnerRecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VerifierInitialization(_) => {
                formatter.write_str("Tenon Document verifier could not be initialized")
            }
            Self::TenonDocumentStore(_) => {
                formatter.write_str("Tenon Document Store recovery failed")
            }
            Self::TenonDocumentSyntax { entry, .. } => {
                write!(
                    formatter,
                    "Stored Tenon Document syntax is invalid: {entry}"
                )
            }
            Self::TenonDocumentVerification { entry, .. } => {
                write!(
                    formatter,
                    "Stored Tenon Document static verification failed: {entry}"
                )
            }
            Self::TenonDocumentIdentityMismatch { entry, document_id } => {
                write!(
                    formatter,
                    "Stored Tenon Document identity does not match its file name: {entry} contains {document_id}"
                )
            }
            Self::PluginStore(_) => formatter.write_str("Plugin Store recovery failed"),
        }
    }
}

impl Error for RunnerRecoveryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::VerifierInitialization(source) => Some(source),
            Self::TenonDocumentStore(source) => Some(source),
            Self::TenonDocumentSyntax { source, .. } => Some(source),
            Self::TenonDocumentVerification { source, .. } => Some(source),
            Self::PluginStore(source) => Some(source),
            Self::TenonDocumentIdentityMismatch { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;
