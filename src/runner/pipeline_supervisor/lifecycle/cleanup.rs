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

//! Tries cleanup again until cleanup is completed or Runner shutdown starts.

use super::{
    PipelineLifecycleContext, PipelineLifecycleIdentity, RetryBackoffSequence,
    RunnerPipelineLifecycleError, publish_state,
};
use crate::error::ErrorChain;
use crate::identifiers::TenonDocumentId;
use crate::runner::management::{PipelineAttemptError, PipelineLifecycleState};
use crate::runner::pipeline::{
    PipelineCleanupError, PipelineLaunchResources, RunnerPipelineProcess,
    cleanup_pipeline_directory,
};
use std::path::Path;
use tokio::time;

pub(super) enum CleanupTarget<'a> {
    Process(&'a mut RunnerPipelineProcess),
    Launch(&'a mut PipelineLaunchResources),
    Directory(&'a Path),
}

impl CleanupTarget<'_> {
    async fn run(&mut self) -> Result<(), PipelineCleanupError> {
        match self {
            Self::Process(process) => process.cleanup().await,
            Self::Launch(resources) => resources.cleanup().await,
            Self::Directory(path) => cleanup_pipeline_directory(path.to_path_buf()).await,
        }
    }
}

impl PipelineLifecycleContext {
    pub(super) async fn cleanup_with_retry(
        &self,
        document_id: &TenonDocumentId,
        identity: &PipelineLifecycleIdentity,
        mut target: CleanupTarget<'_>,
    ) -> Result<(), RunnerPipelineLifecycleError> {
        let mut runner_shutdown = self.runner_shutdown.subscribe();
        let mut backoff = RetryBackoffSequence::new(
            self.config.retry_initial_delay(),
            self.config.retry_maximum_delay(),
        );
        // The first retry also runs during Runner shutdown.
        let mut result = target.run().await;
        if let Err(error) = result {
            self.report_cleanup_failure(document_id, identity, error)
                .await?;
            result = target.run().await;
        }
        while let Err(error) = result {
            let error = self
                .report_cleanup_failure(document_id, identity, error)
                .await?;
            tokio::select! {
                biased;
                _ = runner_shutdown.wait_for(|requested| *requested) => {
                    return Err(RunnerPipelineLifecycleError::Cleanup(error));
                }
                () = time::sleep(backoff.take_next()) => {}
            }
            result = target.run().await;
        }
        Ok(())
    }

    async fn report_cleanup_failure(
        &self,
        document_id: &TenonDocumentId,
        identity: &PipelineLifecycleIdentity,
        error: PipelineCleanupError,
    ) -> Result<PipelineCleanupError, RunnerPipelineLifecycleError> {
        if matches!(error, PipelineCleanupError::Task(_)) {
            return Err(RunnerPipelineLifecycleError::Cleanup(error));
        }
        eprintln!(
            "runner.pipeline_cleanup_failed: Pipeline {document_id}: {}",
            ErrorChain(&error)
        );
        if let Err(primary) = publish_state(
            &self.state_updates,
            document_id,
            identity,
            PipelineLifecycleState::RestartBackoff(PipelineAttemptError {
                document_etag: None,
                code: "runner.pipeline_cleanup_failed",
                message: "Pipeline cleanup error",
            }),
        )
        .await
        {
            return Err(RunnerPipelineLifecycleError::CleanupAfterFailure {
                primary: Box::new(primary),
                cleanup: error,
            });
        }
        Ok(error)
    }
}
