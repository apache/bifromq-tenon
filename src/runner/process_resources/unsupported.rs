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

//! Development platforms accept valid limits without changing execution.

use super::{ResourceLimitsState, ResourcePreparationError};
use crate::tenon_document::VerifiedTenonDocument;
use std::io;
use std::num::NonZeroUsize;
use tokio::process::Command;

pub(super) const APPLIED_STATE: ResourceLimitsState = ResourceLimitsState::Ignored;

/// Observes the CPU count on platforms without CPU-time quota enforcement.
pub(in crate::runner) fn available_cpu_count() -> io::Result<NonZeroUsize> {
    std::thread::available_parallelism()
}

pub(in crate::runner) struct PipelineResourceGroup;

impl PipelineResourceGroup {
    pub(in crate::runner) fn prepare(
        _resources: &RunnerResources,
        _command: &mut Command,
        _document: &VerifiedTenonDocument,
        _launch_id: &[u8],
    ) -> Result<Option<Self>, ResourcePreparationError> {
        Ok(None)
    }

    pub(in crate::runner) async fn cleanup(&self) -> io::Result<()> {
        Ok(())
    }

    pub(in crate::runner) fn discard_empty(&self) -> io::Result<()> {
        Ok(())
    }
}

pub(in crate::runner) fn requires_replacement(
    _current: &VerifiedTenonDocument,
    _target: &VerifiedTenonDocument,
) -> bool {
    false
}

#[derive(Debug)]
pub(in crate::runner) struct RunnerResources;

impl RunnerResources {
    pub(in crate::runner) fn initialize() -> io::Result<Self> {
        Ok(Self)
    }

    pub(in crate::runner) async fn recover_stale_groups(&self) -> io::Result<()> {
        Ok(())
    }
}
