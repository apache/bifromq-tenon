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

//! Aggregate process limits at the Runner's operating-system boundary.
//!
//! A launch owns its resource group alongside its process tree. Preparation
//! completes before exec; explicit cleanup proves the group empty before
//! runtime files are removed. Startup recovery only visits Tenon's workload
//! subtree in the deployment-provided scope. No cgroup path is public config.
//! Limits are derived from verified Documents, never retained as shadow state.

use crate::tenon_document::VerifiedTenonDocument;
use std::io;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(not(target_os = "linux"))]
mod unsupported;
#[cfg(not(target_os = "linux"))]
use unsupported as platform;

pub(in crate::runner) use platform::{
    PipelineResourceGroup, RunnerResources, available_cpu_count, requires_replacement,
};

/// A preparation error keeps any group for cleanup.
pub(in crate::runner) struct ResourcePreparationError {
    pub(in crate::runner) failure: io::Error,
    pub(in crate::runner) group: Option<PipelineResourceGroup>,
}

impl From<io::Error> for ResourcePreparationError {
    fn from(failure: io::Error) -> Self {
        Self {
            failure,
            group: None,
        }
    }
}

/// The interpretation of limits on one live, applied Document.
#[derive(Clone, Copy)]
pub(in crate::runner) enum ResourceLimitsState {
    #[cfg_attr(
        not(target_os = "linux"),
        expect(dead_code, reason = "Linux is the only enforced backend")
    )]
    Enforced,
    #[cfg_attr(
        target_os = "linux",
        expect(dead_code, reason = "Linux never ignores requested limits")
    )]
    Ignored,
}

impl ResourceLimitsState {
    pub(in crate::runner) const fn name(self) -> &'static str {
        match self {
            Self::Enforced => "enforced",
            Self::Ignored => "ignored",
        }
    }

    pub(in crate::runner) const fn reason(self) -> Option<&'static str> {
        match self {
            Self::Enforced => None,
            Self::Ignored => Some("platform_unsupported"),
        }
    }
}

pub(in crate::runner) fn applied_limits(
    document: &VerifiedTenonDocument,
) -> Option<ResourceLimitsState> {
    document
        .resource_limits()
        .filter(|limits| !limits.is_empty())
        .map(|_| platform::APPLIED_STATE)
}

#[cfg(test)]
pub(in crate::runner) mod test_support {
    use super::RunnerResources;
    use std::sync::Arc;

    /// Lifecycle unit fixtures have no authority to rearrange the test runner's cgroups.
    pub(in crate::runner) fn unavailable() -> Arc<RunnerResources> {
        #[cfg(target_os = "linux")]
        let resources = RunnerResources::Unavailable {
            failure: std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "No delegated scope in this unit fixture",
            ),
            _claim: None,
        };
        #[cfg(not(target_os = "linux"))]
        let resources = RunnerResources;
        Arc::new(resources)
    }
}
