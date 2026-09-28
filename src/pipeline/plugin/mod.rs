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

//! Owns every Plugin Instance child in one applied Pipeline.
//!
//! The instance collection owns one child state machine per validated identity.

use std::error::Error;
use std::fmt;

use crate::identifiers::PluginInstanceId;

#[cfg(feature = "repository-test-support")]
pub(in crate::pipeline) mod contract_test_support;
mod control;
mod controlled_lifecycle;
mod instances;
mod lifecycle;
mod output;

pub(in crate::pipeline) use control::{
    PluginControlLauncher, PluginControlServer, PluginControlServerError,
};
pub(in crate::pipeline) use controlled_lifecycle::ControlledPluginLaunch;
pub(in crate::pipeline) use instances::{PluginInstances, PluginRetirementEvent};
pub(crate) use lifecycle::{PluginBells, PluginLaunch, PluginRetryBackoff, SinkChannel};

use lifecycle::PluginLifecycleError;

/// One lifecycle transition selected by the Pipeline Controller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PluginInstanceEvent {
    StatusChanged,
    /// An actually created child failed and has been reaped.
    ProcessFailed(PluginInstanceId),
    RestartDue(PluginInstanceId),
}

/// A fatal ownership or operating-system failure of one Plugin Instance.
#[derive(Debug)]
pub(crate) struct PluginInstanceError {
    identity: PluginInstanceId,
    source: PluginLifecycleError,
}

impl PluginInstanceError {
    fn new(identity: PluginInstanceId, source: PluginLifecycleError) -> Self {
        Self { identity, source }
    }
}

impl fmt::Display for PluginInstanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Plugin Instance lifecycle failed: {}",
            self.identity.as_str()
        )
    }
}

impl Error for PluginInstanceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(test)]
pub(in crate::pipeline) mod test_support;

pub(in crate::pipeline) use controlled_lifecycle::HandoffReadiness;

pub(in crate::pipeline) mod metrics;
