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

//! Errors returned while applying or stopping one Pipeline revision.

use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;

use tokio::task::JoinError;

use crate::lua::LuaVmErrorKind;
use crate::pipeline::channel::{FlowChannelCommandControlError, FlowChannelError};
use crate::pipeline::plugin::PluginInstanceError;
use crate::pipeline::runtime::PipelineRuntimeError;
use tenon_ipc::bell::BellError;
use tenon_ipc::queue::QueueRuntimeError;

/// A fatal failure that prevents a target revision from becoming applied.
#[derive(Debug)]
pub(crate) enum PipelineReconfigureError {
    DocumentIdentityMismatch,
    SourceFailedDuringReconfigure(crate::identifiers::PluginInstanceId),
    DataPlaneFailure(Box<PipelineRuntimeError>),
    DataPlaneStopped,
    BlockingTask(JoinError),
    DirectoryCreate {
        path: PathBuf,
        source: io::Error,
    },
    DirectoryPermission {
        path: PathBuf,
        source: io::Error,
    },
    DirectoryRemove {
        path: PathBuf,
        source: io::Error,
    },
    QueueCreate {
        path: PathBuf,
        source: QueueRuntimeError,
    },
    QueueRemove {
        path: PathBuf,
        source: std::io::Error,
    },
    QueueRename {
        from: PathBuf,
        to: PathBuf,
        source: io::Error,
    },
    QueueOpen {
        path: PathBuf,
        source: QueueRuntimeError,
    },
    BellRegionCreate {
        path: PathBuf,
        source: BellError,
    },
    BellRegionOpen {
        path: PathBuf,
        source: BellError,
    },
    RuntimeStart(PipelineRuntimeError),
    RuntimeTransition(Box<PipelineRuntimeError>),
    PluginInstanceLifecycle(PluginInstanceError),
    ResourceLimitExceeded,
    InternalInvariantViolation,
}

impl PipelineReconfigureError {
    pub(super) fn is_definition_control_failure(&self) -> bool {
        matches!(
            self,
            Self::RuntimeTransition(source)
                if matches!(
                    source.as_ref(),
                    PipelineRuntimeError::InternalEventChannelClosed
                        | PipelineRuntimeError::FlowChannelCommandControl { .. }
                )
        )
    }

    pub(super) fn is_stopped_definition(&self) -> bool {
        matches!(
            self,
            Self::RuntimeTransition(source)
                if matches!(
                    source.as_ref(),
                    PipelineRuntimeError::InternalEventChannelClosed
                        | PipelineRuntimeError::FlowChannelReplacementPreparation {
                            kind: LuaVmErrorKind::ExecutionStopped,
                            ..
                        }
                        | PipelineRuntimeError::FlowChannelCommandControl {
                            source: FlowChannelCommandControlError::WorkerDisconnected,
                            ..
                        }
                )
        )
    }

    pub(super) fn is_cancellation_exit(&self) -> bool {
        let runtime = match self {
            Self::BlockingTask(source) => return source.is_cancelled(),
            Self::RuntimeStart(source) => source,
            Self::RuntimeTransition(source) => source.as_ref(),
            _ => return false,
        };
        matches!(
            runtime,
            PipelineRuntimeError::FlowChannelOpen {
                source: FlowChannelError::LuaVmLoad {
                    kind: LuaVmErrorKind::ExecutionStopped,
                },
                ..
            }
        )
    }
}

impl fmt::Display for PipelineReconfigureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceFailedDuringReconfigure(id) => write!(
                formatter,
                "Source Plugin instance {id} failed during reconfiguration"
            ),
            Self::DocumentIdentityMismatch => {
                formatter.write_str("Target Tenon Document identity does not match the Pipeline")
            }
            Self::DataPlaneFailure(_) => {
                formatter.write_str("Pipeline data plane failed during reconfiguration")
            }
            Self::DataPlaneStopped => {
                formatter.write_str("Pipeline data plane stopped during reconfiguration")
            }
            Self::BlockingTask(_) => {
                formatter.write_str("Pipeline blocking reconfiguration task failed")
            }
            Self::DirectoryCreate { path, .. } => {
                write!(
                    formatter,
                    "Pipeline runtime directory could not be created: {}",
                    path.display()
                )
            }
            Self::DirectoryPermission { path, .. } => {
                write!(
                    formatter,
                    "Pipeline runtime directory permissions could not be fixed: {}",
                    path.display()
                )
            }
            Self::DirectoryRemove { path, .. } => {
                write!(
                    formatter,
                    "Pipeline runtime directory could not be removed: {}",
                    path.display()
                )
            }
            Self::QueueCreate { path, .. } => {
                write!(
                    formatter,
                    "Pipeline Queue could not be created: {}",
                    path.display()
                )
            }
            Self::QueueRemove { path, .. } => {
                write!(formatter, "Queue '{}' could not be removed", path.display())
            }
            Self::QueueRename { from, to, .. } => write!(
                formatter,
                "Pipeline Queue could not be moved from {} to {}",
                from.display(),
                to.display()
            ),
            Self::QueueOpen { path, .. } => {
                write!(
                    formatter,
                    "Pipeline Queue could not be opened: {}",
                    path.display()
                )
            }
            Self::BellRegionCreate { path, .. } => {
                write!(
                    formatter,
                    "Pipeline Bell Region could not be created: {}",
                    path.display()
                )
            }
            Self::BellRegionOpen { path, .. } => {
                write!(
                    formatter,
                    "Pipeline Bell Region could not be opened: {}",
                    path.display()
                )
            }
            Self::RuntimeStart(_) => {
                formatter.write_str("Pipeline data plane could not be started")
            }
            Self::RuntimeTransition(_) => {
                formatter.write_str("Pipeline data plane could not complete its runtime transition")
            }
            Self::PluginInstanceLifecycle(_) => {
                formatter.write_str("Pipeline Plugin lifecycle failed")
            }
            Self::ResourceLimitExceeded => {
                formatter.write_str("Pipeline reconfiguration resource limit was exceeded")
            }
            Self::InternalInvariantViolation => {
                formatter.write_str("Pipeline reconfiguration invariant was violated")
            }
        }
    }
}

impl Error for PipelineReconfigureError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DataPlaneFailure(source) => Some(source.as_ref()),
            Self::BlockingTask(source) => Some(source),
            Self::DirectoryCreate { source, .. }
            | Self::DirectoryPermission { source, .. }
            | Self::DirectoryRemove { source, .. } => Some(source),
            Self::QueueCreate { source, .. } | Self::QueueOpen { source, .. } => Some(source),
            Self::BellRegionCreate { source, .. } => Some(source),
            Self::BellRegionOpen { source, .. } => Some(source),
            Self::QueueRename { source, .. } | Self::QueueRemove { source, .. } => Some(source),
            Self::RuntimeStart(source) => Some(source),
            Self::RuntimeTransition(source) => Some(source.as_ref()),
            Self::PluginInstanceLifecycle(source) => Some(source),
            Self::DocumentIdentityMismatch
            | Self::SourceFailedDuringReconfigure(_)
            | Self::DataPlaneStopped
            | Self::ResourceLimitExceeded
            | Self::InternalInvariantViolation => None,
        }
    }
}
