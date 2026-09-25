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

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::super::control::{
    AttachedPluginControl, PluginControlLauncher, PluginControlSessionError, ReadyPluginControl,
};
use super::super::lifecycle::{
    PluginLaunch, PluginLifecycleError, PluginProcess, PluginStartFailure, PluginStartupWriter,
    StartFailureCleanup,
};
use super::active::ReadyControlledPlugin;
use crate::payload_contract::PluginInterface;

type AttachFuture = Pin<
    Box<
        dyn Future<
                Output = Result<
                    AttachedPluginControl,
                    super::super::control::PluginControlAttachmentError,
                >,
            > + Send,
    >,
>;
type ReadyFuture =
    Pin<Box<dyn Future<Output = Result<ReadyPluginControl, PluginControlSessionError>> + Send>>;

/// Target launch material for one UDS-controlled Plugin Instance.
pub(in crate::pipeline) struct ControlledPluginLaunch<'a> {
    pub(super) process: PluginLaunch<'a>,
    pub(super) interface: PluginInterface,
    pub(in crate::pipeline::plugin) control: &'a PluginControlLauncher,
}

impl<'a> ControlledPluginLaunch<'a> {
    /// Combines process material with this launch's bound interfaces and control endpoint.
    pub(in crate::pipeline) const fn new(
        process: PluginLaunch<'a>,
        interface: PluginInterface,
        control: &'a PluginControlLauncher,
    ) -> Self {
        Self {
            process,
            interface,
            control,
        }
    }
}

impl fmt::Debug for ControlledPluginLaunch<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlledPluginLaunch")
            .field("interface", &self.interface)
            .field("control", &self.control)
            .finish_non_exhaustive()
    }
}

pub(super) struct ControlledPluginStartup {
    pub(super) process: Option<PluginProcess>,
    pub(super) config: PluginStartupWriter,
    pub(super) control: ControlStartup,
}

impl ControlledPluginStartup {
    pub(super) fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<ControlledStartupOutcome, PluginLifecycleError>> {
        let process = self
            .process
            .as_mut()
            .ok_or(PluginLifecycleError::InvalidTransition)?;
        match process.poll_exit(context) {
            Poll::Ready(Ok(_)) => {
                let _process = self
                    .process
                    .take()
                    .ok_or(PluginLifecycleError::InvalidTransition)?;
                return Poll::Ready(Ok(ControlledStartupOutcome::Failed(
                    PluginStartFailure::ExitedBeforeReady,
                )));
            }
            Poll::Ready(Err(source)) => {
                return Poll::Ready(Err(PluginLifecycleError::Status(source)));
            }
            Poll::Pending => {}
        }
        match process.poll_config(&mut self.config, context) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(source)) => {
                return Poll::Ready(self.cleanup(PluginStartFailure::ConfigWrite(source)));
            }
            Poll::Pending => return Poll::Pending,
        }

        loop {
            match &mut self.control {
                ControlStartup::Attaching(attach) => match attach.as_mut().poll(context) {
                    Poll::Ready(Ok(attached)) => {
                        self.control =
                            ControlStartup::WaitingForReady(Box::pin(attached.wait_for_ready()));
                    }
                    Poll::Ready(Err(source)) => {
                        return Poll::Ready(
                            self.cleanup(PluginStartFailure::ControlAttachment(source)),
                        );
                    }
                    Poll::Pending => return Poll::Pending,
                },
                ControlStartup::WaitingForReady(ready) => match ready.as_mut().poll(context) {
                    Poll::Ready(Ok(control)) => {
                        let process = self
                            .process
                            .take()
                            .ok_or(PluginLifecycleError::InvalidTransition)?;
                        return Poll::Ready(Ok(ControlledStartupOutcome::Ready(Box::new(
                            ReadyControlledPlugin { process, control },
                        ))));
                    }
                    Poll::Ready(Err(source)) => {
                        return Poll::Ready(
                            self.cleanup(PluginStartFailure::ControlSession(source)),
                        );
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }

    pub(super) fn request_force_stop(&mut self) -> Result<(), PluginLifecycleError> {
        self.process
            .as_mut()
            .map_or(Ok(()), PluginProcess::request_force_stop)
            .map_err(PluginLifecycleError::Stop)
    }

    /// Reports whether this pending launch was already asked to stop.
    pub(super) fn stop_requested(&self) -> bool {
        self.process
            .as_ref()
            .is_some_and(PluginProcess::stop_requested)
    }

    pub(super) fn poll_stopped(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), PluginLifecycleError>> {
        let Some(process) = self.process.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        match process.poll_exit(context) {
            Poll::Ready(Ok(_)) => {
                let _process = self.process.take();
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(source)) => Poll::Ready(Err(PluginLifecycleError::Status(source))),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(super) async fn reap_after_stop(&mut self) -> Result<(), PluginLifecycleError> {
        match self.process.as_mut() {
            Some(process) => process
                .reap_after_stop()
                .await
                .map_err(PluginLifecycleError::Stop),
            None => Ok(()),
        }
    }

    fn cleanup(
        &mut self,
        failure: PluginStartFailure,
    ) -> Result<ControlledStartupOutcome, PluginLifecycleError> {
        let process = self
            .process
            .take()
            .ok_or(PluginLifecycleError::InvalidTransition)?;
        Ok(ControlledStartupOutcome::Cleanup(StartFailureCleanup::new(
            process.into_child(),
            failure,
        )))
    }
}

impl fmt::Debug for ControlledPluginStartup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlledPluginStartup")
            .field(
                "process_id",
                &self.process.as_ref().and_then(PluginProcess::process_id),
            )
            .field("config", &self.config)
            .field("control", &self.control)
            .finish()
    }
}

pub(super) enum ControlStartup {
    Attaching(AttachFuture),
    WaitingForReady(ReadyFuture),
}

impl fmt::Debug for ControlStartup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Attaching(_) => "Attaching",
            Self::WaitingForReady(_) => "WaitingForReady",
        })
    }
}

pub(super) enum ControlledStartupOutcome {
    Ready(Box<ReadyControlledPlugin>),
    Failed(PluginStartFailure),
    Cleanup(StartFailureCleanup),
}
