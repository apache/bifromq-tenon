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

//! Executes one ownership-ordered cutover for every resource combination.
//!
//! Selected Sources quiesce together while old consumers remain available.
//! Actual Completion consumption, Channel exit, route release, pump exit and
//! child reap each precede the file mutation they protect. No Ready barrier or
//! graph order is imposed on shared or cyclic Source/Sink bindings.

use crate::pipeline::plugin::HandoffReadiness;
use std::collections::BTreeSet;
use std::future::{Future as _, poll_fn};
use std::pin::pin;
use std::task::{Context, Poll};

use super::abort_with_unreaped_plugins;
use super::runtime_status::handle_reconfigure_plugin_event;
use super::{ActivePipeline, StagedResourceChanges, StartedResourceChanges};
use crate::identifiers::{FlowId, PluginInstanceId};
use crate::pipeline::plugin::{PluginRetirementEvent, PluginRetryBackoff};
use crate::pipeline::reconfigure::operation::{
    BlockingReconfigureJob, ReconfigureProgress, spawn_blocking_reconfigure_work, wait_for_shutdown,
};
use crate::pipeline::reconfigure::plan::{FlowChange, ResourceMutation};
use crate::pipeline::reconfigure::{PipelineReconfigureError, PipelineReconfigurer};
use crate::pipeline::runtime::{
    PausedFlowDefinitionReplacement, PipelineDrainObservation, PipelineRuntimeError,
    PreparedFlowDefinitionReplacement,
};

type DefinitionCutoverResult = (
    Vec<PreparedFlowDefinitionReplacement>,
    Result<(), PipelineRuntimeError>,
);
type CutoverResources = (StartedResourceChanges, Vec<PausedFlowDefinitionReplacement>);

impl PipelineReconfigurer {
    pub(in crate::pipeline::reconfigure) async fn handoff_instances(
        &mut self,
        staged: StagedResourceChanges,
        mut channels: Vec<PreparedFlowDefinitionReplacement>,
    ) -> Result<ReconfigureProgress<CutoverResources>, PipelineReconfigureError> {
        let backoff = self.environment.retry_backoff();
        let backoff = PluginRetryBackoff::new(backoff.initial_delay(), backoff.maximum_delay());
        let mut started = staged.begin_cutover(backoff);
        if self.current.is_none() {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut self.shutdown_receiver) => return Ok(ReconfigureProgress::Stopped),
                result = started.staged.runtime.bind() => result.map_err(transition_error)?,
            }
            started.launch_instances(|_| true, &self.control, backoff, &self.diagnostics);
            return Ok(ReconfigureProgress::Completed((started, Vec::new())));
        }
        let mut definition_work = None;
        let mut file_work = None;
        let mut definitions = Vec::new();
        let result = self
            .handoff_old_resources(
                &mut started,
                &mut channels,
                &mut definitions,
                &mut definition_work,
                &mut file_work,
            )
            .await;
        if let Ok(ReconfigureProgress::Completed(())) = result {
            return Ok(ReconfigureProgress::Completed((started, definitions)));
        }
        let file_cleanup = match file_work {
            Some(mut work) => work.cancel_and_discard().await,
            None => Ok(()),
        };
        if let Err(error) = started.force_stop().await {
            abort_with_unreaped_plugins(&error);
        }
        let cleanup = self
            .terminate_staged_resources(&mut Some(started.staged))
            .await;
        // Runtime Stop closes a pending Channel cutover handshake. Join its
        // blocking coordinator only after every worker has received that Stop.
        let definition_cleanup = match definition_work {
            Some(mut work) => match work.wait().await {
                Ok((prepared, result)) => {
                    drop(prepared);
                    result.map_err(transition_error)
                }
                Err(error) => Err(error),
            },
            None => Ok(()),
        };
        drop((definitions, channels));
        cleanup?;
        file_cleanup?;
        if let Err(error) = definition_cleanup
            && !error.is_stopped_definition()
        {
            return Err(error);
        }
        result.map(|_| ReconfigureProgress::Stopped)
    }

    #[expect(
        clippy::expect_used,
        reason = "Cutover retains current until adoption or terminal cleanup"
    )]
    async fn handoff_old_resources(
        &mut self,
        started: &mut StartedResourceChanges,
        channels: &mut Vec<PreparedFlowDefinitionReplacement>,
        definitions: &mut Vec<PausedFlowDefinitionReplacement>,
        definition_work: &mut Option<BlockingReconfigureJob<DefinitionCutoverResult>>,
        file_work: &mut Option<BlockingReconfigureJob<()>>,
    ) -> Result<ReconfigureProgress<()>, PipelineReconfigureError> {
        let backoff = self.environment.retry_backoff();
        let backoff = PluginRetryBackoff::new(backoff.initial_delay(), backoff.maximum_delay());
        let selected: BTreeSet<_> = started
            .staged
            .changes
            .processes
            .iter()
            .filter(|(_, mutation)| *mutation != ResourceMutation::Add)
            .map(|(id, _)| id.clone())
            .collect();
        // An Instance the target revision no longer launches can never release an
        // Egress record a Channel already committed to it: no successor will own
        // that identity. Recording the departure before any wait starts is what
        // keeps a settlement from waiting on a Sink that is gone for good.
        let departing: BTreeSet<_> = selected
            .iter()
            .filter(|id| {
                started.staged.changes.process_mutation(id) == Some(ResourceMutation::Remove)
            })
            .cloned()
            .collect();
        if !departing.is_empty() {
            self.current
                .as_ref()
                .expect("Cutover requires current")
                .runtime
                .depart_egress_targets(&departing);
        }
        let current = self.current.as_mut().expect("Cutover requires current");
        let readiness = current
            .plugins
            .instances
            .handoff_readiness(&selected)
            .await
            .map_err(PipelineReconfigureError::PluginInstanceLifecycle)?;
        for (id, readiness) in &readiness {
            if *readiness == HandoffReadiness::FailureObserved
                && current
                    .target
                    .document()
                    .flows()
                    .values()
                    .any(|flow| flow.source() == id)
            {
                return Err(PipelineReconfigureError::SourceFailedDuringReconfigure(
                    id.clone(),
                ));
            }
        }
        // Only an identity nobody owns any more may be retired on sight. A child
        // that is still starting is alive and still owes the ordered handoff, so
        // retiring it here would tear down a peer the data plane still waits on.
        let healthy = readiness
            .into_iter()
            .filter_map(|(id, readiness)| {
                matches!(
                    readiness,
                    HandoffReadiness::Ready | HandoffReadiness::NotReady
                )
                .then_some(id)
            })
            .collect();
        let failed: BTreeSet<_> = selected.difference(&healthy).cloned().collect();
        current
            .plugins
            .instances
            .request_retirement(&failed)
            .map_err(PipelineReconfigureError::PluginInstanceLifecycle)?;
        if matches!(
            self.wait_handoff(started, &healthy, &failed, None, |_, _| {
                if failed.is_empty() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }
        // A target can start early only when every interface file it opens is
        // already compatible. A dual-interface child cannot open old Source
        // files merely to provide an otherwise useful Sink consumer.
        let early: BTreeSet<_> = started
            .staged
            .changes
            .launched_instances()
            .filter(|id| failed.contains(*id) && self.instance_files_are_retained(started, id))
            .cloned()
            .collect();
        started.launch_instances(
            |id| early.contains(id),
            &self.control,
            backoff,
            &self.diagnostics,
        );
        // A live old session that has not finished starting still owes the
        // ordered handoff: it is the only peer that can release the Egress
        // records a Channel already committed to it. Finish its startup before
        // asking it to quiesce, because quiescing a pending launch force stops
        // the very child that still owes those releases. Not being Ready never
        // means owing nothing, whichever interface the child declares.
        if matches!(
            self.wait_handoff(started, &healthy, &BTreeSet::new(), None, |current, _| {
                if current
                    .plugins
                    .instances
                    .handoff_peers_are_started(&healthy)
                {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }
        self.current
            .as_mut()
            .expect("Cutover retains current")
            .plugins
            .instances
            .request_source_quiesce(&healthy)
            .map_err(PipelineReconfigureError::PluginInstanceLifecycle)?;
        if matches!(
            self.wait_handoff(started, &healthy, &BTreeSet::new(), None, |current, _| {
                if current.plugins.instances.sources_are_quiesced(&healthy) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }
        let current = self.current.as_ref().expect("Cutover retains current");
        let mut finishes = current
            .target
            .document()
            .flows()
            .iter()
            .filter(|(_, flow)| healthy.contains(flow.source()))
            .map(|(id, _)| current.runtime.begin_source_session_finish(id))
            .collect::<Result<Vec<_>, _>>()
            .map_err(transition_error)?;
        if matches!(
            self.wait_handoff(started, &healthy, &BTreeSet::new(), None, |_, context| {
                for finish in &mut finishes {
                    match pin!(finish.wait()).poll(context) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(error)) => {
                            return Poll::Ready(Err(transition_error(error)));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                Poll::Ready(Ok(()))
            })
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }
        let mut prepared = std::mem::take(channels);
        *definition_work = Some(spawn_blocking_reconfigure_work(None, move || {
            let result = prepared
                .iter_mut()
                .try_for_each(PreparedFlowDefinitionReplacement::cutover_in_place);
            Ok((prepared, result))
        }));
        let mut cutover_result = None;
        if matches!(
            self.wait_handoff(started, &healthy, &BTreeSet::new(), None, |_, context| {
                match pin!(
                    definition_work
                        .as_mut()
                        .expect("Cutover job is owned until joined")
                        .wait()
                )
                .poll(context)
                {
                    Poll::Ready(result) => {
                        cutover_result = Some(result);
                        Poll::Ready(Ok(()))
                    }
                    Poll::Pending => Poll::Pending,
                }
            })
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }
        drop(definition_work.take());
        let (prepared, result) = cutover_result.expect("Completed cutover has one result")?;
        *channels = prepared;
        result.map_err(transition_error)?;
        definitions.extend(
            std::mem::take(channels)
                .into_iter()
                .map(PreparedFlowDefinitionReplacement::into_paused),
        );

        let retired_flows: BTreeSet<_> = started
            .staged
            .changes
            .flows
            .iter()
            .filter(|(_, change)| matches!(change, FlowChange::ReplaceQueues | FlowChange::Remove))
            .map(|(id, _)| id.clone())
            .collect();
        self.current
            .as_ref()
            .expect("Cutover retains current")
            .runtime
            .request_flow_retirement(&retired_flows);
        if matches!(
            self.wait_handoff(
                started,
                &healthy,
                &BTreeSet::new(),
                Some(&retired_flows),
                |current, context| {
                    retirement_completion(
                        current
                            .runtime
                            .poll_resource_retirement(&retired_flows, context),
                    )
                }
            )
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }
        self.current
            .as_mut()
            .expect("Cutover retains current")
            .runtime
            .finish_resource_retirement(&retired_flows)
            .await
            .map_err(transition_error)?;

        self.current
            .as_mut()
            .expect("Cutover retains current")
            .plugins
            .instances
            .request_retirement(&healthy)
            .map_err(PipelineReconfigureError::PluginInstanceLifecycle)?;
        if matches!(
            self.wait_handoff(started, &BTreeSet::new(), &healthy, None, |_, _| {
                if healthy.is_empty() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }

        *file_work = Some(
            started.staged.working_directory.begin_install(
                &started.staged.changes,
                &self
                    .current
                    .as_ref()
                    .expect("Cutover retains current")
                    .target,
            ),
        );
        let mut file_result = None;
        if matches!(
            self.wait_handoff(
                started,
                &BTreeSet::new(),
                &BTreeSet::new(),
                None,
                |_, context| {
                    match pin!(
                        file_work
                            .as_mut()
                            .expect("File work is owned until joined")
                            .wait()
                    )
                    .poll(context)
                    {
                        Poll::Ready(result) => {
                            file_result = Some(result);
                            Poll::Ready(Ok(()))
                        }
                        Poll::Pending => Poll::Pending,
                    }
                }
            )
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }
        drop(file_work.take());
        file_result.expect("Completed file work has one result")?;
        started.staged.working_directory.finish_install();
        let mut binding = pin!(started.staged.runtime.bind());
        if matches!(
            self.wait_handoff(
                started,
                &BTreeSet::new(),
                &BTreeSet::new(),
                None,
                |_, context| {
                    binding
                        .as_mut()
                        .poll(context)
                        .map(|result| result.map_err(transition_error))
                }
            )
            .await?,
            ReconfigureProgress::Stopped
        ) {
            return Ok(ReconfigureProgress::Stopped);
        }
        started.launch_instances(
            |id| !early.contains(id),
            &self.control,
            backoff,
            &self.diagnostics,
        );
        Ok(ReconfigureProgress::Completed(()))
    }

    #[expect(
        clippy::expect_used,
        reason = "Only current replacements require an early-launch check"
    )]
    fn instance_files_are_retained(
        &self,
        started: &StartedResourceChanges,
        id: &PluginInstanceId,
    ) -> bool {
        let changes = &started.staged.changes;
        if changes.sink_bells.contains_key(id)
            || changes
                .egress_queues
                .iter()
                .any(|queue| &queue.instance_id == id)
        {
            return false;
        }
        let current = self.current.as_ref().expect("Replacement requires current");
        !changes.flows.iter().any(|(flow_id, change)| match change {
            FlowChange::Add => changes.target.document().flows()[flow_id].source() == id,
            FlowChange::ReplaceQueues => {
                current.target.document().flows()[flow_id].source() == id
                    || changes.target.document().flows()[flow_id].source() == id
            }
            FlowChange::Remove => current.target.document().flows()[flow_id].source() == id,
            FlowChange::ReplaceDefinition | FlowChange::UpdateRoutes => false,
        })
    }

    /// Every completion check follows core failure and frozen old-session health.
    #[expect(
        clippy::expect_used,
        reason = "Cutover retains current until adoption or cleanup"
    )]
    async fn wait_handoff(
        &mut self,
        started: &mut StartedResourceChanges,
        healthy: &BTreeSet<PluginInstanceId>,
        retiring: &BTreeSet<PluginInstanceId>,
        resources: Option<&BTreeSet<FlowId>>,
        mut completion: impl FnMut(
            &mut ActivePipeline,
            &mut Context<'_>,
        ) -> Poll<Result<(), PipelineReconfigureError>>,
    ) -> Result<ReconfigureProgress<()>, PipelineReconfigureError> {
        loop {
            if self.shutdown_requested().is_some() {
                return Ok(ReconfigureProgress::Stopped);
            }
            let current = self.current.as_mut().expect("Cutover retains current");
            tokio::select! {
                biased;
                event = poll_fn(|context| {
                    let failed = match resources {
                        Some(flows) => matches!(current.runtime.poll_resource_retirement(flows, context), Poll::Ready(PipelineDrainObservation::WorkerExited)),
                        None => pin!(current.runtime.wait_for_worker_exit()).poll(context).is_ready(),
                    };
                    if failed { return Poll::Ready(Err(PipelineReconfigureError::InternalInvariantViolation)); }
                    match current
                        .plugins
                        .instances
                        .poll_handoff_event(context, healthy, retiring)
                    {
                        Poll::Ready(event) => Poll::Ready(event.map(Some).map_err(PipelineReconfigureError::PluginInstanceLifecycle)),
                        Poll::Pending => completion(current, context).map(|result| result.map(|()| None)),
                    }
                }) => match event? {
                    None | Some(PluginRetirementEvent::Reaped) => return Ok(ReconfigureProgress::Completed(())),
                    Some(PluginRetirementEvent::Retained(event)) => {
                        if self.handle_instance_event(event)?.is_some() {
                            return Ok(ReconfigureProgress::Stopped);
                        }
                    }
                },
                _ = wait_for_shutdown(&mut self.shutdown_receiver) => return Ok(ReconfigureProgress::Stopped),
                event = started.plugins.instances.next_event() => {
                    if handle_reconfigure_plugin_event(event.map_err(PipelineReconfigureError::PluginInstanceLifecycle)?,
                        &mut started.plugins.instances, &started.staged.changes.target,
                        started.staged.working_directory.path(), &self.control, &self.diagnostics,
                        &self.shutdown_receiver, self.environment.available_cpu_count())?.is_some() {
                        return Ok(ReconfigureProgress::Stopped);
                    }
                }
            }
        }
    }
}

fn transition_error(source: PipelineRuntimeError) -> PipelineReconfigureError {
    PipelineReconfigureError::RuntimeTransition(Box::new(source))
}

fn retirement_completion(
    observation: Poll<PipelineDrainObservation>,
) -> Poll<Result<(), PipelineReconfigureError>> {
    match observation {
        Poll::Ready(PipelineDrainObservation::Drained) => Poll::Ready(Ok(())),
        Poll::Ready(PipelineDrainObservation::WorkerExited) => {
            Poll::Ready(Err(PipelineReconfigureError::InternalInvariantViolation))
        }
        Poll::Pending => Poll::Pending,
    }
}
