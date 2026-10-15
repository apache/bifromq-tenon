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

use super::*;

const CONTRACT: &str = "com.example.kafka@1.0.0";

struct RoutingFixture {
    source: SourceQueueFixture,
    a: RunningSink,
    b: RunningSink,
    _directory: tempfile::TempDir,
}

impl RoutingFixture {
    fn new() -> io::Result<Self> {
        let source = SourceQueueFixture::new()?;
        let directory = tempfile::tempdir()?;
        let a = RunningSink::start(directory.path(), "A", Arc::clone(&source.channel_region))?;
        let b = RunningSink::start(directory.path(), "B", Arc::clone(&source.channel_region))?;
        Ok(Self {
            source,
            a,
            b,
            _directory: directory,
        })
    }

    fn routes(&self) -> io::Result<PreparedEgressRoutes> {
        let mut targets = self.a.route()?;
        targets.extend(self.b.route()?);
        Ok(HashMap::from([(sink_contract_id(CONTRACT)?, targets)]))
    }

    fn open(
        &self,
        script: &str,
        diagnostics: ChannelDiagnosticPublisher,
    ) -> io::Result<(FlowChannel, FlowChannelCommandControl)> {
        let spec = channel_spec(
            script,
            SourceDelivery::AtLeastOnce,
            [&sink_contract_id(CONTRACT)?],
        )?;
        let (channel, _, commands) = open_channel(
            self.source.queue_paths(),
            self.source.bells()?,
            Instant::now(),
            spec,
            diagnostics,
            self.routes()?,
            Arc::new(FlowChannelControl::new()),
            || false,
        )
        .map_err(channel_error)?;
        Ok((channel, commands))
    }
}

#[test]
fn source_selects_one_instance_or_broadcasts_and_waits_for_only_its_receipts() -> io::Result<()> {
    let mut fixture = RoutingFixture::new()?;
    let (mut channel, _) = fixture.open(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        function main(event)
            builder:setLabel(event.payload.deviceId)
            if event.payload.deviceId == "all" then emit(builder:build())
            else emit(builder:build(), event.payload.deviceId) end
        end
    "#,
        test_channel_publisher(0),
    )?;
    for (record_id, target) in [(1, "A"), (2, "B"), (3, "all")] {
        fixture.source.submit(record_id, source_payload(target)?)?;
        assert_eq!(
            process_one_source(&mut channel)?,
            FlowChannelStep::EventProcessed
        );
        for (name, sink) in [("A", &mut fixture.a), ("B", &mut fixture.b)] {
            let record = sink.try_read_record()?;
            if target == name || target == "all" {
                assert_eq!(
                    record.map(|r| r.payload),
                    Some(expected_sink_payload(target)?)
                );
            } else {
                assert!(record.is_none());
            }
        }
        assert!(fixture.source.try_completion()?.is_none());
        if target != "B" {
            fixture.a.release(1)?;
        }
        channel.drain_released().map_err(channel_error)?;
        if target == "all" {
            assert!(fixture.source.try_completion()?.is_none());
        }
        if target != "A" {
            fixture.b.release(1)?;
        }
        channel.drain_released().map_err(channel_error)?;
        assert_eq!(
            fixture.source.wait_completion()?,
            completion(record_id, IngressCompletionStatus::Ok)
        );
    }
    Ok(())
}

#[test]
fn invalid_target_uses_lua_diagnostics_and_reload_keeps_the_bound_targets() -> io::Result<()> {
    let mut fixture = RoutingFixture::new()?;
    let (diagnostics, mut records) = interested_channel(0);
    let (mut channel, _) = fixture.open(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        function main(event)
            emit(builder:build(), event.payload.deviceId)
        end
    "#,
        diagnostics,
    )?;
    fixture.source.submit(1, source_payload("missing")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert!(fixture.a.try_read_record()?.is_none());
    assert!(fixture.b.try_read_record()?.is_none());
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(1, IngressCompletionStatus::Error)
    );
    let error = channel_diagnostic(records.try_recv().map_err(io::Error::other)?)?;
    assert_eq!(error.kind, ChannelDiagnosticKind::Error as i32);
    assert_eq!(error.phase, "lua_main");
    assert_eq!(error.code, "process.lua_main_failed");
    assert_eq!(error.text, "emit target is not a Sink in this Flow");
    fixture.source.submit(2, source_payload("A")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert!(fixture.a.try_read_record()?.is_some());
    assert!(fixture.b.try_read_record()?.is_none());
    fixture.a.release(1)?;
    channel.drain_released().map_err(channel_error)?;
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(2, IngressCompletionStatus::Ok)
    );
    assert!(records.try_recv().is_err());
    Ok(())
}

#[test]
fn caught_target_error_does_not_publish_a_main_failure() -> io::Result<()> {
    let mut fixture = RoutingFixture::new()?;
    let (diagnostics, mut records) = interested_channel(0);
    let (mut channel, _) = fixture.open(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        function main(event)
            assert(not pcall(emit, builder:build(), "missing"))
            emit()
        end
    "#,
        diagnostics,
    )?;
    fixture.source.submit(1, source_payload("input")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(1, IngressCompletionStatus::Ok)
    );
    assert!(fixture.a.try_read_record()?.is_none());
    assert!(fixture.b.try_read_record()?.is_none());
    assert!(records.try_recv().is_err());
    Ok(())
}

#[test]
fn target_outputs_keep_first_boundary_ownership_and_completion_order() -> io::Result<()> {
    let mut fixture = RoutingFixture::new()?;
    let (mut channel, _) = fixture.open(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        function main(event)
            emit(builder:build(), event.payload.deviceId)
            if event.payload.deviceId == "A" then emit(builder:build(), "B") end
        end
    "#,
        test_channel_publisher(0),
    )?;
    fixture.source.submit(1, source_payload("B")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    fixture.source.submit(2, source_payload("A")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert!(fixture.a.try_read_record()?.is_some());
    assert!(fixture.b.try_read_record()?.is_some());
    assert!(fixture.b.try_read_record()?.is_some());
    fixture.a.release(1)?;
    channel.drain_released().map_err(channel_error)?;
    assert!(fixture.source.try_completion()?.is_none());
    fixture.b.release(1)?;
    channel.drain_released().map_err(channel_error)?;
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(1, IngressCompletionStatus::Ok)
    );
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(2, IngressCompletionStatus::Ok)
    );
    fixture.b.release(1)?;
    channel.drain_released().map_err(channel_error)?;
    Ok(())
}

#[test]
fn route_changes_keep_lua_state_and_timer_but_change_allowed_instances() -> io::Result<()> {
    let mut fixture = RoutingFixture::new()?;
    let (diagnostics, mut records) = interested_channel(0);
    let (mut channel, commands) = fixture.open(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        local count = 0
        function main(event)
            count = count + 1
            if event.type == "source" then
                setTimeout(60000)
                emit()
            else
                assert(count == 2)
                assert(not pcall(emit, builder:build(), "A"))
                emit(builder:build(), "B")
            end
        end
    "#,
        diagnostics,
    )?;
    fixture.source.submit(1, source_payload("input")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(1, IngressCompletionStatus::Ok)
    );
    let timer = channel
        .lua_vm()
        .map_err(channel_error)?
        .next_timer_event()
        .map(|t| t.schedule);
    assert!(timer.is_some());
    replace_routes(
        &mut channel,
        &commands,
        HashMap::from([(sink_contract_id(CONTRACT)?, fixture.b.route()?)]),
    )?;
    assert_eq!(
        channel
            .lua_vm()
            .map_err(channel_error)?
            .next_timer_event()
            .map(|t| t.schedule),
        timer
    );
    assert_eq!(
        channel.process_timer().map_err(channel_error)?,
        FlowChannelStep::EventProcessed
    );
    assert!(fixture.a.try_read_record()?.is_none());
    assert!(fixture.b.try_read_record()?.is_some());
    fixture.b.release(1)?;
    channel.drain_released().map_err(channel_error)?;
    assert!(records.try_recv().is_err());
    Ok(())
}

fn replace_routes(
    channel: &mut FlowChannel,
    commands: &FlowChannelCommandControl,
    routes: PreparedEgressRoutes,
) -> io::Result<()> {
    let (events, observations) = mpsc::sync_channel(8);
    let ticket = commands
        .begin_replacement(0, ChannelDefinitionChange::KeepLua, routes, events)
        .map_err(io::Error::other)?;
    let Some(super::super::ChannelCommand::Replace(request)) = channel.commands.try_take() else {
        return Err(io::Error::other("replacement request missing"));
    };
    channel
        .prepare_replacement(request)
        .map_err(channel_error)?;
    assert!(matches!(
        observations
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?,
        FlowChannelReplacementEvent::Prepared { .. }
    ));
    ticket.cutover().map_err(io::Error::other)?;
    thread::scope(|scope| -> io::Result<()> {
        let activation = scope.spawn(move || -> io::Result<()> {
            assert!(matches!(
                observations
                    .recv_timeout(WAIT_LIMIT)
                    .map_err(io::Error::other)?,
                FlowChannelReplacementEvent::CutoverComplete { .. }
            ));
            ticket.activate().map_err(io::Error::other)
        });
        channel.advance_replacement().map_err(channel_error)?;
        activation
            .join()
            .map_err(|_| io::Error::other("activation thread panicked"))??;
        Ok(())
    })
}

#[test]
fn contract_swap_uses_the_new_instance_binding_without_reloading_lua() -> io::Result<()> {
    let mut fixture = RoutingFixture::new()?;
    let first = sink_contract_id(CONTRACT)?;
    let second = sink_contract_id("com.example.kafka@2.0.0")?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        local count = 0
        function main(event)
            count = count + 1
            builder:setLabel(tostring(count))
            local other = event.payload.deviceId == "A" and "B" or "A"
            assert(not pcall(emit, builder:build(), other))
            emit(builder:build(), event.payload.deviceId)
        end
    "#,
        SourceDelivery::AtLeastOnce,
        [&first, &second],
    )?;
    let (diagnostics, mut records) = interested_channel(0);
    let (mut channel, _, commands) = open_channel(
        fixture.source.queue_paths(),
        fixture.source.bells()?,
        Instant::now(),
        spec,
        diagnostics,
        HashMap::from([
            (first.clone(), fixture.a.route()?),
            (second.clone(), fixture.b.route()?),
        ]),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;
    fixture.source.submit(1, source_payload("A")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        fixture.a.read_record()?.payload,
        expected_sink_payload("1")?
    );
    fixture.a.release(1)?;
    channel.drain_released().map_err(channel_error)?;
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(1, IngressCompletionStatus::Ok)
    );
    replace_routes(
        &mut channel,
        &commands,
        HashMap::from([(first, fixture.b.route()?), (second, fixture.a.route()?)]),
    )?;
    fixture.source.submit(2, source_payload("B")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        fixture.b.read_record()?.payload,
        expected_sink_payload("2")?
    );
    assert!(fixture.a.try_read_record()?.is_none());
    fixture.b.release(1)?;
    channel.drain_released().map_err(channel_error)?;
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(2, IngressCompletionStatus::Ok)
    );
    assert!(records.try_recv().is_err());
    Ok(())
}

#[test]
fn added_target_is_visible_only_after_cutover() -> io::Result<()> {
    let mut fixture = RoutingFixture::new()?;
    let contract = sink_contract_id(CONTRACT)?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        local count = 0
        function main(event)
            count = count + 1
            local ok = pcall(emit, builder:build(), "B")
            assert(ok == (count == 3))
            if not ok then emit() end
        end
    "#,
        SourceDelivery::AtLeastOnce,
        [&contract],
    )?;
    let (diagnostics, mut records) = interested_channel(0);
    let (mut channel, _, commands) = open_channel(
        fixture.source.queue_paths(),
        fixture.source.bells()?,
        Instant::now(),
        spec,
        diagnostics,
        HashMap::from([(contract.clone(), fixture.a.route()?)]),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;
    let mut routes = fixture.a.route()?;
    routes.insert(
        PluginInstanceId::try_from("B").map_err(io::Error::other)?,
        PreparedEgressQueue::New(fixture.b.writer()?),
    );
    let (events, observations) = mpsc::sync_channel(8);
    let ticket = commands
        .begin_replacement(
            0,
            ChannelDefinitionChange::KeepLua,
            HashMap::from([(contract.clone(), routes)]),
            events,
        )
        .map_err(io::Error::other)?;
    let Some(super::super::ChannelCommand::Replace(request)) = channel.commands.try_take() else {
        return Err(io::Error::other("replacement request missing"));
    };
    channel
        .prepare_replacement(request)
        .map_err(channel_error)?;
    assert!(matches!(
        observations
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?,
        FlowChannelReplacementEvent::Prepared { .. }
    ));
    fixture.source.submit(1, source_payload("before")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(1, IngressCompletionStatus::Ok)
    );
    drop(ticket);
    channel.advance_replacement().map_err(channel_error)?;
    fixture.source.submit(2, source_payload("aborted")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(2, IngressCompletionStatus::Ok)
    );
    let mut routes = fixture.a.route()?;
    routes.insert(
        PluginInstanceId::try_from("B").map_err(io::Error::other)?,
        PreparedEgressQueue::New(fixture.b.writer()?),
    );
    replace_routes(&mut channel, &commands, HashMap::from([(contract, routes)]))?;
    fixture.source.submit(3, source_payload("after")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert!(fixture.b.try_read_record()?.is_some());
    assert!(fixture.a.try_read_record()?.is_none());
    fixture.b.release(1)?;
    channel.drain_released().map_err(channel_error)?;
    assert_eq!(
        fixture.source.wait_completion()?,
        completion(3, IngressCompletionStatus::Ok)
    );
    assert!(records.try_recv().is_err());
    Ok(())
}
