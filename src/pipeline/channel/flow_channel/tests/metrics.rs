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
use crate::metrics::capture_test_support::Capture;
use crate::pipeline::channel::flow_channel_command::{ChannelCommand, ChannelDefinitionChange};
use crate::pipeline::channel::metrics::FlowMetrics;

#[test]
fn input_decode_main_and_completion_metrics_follow_actual_work() -> io::Result<()> {
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let mut source = SourceQueueFixture::new()?;
    let spec = channel_spec(
        "function main(event) emit(); error('script failed') end",
        SourceDelivery::AtMostOnce,
        [],
    )?;
    let (prepared, _wake, _commands) = FlowChannel::prepare(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
        flow.channel(0),
    )
    .map_err(channel_error)?;
    let mut channel = prepared.bind().map_err(channel_error)?;
    let initial = captured.collect()?;
    assert!(
        initial
            .number("tenon.flow.lua.memory", &[])
            .is_some_and(|bytes| bytes > 0.0)
    );
    assert_eq!(initial.number("tenon.flow.waiting", &[]), Some(0.0));
    assert_eq!(
        initial.number("tenon.queue.usage", &[("tenon.queue.kind", "submission")]),
        Some(0.0)
    );
    let payload = source_payload("device")?;
    let payload_bytes = payload.len();
    source.submit(1, payload)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    source.submit(2, vec![0xff])?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    let observed = captured.collect()?;
    assert_eq!(observed.number("tenon.flow.input.records", &[]), Some(2.0));
    assert_eq!(
        observed.number("tenon.flow.input.bytes", &[]),
        Some((payload_bytes + 1) as f64)
    );
    assert_eq!(
        observed.number("tenon.flow.completion.records", &[("result", "ok")]),
        Some(2.0)
    );
    assert_eq!(
        observed.number("tenon.flow.errors", &[("phase", "decode")]),
        Some(1.0)
    );
    assert_eq!(
        observed.number("tenon.flow.errors", &[("phase", "lua_main")]),
        Some(1.0)
    );
    assert_eq!(
        observed.number("tenon.flow.emits", &[("kind", "boundary")]),
        Some(1.0)
    );
    assert_eq!(
        observed
            .histogram("tenon.flow.lua.duration", &[])
            .map(|point| point.count),
        Some(1)
    );
    assert!(
        observed
            .number("tenon.queue.usage", &[("tenon.queue.kind", "completion")])
            .is_some_and(|bytes| bytes > 0.0)
    );
    drop(channel);
    let retired = captured.collect()?;
    assert_eq!(retired.number("tenon.flow.waiting", &[]), None);
    assert_eq!(retired.number("tenon.queue.capacity", &[]), None);
    assert_eq!(retired.number("tenon.flow.lua.memory", &[]), Some(0.0));
    drop(flow);
    let deleted = captured.collect()?;
    assert_eq!(deleted.number("tenon.flow.lua.memory", &[]), None);
    assert_eq!(deleted.number("tenon.flow.input.records", &[]), Some(2.0));
    Ok(())
}

#[test]
fn accepted_emits_and_script_error_are_visible_before_the_sink_releases() -> io::Result<()> {
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let mut source = SourceQueueFixture::new()?;
    let directory = tempfile::tempdir()?;
    let mut first = RunningSink::start(
        directory.path(),
        "first",
        Arc::clone(&source.channel_region),
    )?;
    let mut second = RunningSink::start(
        directory.path(),
        "second",
        Arc::clone(&source.channel_region),
    )?;
    let contract = sink_contract_id("com.example.kafka@1.0.0")?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        function main(event)
            builder:setLabel("accepted")
            emit(builder:build())
            emit()
            error("script failed after accepted output")
        end
    "#,
        SourceDelivery::AtLeastOnce,
        [&contract],
    )?;
    let mut targets = first.route()?;
    targets.extend(second.route()?);
    let channel = RunningChannel::start_observed(
        &source,
        spec,
        HashMap::from([(contract, targets)]),
        flow.channel(0),
    )?;
    source.submit(1, source_payload("device")?)?;
    let output = first.read_record()?;
    assert_eq!(second.read_record()?, output);
    let waiting = wait_number(&captured, "tenon.flow.emits", &[("kind", "payload")], 1.0)?;
    assert_eq!(
        waiting.number("tenon.flow.emits", &[("kind", "payload")]),
        Some(1.0)
    );
    assert_eq!(
        waiting.number("tenon.flow.emits", &[("kind", "boundary")]),
        Some(1.0)
    );
    assert_eq!(
        waiting.number("tenon.flow.errors", &[("phase", "lua_main")]),
        Some(1.0)
    );
    assert_eq!(
        waiting
            .histogram("tenon.flow.lua.duration", &[])
            .map(|point| point.count),
        Some(1)
    );
    for target in ["first", "second"] {
        assert_eq!(
            waiting.number(
                "tenon.flow.egress.records",
                &[("tenon.plugin.instance.id", target)]
            ),
            Some(1.0)
        );
        assert_eq!(
            waiting.number(
                "tenon.flow.egress.bytes",
                &[("tenon.plugin.instance.id", target)]
            ),
            Some(output.payload.len() as f64)
        );
    }
    assert_eq!(source.try_completion()?, None);
    first.release(1)?;
    second.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(1, IngressCompletionStatus::Ok)
    );
    // The Channel settles accepted output without ever blocking on the Sink, so
    // an unreleased record never becomes a modelled wait. The only wait a
    // settled Channel enters is its own doorbell park, which reports `idle`.
    let resting = wait_number(&captured, "tenon.flow.waiting", &[], 5.0)?;
    assert_eq!(
        resting
            .histogram("tenon.flow.wait.duration", &[("wait.kind", "sink_release")])
            .map(|point| point.count),
        None
    );
    channel.stop()?;
    assert_eq!(
        captured.collect()?.number("tenon.queue.capacity", &[]),
        None
    );
    Ok(())
}

#[test]
fn completion_drain_interruption_keeps_one_span_until_real_release() -> io::Result<()> {
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let mut source = SourceQueueFixture::new()?;
    let spec = channel_spec("function main(event) end", SourceDelivery::AtMostOnce, [])?;
    let channel = RunningChannel::start_observed(&source, spec, HashMap::new(), flow.channel(0))?;
    source.submit(1, source_payload("device")?)?;
    wait_number(
        &captured,
        "tenon.flow.completion.records",
        &[("result", "ok")],
        1.0,
    )?;
    let mut finish_received = channel
        .commands
        .finish_source_session()
        .map_err(io::Error::other)?;
    wait_number(&captured, "tenon.flow.waiting", &[], 4.0)?;
    channel
        .wake
        .as_ref()
        .ok_or_else(|| io::Error::other("running Channel lost wake"))?
        .wake()
        .map_err(io::Error::other)?;
    assert_eq!(
        // An interruption keeps the drain span open: no `completion_drain`
        // sample exists until the real release ends it below.
        captured
            .collect()?
            .histogram(
                "tenon.flow.wait.duration",
                &[("wait.kind", "completion_drain")]
            )
            .map(|point| point.count),
        None
    );
    assert_eq!(
        source.wait_completion()?,
        completion(1, IngressCompletionStatus::Ok)
    );
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        match finish_received.try_recv() {
            Ok(()) => break,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) if Instant::now() < deadline => {
                thread::yield_now()
            }
            Err(error) => return Err(io::Error::other(error)),
        }
    }
    // The finished drain hands the Channel back to its own doorbell park.
    let resumed = wait_number(&captured, "tenon.flow.waiting", &[], 5.0)?;
    assert_eq!(
        resumed
            .histogram(
                "tenon.flow.wait.duration",
                &[("wait.kind", "completion_drain"), ("result", "ready")]
            )
            .map(|point| point.count),
        Some(1)
    );
    channel.stop()?;
    Ok(())
}

#[test]
fn stopped_completion_capacity_wait_retires_gauges_without_a_false_commit() -> io::Result<()> {
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let mut source = SourceQueueFixture::new()?;
    // A previous Source session left results behind, so this fresh session's
    // admission permits do not imply free Completion capacity.
    let mut previous = source.completion_filler()?;
    let encoded = completion(u64::MAX, IngressCompletionStatus::Error).encode_to_vec();
    while matches!(
        previous
            .try_write_with(
                encoded.len(),
                |destination| std::io::Write::write_all(destination, &encoded),
                || {}
            )
            .map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ) {}
    drop(previous);
    let spec = channel_spec(
        "function main(event) emit() end",
        SourceDelivery::AtMostOnce,
        [],
    )?;
    let channel = RunningChannel::start_observed(&source, spec, HashMap::new(), flow.channel(0))?;
    source.submit(u64::MAX, source_payload("device")?)?;
    let waiting = wait_number(&captured, "tenon.flow.waiting", &[], 3.0)?;
    assert_eq!(waiting.number("tenon.flow.completion.records", &[]), None);
    assert_eq!(
        waiting
            .histogram("tenon.flow.lua.duration", &[])
            .map(|point| point.count),
        None
    );
    channel.stop()?;
    let stopped = captured.collect()?;
    assert_eq!(
        stopped
            .histogram(
                "tenon.flow.wait.duration",
                &[
                    ("wait.kind", "completion_capacity"),
                    ("result", "cancelled")
                ]
            )
            .map(|point| point.count),
        Some(1)
    );
    assert_eq!(stopped.number("tenon.flow.waiting", &[]), None);
    assert_eq!(stopped.number("tenon.flow.completion.records", &[]), None);
    Ok(())
}

fn wait_number(
    captured: &Capture,
    name: &str,
    labels: &[(&str, &str)],
    expected: f64,
) -> io::Result<crate::metrics::capture_test_support::Snapshot> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let snapshot = captured.collect()?;
        if snapshot.number(name, labels) == Some(expected) {
            return Ok(snapshot);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "metric {name} did not reach {expected}"
            )));
        }
        thread::yield_now();
    }
}

#[test]
fn candidate_memory_is_counted_once_and_retires_before_abort_or_cutover_finishes() -> io::Result<()>
{
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let source = SourceQueueFixture::new()?;
    let spec = channel_spec("function main(event) end", SourceDelivery::AtMostOnce, [])?;
    let (prepared, _wake, commands) = FlowChannel::prepare(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
        flow.channel(0),
    )
    .map_err(channel_error)?;
    let mut channel = prepared.bind().map_err(channel_error)?;
    let baseline = captured
        .collect()?
        .number("tenon.flow.lua.memory", &[])
        .ok_or_else(|| io::Error::other("initial Lua memory missing"))?;
    enum Decision {
        Abort,
        Cutover,
    }
    for decision in [Decision::Abort, Decision::Cutover] {
        let spec = channel_spec(
            "local retained = string.rep('candidate', 4096); function main(event) assert(#retained > 0) end",
            SourceDelivery::AtMostOnce,
            [],
        )?;
        let (events, received) = mpsc::sync_channel(4);
        let ticket = commands
            .begin_replacement(
                0,
                ChannelDefinitionChange::Replace(spec),
                HashMap::new(),
                events,
            )
            .map_err(io::Error::other)?;
        let Some(ChannelCommand::Replace(request)) = channel.commands.try_take() else {
            return Err(io::Error::other("replacement command missing"));
        };
        channel
            .prepare_replacement(request)
            .map_err(channel_error)?;
        received
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?;
        let combined = captured
            .collect()?
            .number("tenon.flow.lua.memory", &[])
            .ok_or_else(|| io::Error::other("candidate memory missing"))?;
        assert!(combined > baseline + 32_000.0);
        let expected = match decision {
            Decision::Abort => {
                drop(ticket);
                baseline
            }
            Decision::Cutover => {
                ticket.cutover().map_err(io::Error::other)?;
                ticket.activate().map_err(io::Error::other)?;
                combined - baseline
            }
        };
        channel.advance_replacement().map_err(channel_error)?;
        received
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?;
        let final_memory = captured.collect()?.number("tenon.flow.lua.memory", &[]);
        assert_eq!(final_memory, Some(expected));
    }
    drop(channel);
    assert_eq!(
        captured.collect()?.number("tenon.flow.lua.memory", &[]),
        Some(0.0)
    );
    Ok(())
}

#[test]
fn initialization_failure_counts_once_and_cancelled_initialization_is_not_an_error()
-> io::Result<()> {
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let source = SourceQueueFixture::new()?;
    let spec = channel_spec("error('top level failed')", SourceDelivery::AtMostOnce, [])?;
    assert!(
        FlowChannel::prepare(
            source.queue_paths(),
            source.bells()?,
            Instant::now(),
            spec,
            test_channel_publisher(0),
            HashMap::new(),
            Arc::new(FlowChannelControl::new()),
            || false,
            flow.channel(0)
        )
        .is_err()
    );
    let failed = captured.collect()?;
    assert_eq!(
        failed.number(
            "tenon.flow.errors",
            &[
                ("phase", "lua_initialize"),
                ("error.type", "process.lua_top_level_failed")
            ]
        ),
        Some(1.0)
    );
    assert_eq!(failed.number("tenon.flow.lua.memory", &[]), Some(0.0));
    let spec = channel_spec(
        "while true do end; function main(event) end",
        SourceDelivery::AtMostOnce,
        [],
    )?;
    assert!(
        FlowChannel::prepare(
            source.queue_paths(),
            source.bells()?,
            Instant::now(),
            spec,
            test_channel_publisher(0),
            HashMap::new(),
            Arc::new(FlowChannelControl::new()),
            || true,
            flow.channel(0)
        )
        .is_err()
    );
    let cancelled = captured.collect()?;
    assert_eq!(
        cancelled.number("tenon.flow.errors", &[("phase", "lua_initialize")]),
        Some(1.0)
    );
    assert_eq!(
        cancelled
            .histogram("tenon.flow.lua.duration", &[])
            .map(|point| point.count),
        None
    );
    Ok(())
}

#[test]
fn target_capacity_wait_has_no_partial_fanout_and_does_not_block_another_channel() -> io::Result<()>
{
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let contract = sink_contract_id("com.example.kafka@1.0.0")?;
    enum Resolution {
        Release,
        Stop,
    }
    for resolution in [Resolution::Release, Resolution::Stop] {
        let mut source = SourceQueueFixture::new()?;
        let directory = tempfile::tempdir()?;
        let mut first = RunningSink::start(
            directory.path(),
            "first",
            Arc::clone(&source.channel_region),
        )?;
        let mut second = RunningSink::start(
            directory.path(),
            "second",
            Arc::clone(&source.channel_region),
        )?;
        let mut previous = second.writer()?;
        let inherited = EgressRecord {
            payload: vec![1; 1000],
        }
        .encode_to_vec();
        assert!(matches!(
            previous.try_write_with(
                inherited.len(),
                |destination| std::io::Write::write_all(destination, &inherited),
                || {}
            ),
            Ok(WriteOutcome::Committed(_))
        ));
        drop(previous);
        let spec = channel_spec(
            r#"local builder = registry:getBuilder("com.example.kafka@1.0.0"); function main(event) builder:setLabel("output"); emit(builder:build()) end"#,
            SourceDelivery::AtLeastOnce,
            [&contract],
        )?;
        let mut targets = first.route()?;
        targets.extend(second.route()?);
        let channel = RunningChannel::start_observed(
            &source,
            spec,
            HashMap::from([(contract.clone(), targets)]),
            flow.channel(0),
        )?;
        source.submit(1, source_payload("device")?)?;
        wait_number(
            &captured,
            "tenon.flow.waiting",
            &[("tenon.channel.index", "0")],
            1.0,
        )?;
        assert_eq!(
            first.try_read_record()?,
            None,
            "every target must be writable before fanout starts"
        );
        assert_eq!(source.try_completion()?, None);
        let mut parallel_source = SourceQueueFixture::new()?;
        let parallel = RunningChannel::start_observed(
            &parallel_source,
            channel_spec(
                "function main(event) emit() end",
                SourceDelivery::AtMostOnce,
                [],
            )?,
            HashMap::new(),
            flow.channel(1),
        )?;
        parallel_source.submit(1, source_payload("parallel")?)?;
        assert_eq!(
            parallel_source.wait_completion()?,
            completion(1, IngressCompletionStatus::Ok)
        );
        // Channel 1 finished its record and parks on its own doorbell; it never
        // enters a capacity wait, while Channel 0 stays blocked on Egress.
        let parallel_snapshot = wait_number(
            &captured,
            "tenon.flow.waiting",
            &[("tenon.channel.index", "1")],
            5.0,
        )?;
        assert_eq!(
            parallel_snapshot.number("tenon.flow.waiting", &[("tenon.channel.index", "0")]),
            Some(1.0)
        );
        match resolution {
            Resolution::Release => {
                assert_eq!(second.read_record()?.payload.len(), 1000);
                second.release(1)?;
                assert_eq!(first.read_record()?, second.read_record()?);
                first.release(1)?;
                second.release(1)?;
                assert_eq!(
                    source.wait_completion()?,
                    completion(1, IngressCompletionStatus::Ok)
                );
                channel.stop()?;
                assert_eq!(
                    captured
                        .collect()?
                        .histogram(
                            "tenon.flow.wait.duration",
                            &[("wait.kind", "egress_capacity"), ("result", "ready")]
                        )
                        .map(|point| point.count),
                    Some(1)
                );
            }
            Resolution::Stop => {
                channel.stop()?;
                assert_eq!(first.try_read_record()?, None);
                assert_eq!(
                    captured
                        .collect()?
                        .histogram(
                            "tenon.flow.wait.duration",
                            &[("wait.kind", "egress_capacity"), ("result", "cancelled")]
                        )
                        .map(|point| point.count),
                    Some(1)
                );
            }
        }
        parallel.stop()?;
    }
    Ok(())
}

/// An unconditional recovery wake with no work behind it is the doorbell's cost.
///
/// One doorbell carries no cause, so a peer that wakes the loop without
/// publishing any subscribed fact forces one whole recheck. That wake must be
/// counted rather than silently absorbed.
#[test]
fn an_unconditional_wake_with_no_work_is_counted_as_spurious() -> io::Result<()> {
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let source = SourceQueueFixture::new()?;
    let spec = channel_spec("function main(event) end", SourceDelivery::AtLeastOnce, [])?;
    let bell = loop_bell(&source.channel_region, 0)?;
    let control = Arc::new(FlowChannelControl::new());
    let worker_control = Arc::clone(&control);
    let queues = source.queue_paths();
    let bells = FlowChannelBells::new(Arc::clone(&bell), Arc::clone(&source.source_region));
    let channel_metrics = flow.channel(0);
    let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
    let (outcome_sender, outcome) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let opened = FlowChannel::prepare(
            queues,
            bells,
            Instant::now(),
            spec,
            test_channel_publisher(0),
            HashMap::new(),
            worker_control,
            || false,
            channel_metrics,
        )
        .and_then(|(prepared, wake, _commands)| Ok((prepared.bind()?, wake)));
        let (channel, wake) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                let _ = startup_sender.send(Err(error.to_string()));
                return;
            }
        };
        if startup_sender.send(Ok(wake)).is_err() {
            return;
        }
        let _ = outcome_sender.send(channel.run().map_err(|error| error.to_string()));
    });
    let wake = match startup_receiver.recv_timeout(WAIT_LIMIT) {
        Ok(Ok(wake)) => wake,
        Ok(Err(error)) => {
            worker
                .join()
                .map_err(|_| io::Error::other("Flow Channel thread panicked"))?;
            return Err(io::Error::other(error));
        }
        Err(_) => return Err(io::Error::other("Flow Channel did not finish startup")),
    };
    wait_until_platform_wait(&bell)?;
    wake.force_wake().map_err(io::Error::other)?;
    wait_until_spurious_wakes(&bell, 1)?;
    control.request_stop();
    wake.wake().map_err(io::Error::other)?;
    let outcome = outcome
        .recv_timeout(WAIT_LIMIT)
        .map_err(|_| io::Error::other("Flow Channel did not stop"))?;
    worker
        .join()
        .map_err(|_| io::Error::other("Flow Channel thread panicked"))?;
    outcome.map_err(io::Error::other)?;
    // The park reports the wakes it absorbed when the park ends.
    let observed = captured.collect()?;
    assert_eq!(
        observed.number("tenon.flow.wake.spurious", &[("tenon.channel.index", "0")]),
        Some(1.0)
    );
    Ok(())
}
