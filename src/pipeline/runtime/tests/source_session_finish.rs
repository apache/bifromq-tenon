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

use std::future::Future;
use std::os::unix::fs::MetadataExt;
use std::task::{Context, Waker};

use super::*;

#[test]
fn empty_source_session_finish_returns_without_stopping_the_channel() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        "function main(event) emit() end",
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    let pipeline = runtime
        .runtime
        .as_ref()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    let flow_id = FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
    let mut finish = pipeline
        .begin_source_session_finish(&flow_id)
        .map_err(io::Error::other)?;
    runtime.executor.block_on(async {
        tokio::time::timeout(WAIT_LIMIT, finish.wait())
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)
    })?;
    source.submit(11, source_payload("new")?)?;
    assert_eq!(
        source.wait_completion()?,
        completion(11, IngressCompletionStatus::Ok)
    );
    runtime.stop()
}

#[test]
fn source_session_finish_retries_pending_and_keeps_each_channel_state() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut first = SourceEndpoint::create(directory.path(), 0)?;
    let mut second = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (second_binding, mut second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        local count = 0
        function main(event)
            count = count + 1
            if event.payload.deviceId ~= "hold" then
                builder:setLabel(event.payload.deviceId .. ":" .. count)
                emit(builder:build())
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![first.queues(), second.queues()],
        vec![vec![binding], vec![second_binding]],
    )?;
    // Both records belong to the quiesced old session before the command.
    first.submit(11, source_payload("hold")?)?;
    second.submit(22, source_payload("hold")?)?;
    let paths = [
        &first.submission_path,
        &first.completion_path,
        &second.submission_path,
        &second.completion_path,
        &sink.path,
        &second_sink.path,
    ];
    let identities = paths
        .iter()
        .map(|path| std::fs::metadata(path).map(|metadata| (metadata.dev(), metadata.ino())))
        .collect::<io::Result<Vec<_>>>()?;
    let (exited, exits) = mpsc::channel();
    let mut worker_threads = Vec::new();
    let mut spawner = |name: String, task: WorkerTask| {
        let exited = exited.clone();
        let handle = thread::Builder::new().name(name).spawn(move || {
            task.run();
            let _ = exited.send(thread::current().id());
        })?;
        worker_threads.push(handle.thread().id());
        Ok(handle)
    };
    let runtime = RunningRuntime {
        runtime: Some(
            start_runtime_with_spawner(spec, startup_control(), &mut spawner)
                .map_err(io::Error::other)?,
        ),
        executor: current_thread_executor()?,
        initial_routes: Vec::new(),
    };
    let pipeline = runtime
        .runtime
        .as_ref()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    let flow_id = FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
    let mut finish = pipeline
        .begin_source_session_finish(&flow_id)
        .map_err(io::Error::other)?;
    assert_eq!(
        wait_completion_held(&mut first)?,
        completion(11, IngressCompletionStatus::Retry)
    );
    assert_eq!(
        second.wait_completion()?,
        completion(22, IngressCompletionStatus::Retry)
    );
    wait_completion_writer_armed(&first)?;
    second.wait_submission_reader_armed()?;
    // The second reply is consumed before the first can complete. Dropping this
    // borrowed wait must preserve that partial progress in the operation.
    assert!(
        std::pin::pin!(finish.wait())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    first.completion.release(1).map_err(io::Error::other)?;
    runtime.executor.block_on(async {
        tokio::time::timeout(WAIT_LIMIT, finish.wait())
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)
    })?;
    first.submit(33, source_payload("first")?)?;
    second.submit(44, source_payload("second")?)?;
    let labels = HashSet::from([
        sink_label(&sink.wait_record()?.payload)?,
        sink_label(&second_sink.wait_record()?.payload)?,
    ]);
    assert_eq!(
        labels,
        HashSet::from([String::from("first:2"), String::from("second:2")])
    );
    sink.release(1)?;
    second_sink.release(1)?;
    assert_eq!(
        first.wait_completion()?,
        completion(33, IngressCompletionStatus::Ok)
    );
    assert_eq!(
        second.wait_completion()?,
        completion(44, IngressCompletionStatus::Ok)
    );
    let current_identities = [
        &first.submission_path,
        &first.completion_path,
        &second.submission_path,
        &second.completion_path,
        &sink.path,
        &second_sink.path,
    ]
    .iter()
    .map(|path| std::fs::metadata(path).map(|metadata| (metadata.dev(), metadata.ino())))
    .collect::<io::Result<Vec<_>>>()?;
    assert_eq!(identities, current_identities);
    assert!(
        exits.try_recv().is_err(),
        "A retained worker exited before shutdown"
    );
    runtime.stop()?;
    assert_eq!(
        exits.try_iter().collect::<HashSet<_>>(),
        worker_threads.into_iter().collect()
    );
    Ok(())
}

#[test]
fn source_session_finish_waits_for_completion_written_before_command() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        "function main(event) emit() end",
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    source.submit(11, source_payload("old")?)?;
    // Reading without release models the old SDK before its consume boundary.
    let result = wait_completion_held(&mut source)?;
    assert_eq!(result, completion(11, IngressCompletionStatus::Ok));
    let pipeline = runtime
        .runtime
        .as_ref()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    let flow_id = FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
    let mut finish = pipeline
        .begin_source_session_finish(&flow_id)
        .map_err(io::Error::other)?;
    wait_completion_writer_armed(&source)?;
    let mut context = Context::from_waker(Waker::noop());
    assert!(
        std::pin::pin!(finish.wait())
            .as_mut()
            .poll(&mut context)
            .is_pending()
    );
    source.completion.release(1).map_err(io::Error::other)?;
    runtime.executor.block_on(async {
        tokio::time::timeout(WAIT_LIMIT, finish.wait())
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)
    })?;
    source.submit(22, source_payload("new")?)?;
    assert_eq!(
        source.wait_completion()?,
        completion(22, IngressCompletionStatus::Ok)
    );
    runtime.stop()
}

#[test]
fn source_session_finish_waits_for_old_emit_then_final_completion_consumption() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"local builder = registry:getBuilder("com.example.kafka@1.0.0")
        function main(event) builder:setLabel(event.payload.deviceId); emit(builder:build()) end"#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    source.submit(11, source_payload("old")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "old");
    let pipeline = runtime
        .runtime
        .as_ref()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    let flow_id = FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
    let mut finish = pipeline
        .begin_source_session_finish(&flow_id)
        .map_err(io::Error::other)?;
    assert!(source.try_completion()?.is_none());
    assert!(
        std::pin::pin!(finish.wait())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    sink.release(1)?;
    assert_eq!(
        wait_completion_held(&mut source)?,
        completion(11, IngressCompletionStatus::Ok)
    );
    wait_completion_writer_armed(&source)?;
    assert!(
        std::pin::pin!(finish.wait())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    source.completion.release(1).map_err(io::Error::other)?;
    runtime.executor.block_on(async {
        tokio::time::timeout(WAIT_LIMIT, finish.wait())
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)
    })?;
    runtime.stop()
}

#[test]
fn source_session_finish_waits_for_inherited_completion_capacity() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let inherited = seed_full_completion_queue(&source)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        "function main(event) end",
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    // Only one current-session permit is used; Full comes from older frames.
    source.submit(11, source_payload("pending")?)?;
    let runtime = RunningRuntime::start(spec)?;
    let pipeline = runtime
        .runtime
        .as_ref()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    let flow_id = FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
    let mut finish = pipeline
        .begin_source_session_finish(&flow_id)
        .map_err(io::Error::other)?;
    wait_completion_writer_armed(&source)?;
    for expected in &inherited {
        assert_eq!(&wait_completion_held(&mut source)?, expected);
    }
    assert!(source.try_completion_without_release()?.is_none());
    assert!(
        std::pin::pin!(finish.wait())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    source
        .completion
        .release(inherited.len())
        .map_err(io::Error::other)?;
    assert_eq!(
        wait_completion_held(&mut source)?,
        completion(11, IngressCompletionStatus::Retry)
    );
    wait_completion_writer_armed(&source)?;
    assert!(
        std::pin::pin!(finish.wait())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    source.completion.release(1).map_err(io::Error::other)?;
    runtime.executor.block_on(async {
        tokio::time::timeout(WAIT_LIMIT, finish.wait())
            .await
            .map_err(io::Error::other)?
            .map_err(io::Error::other)
    })?;
    runtime.stop()
}

#[derive(Clone, Copy, Debug)]
enum FinishWaitBoundary {
    EgressRelease,
    CompletionCapacity,
    CompletionConsumption,
}

#[test]
fn stopping_source_session_finish_interrupts_each_wait_without_success() -> io::Result<()> {
    for boundary in [
        FinishWaitBoundary::EgressRelease,
        FinishWaitBoundary::CompletionCapacity,
        FinishWaitBoundary::CompletionConsumption,
    ] {
        let directory = tempfile::tempdir()?;
        let mut source = SourceEndpoint::create(directory.path(), 0)?;
        let inherited = match boundary {
            FinishWaitBoundary::CompletionCapacity => seed_full_completion_queue(&source)?,
            _ => Vec::new(),
        };
        let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
        let (binding, mut sink) =
            EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
        let script = match boundary {
            FinishWaitBoundary::EgressRelease => {
                r#"local builder = registry:getBuilder("com.example.kafka@1.0.0")
                function main(event) emit(builder:build()) end"#
            }
            FinishWaitBoundary::CompletionCapacity => "function main(event) end",
            FinishWaitBoundary::CompletionConsumption => "function main(event) emit() end",
        };
        let spec = runtime_spec(
            script,
            SourceDelivery::AtLeastOnce,
            [&sink_contract_id],
            vec![source.queues()],
            vec![vec![binding]],
        )?;
        let runtime = RunningRuntime::start(spec)?;
        source.submit(11, source_payload("old")?)?;
        match boundary {
            FinishWaitBoundary::EgressRelease => {
                sink.wait_record()?;
            }
            FinishWaitBoundary::CompletionConsumption => {
                assert_eq!(
                    wait_completion_held(&mut source)?,
                    completion(11, IngressCompletionStatus::Ok)
                );
            }
            FinishWaitBoundary::CompletionCapacity => {}
        }
        let pipeline = runtime
            .runtime
            .as_ref()
            .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
        let flow_id = FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
        let mut finish = pipeline
            .begin_source_session_finish(&flow_id)
            .map_err(io::Error::other)?;
        if !matches!(boundary, FinishWaitBoundary::EgressRelease) {
            wait_completion_writer_armed(&source)?;
        }
        assert!(
            std::pin::pin!(finish.wait())
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "{boundary:?}"
        );
        runtime.stop()?;
        assert!(
            matches!(
                current_thread_executor()?.block_on(finish.wait()),
                Err(PipelineRuntimeError::InternalEventChannelClosed)
            ),
            "{boundary:?}"
        );
        for expected in inherited {
            assert_eq!(wait_completion_held(&mut source)?, expected);
        }
        assert!(
            source.try_completion_without_release()?.is_none(),
            "Stop fabricated a result at {boundary:?}"
        );
        let mut release = [0_u8; 8];
        std::fs::File::open(&source.completion_path)?
            .read_exact_at(&mut release, TEST_RELEASE_OFFSET)?;
        assert_eq!(
            u64::from_le_bytes(release),
            0,
            "Stop fabricated consumption at {boundary:?}"
        );
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum FailureObservationOrder {
    ReplyFirst,
    WorkerFirst,
}

#[test]
fn source_session_finish_preserves_core_failure_in_both_observation_orders() -> io::Result<()> {
    for order in [
        FailureObservationOrder::ReplyFirst,
        FailureObservationOrder::WorkerFirst,
    ] {
        let directory = tempfile::tempdir()?;
        let mut blocked = SourceEndpoint::create(directory.path(), 0)?;
        let mut failing = SourceEndpoint::create(directory.path(), 1)?;
        let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
        let (binding, mut sink) =
            EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
        let (failing_binding, mut failing_sink) =
            EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
        let routes = match order {
            FailureObservationOrder::ReplyFirst => vec![vec![binding], vec![failing_binding]],
            FailureObservationOrder::WorkerFirst => vec![vec![failing_binding], vec![binding]],
        };
        let queues = match order {
            FailureObservationOrder::ReplyFirst => vec![blocked.queues(), failing.queues()],
            FailureObservationOrder::WorkerFirst => vec![failing.queues(), blocked.queues()],
        };
        let spec = runtime_spec(
            r#"local builder = registry:getBuilder("com.example.kafka@1.0.0")
            function main(event) builder:setLabel(event.payload.deviceId); emit(builder:build()) end"#,
            SourceDelivery::AtLeastOnce,
            [&sink_contract_id],
            queues,
            routes,
        )?;
        let mut runtime = RunningRuntime::start(spec)?;
        failing.submit(22, source_payload("failing")?)?;
        assert_eq!(sink_label(&failing_sink.wait_record()?.payload)?, "failing");
        blocked.submit(11, source_payload("blocked")?)?;
        assert_eq!(sink_label(&sink.wait_record()?.payload)?, "blocked");
        let pipeline = runtime
            .runtime
            .as_mut()
            .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
        let flow_id = FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
        let mut finish = pipeline
            .begin_source_session_finish(&flow_id)
            .map_err(io::Error::other)?;
        failing.corrupt_completion_release_position()?;
        // Only the failing Channel's emit can finish. The other still depends
        // on a real release that never occurs, so failure must stop that wait.
        failing_sink.release(1)?;
        runtime.executor.block_on(async {
            match order {
                FailureObservationOrder::ReplyFirst => {
                    assert!(matches!(tokio::time::timeout(WAIT_LIMIT, finish.wait()).await
                        .map_err(io::Error::other)?, Err(PipelineRuntimeError::InternalEventChannelClosed)));
                }
                FailureObservationOrder::WorkerFirst => {
                    tokio::time::timeout(WAIT_LIMIT, async {
                        tokio::select! {
                            biased;
                            result = finish.wait() => panic!("The last Channel is still blocked: {result:?}"),
                            () = pipeline.wait_for_worker_exit() => {}
                        }
                    }).await.map_err(io::Error::other)?;
                }
            }
            Ok::<_, io::Error>(())
        })?;
        let pipeline = runtime
            .runtime
            .take()
            .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
        let result = runtime.executor.block_on(pipeline.stop_and_join());
        let expected_index = match order {
            FailureObservationOrder::ReplyFirst => 1,
            FailureObservationOrder::WorkerFirst => 0,
        };
        assert!(
            matches!(result, Err(PipelineRuntimeError::FlowChannelFailed { channel_index, .. }) if channel_index == expected_index)
        );
        assert!(blocked.try_completion()?.is_none());
    }
    Ok(())
}

#[test]
fn partially_sent_source_session_finish_stops_and_retains_worker_failure() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut blocked = SourceEndpoint::create(directory.path(), 0)?;
    let mut failing = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (failing_binding, _failing_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"local builder = registry:getBuilder("com.example.kafka@1.0.0")
        function main(event)
            if event.payload.deviceId == "blocked" then emit(builder:build()) else emit() end
        end"#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![blocked.queues(), failing.queues()],
        vec![vec![binding], vec![failing_binding]],
    )?;
    let mut runtime = RunningRuntime::start(spec)?;
    blocked.submit(11, source_payload("blocked")?)?;
    sink.wait_record()?;
    let pipeline = runtime
        .runtime
        .as_mut()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    assert!(!pipeline.has_worker_exited_now());
    // A worker can exit after the caller's health check but before all commands
    // are published. Force that reachable order without racing the scheduler.
    failing.corrupt_completion_release_position()?;
    failing.submit(22, source_payload("failure")?)?;
    runtime.executor.block_on(async {
        tokio::time::timeout(WAIT_LIMIT, pipeline.wait_for_worker_exit())
            .await
            .map_err(io::Error::other)
    })?;
    let flow_id = FlowId::try_from(String::from("main")).map_err(io::Error::other)?;
    assert!(matches!(
        pipeline.begin_source_session_finish(&flow_id),
        Err(PipelineRuntimeError::FlowChannelCommandControl {
            channel_index: 1,
            source: crate::pipeline::channel::FlowChannelCommandControlError::WorkerDisconnected,
            ..
        })
    ));
    let pipeline = runtime
        .runtime
        .take()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    assert!(matches!(
        runtime.executor.block_on(pipeline.stop_and_join()),
        Err(PipelineRuntimeError::FlowChannelFailed {
            channel_index: 1,
            ..
        })
    ));
    assert!(blocked.try_completion()?.is_none());
    Ok(())
}

fn seed_full_completion_queue(source: &SourceEndpoint) -> io::Result<Vec<IngressCompletion>> {
    // The Channel owns the writer of a Completion Queue, so this fixture writes
    // through the Channel's doorbell and rings the Source loop's region, exactly
    // as the real Channel does.
    let mut writer = QueueWriter::open(
        &source.completion_path,
        Arc::clone(&source.channel_bell),
        Arc::clone(&source.source_region),
    )
    .map_err(io::Error::other)?;
    let mut inherited = Vec::new();
    loop {
        let record = completion(
            u64::MAX - inherited.len() as u64,
            IngressCompletionStatus::Ok,
        );
        match writer
            .try_write_with(
                record.encoded_len(),
                |destination| record.encode(destination).map_err(std::io::Error::other),
                || {},
            )
            .map_err(io::Error::other)?
        {
            WriteOutcome::Full => return Ok(inherited),
            WriteOutcome::Committed(_) => inherited.push(record),
        }
    }
}

fn wait_completion_held(source: &mut SourceEndpoint) -> io::Result<IngressCompletion> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        if let Some(result) = source.try_completion_without_release()? {
            return Ok(result);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other("Completion was not written"));
        }
        thread::yield_now();
    }
}

fn wait_completion_writer_armed(source: &SourceEndpoint) -> io::Result<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    while !queue_waiter_is_armed(
        &source.completion_path,
        &source.channel_region_path,
        QueueWaiter::Writer,
    )
    .map_err(io::Error::other)?
    {
        if Instant::now() >= deadline {
            return Err(io::Error::other("Completion writer did not arm its wait"));
        }
        thread::yield_now();
    }
    Ok(())
}
