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
use crate::contracts::sink::EgressRecord;
use crate::pipeline::channel::FlowChannelControl;
use crate::pipeline::channel::metrics::ChannelMetrics;
use crate::pipeline::queue_test_support::wait_for_armed_loop;
use prost::Message as _;
use std::io;
use std::num::NonZeroU32;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use tenon_ipc::bell::create_bell_region;
use tenon_ipc::queue::contract_test_support::open_reader;
use tenon_ipc::queue::{DataCapacity, QueueReader, QueueWaiter, ReadOutcome, create_queue_file};

#[test]
fn selected_target_does_not_wait_for_a_full_unselected_queue() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let bell = writer_bell(directory.path())?;
    let (first, mut first_reader) = queue(&directory.path().join("first"), 16, &bell)?;
    let (mut second, mut second_reader) = queue(&directory.path().join("second"), 16, &bell)?;
    assert!(matches!(
        second
            .try_write_with(
                8,
                |destination| Write::write_all(destination, &[9; 8]),
                || {}
            )
            .map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    let wake = second.wait_interrupter();
    let control = Arc::new(FlowChannelControl::new());
    let worker_control = Arc::clone(&control);
    let mut routes = routes(first, second)?;
    let id = PluginInstanceId::try_from("first").map_err(io::Error::other)?;
    let selected = id.clone();
    let outputs = [output(encoded_record(&[1])?, Some(selected))?];
    let (done, result) = std::sync::mpsc::sync_channel(1);
    let writer = thread::spawn(move || {
        let outcome = send_batch(
            &mut routes,
            &outputs,
            &bell,
            &ChannelMetrics::default(),
            || worker_control.is_stopping(),
        );
        let _ = done.send(outcome);
    });
    let outcome = result.recv_timeout(std::time::Duration::from_secs(5));
    control.request_stop();
    wake.interrupt().map_err(io::Error::other)?;
    writer
        .join()
        .map_err(|_| io::Error::other("Selected writer panicked"))?;
    let outcome = outcome
        .map_err(io::Error::other)?
        .map_err(io::Error::other)?;
    let BatchSendOutcome::Committed(pending) = outcome else {
        return Err(io::Error::other(
            "Selected target waited for the full queue",
        ));
    };
    assert_eq!(pending.targets.len(), 1);
    assert_eq!(pending.targets[0].1, id);
    assert!(matches!(
        first_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    ));
    first_reader.release(1).map_err(io::Error::other)?;
    second_reader.release(1).map_err(io::Error::other)?;
    Ok(())
}

#[test]
fn a_full_last_target_prevents_any_partial_fanout_until_real_release() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let bell = writer_bell(directory.path())?;
    let (first, mut first_reader) = queue(&directory.path().join("first"), 16, &bell)?;
    let second_path = directory.path().join("second");
    let (mut second, mut second_reader) = queue(&second_path, 16, &bell)?;
    assert!(matches!(
        second
            .try_write_with(
                [9; 8].len(),
                |destination| std::io::Write::write_all(destination, &[9; 8]),
                || {}
            )
            .map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    let mut routes = routes(first, second)?;
    let outputs = [output(encoded_record(&[1])?, None)?];
    let writer = thread::spawn(move || {
        send_batch(
            &mut routes,
            &outputs,
            &bell,
            &ChannelMetrics::default(),
            || false,
        )
    });

    let armed = wait_for_armed_loop(
        &second_path,
        &writer_bell_path(&second_path),
        QueueWaiter::Writer,
    );
    let first_empty = matches!(
        first_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    second_reader.release(1).map_err(io::Error::other)?;
    assert!(matches!(
        writer
            .join()
            .map_err(|_| io::Error::other("Fanout writer panicked"))?
            .map_err(io::Error::other)?,
        BatchSendOutcome::Committed(_)
    ));
    armed?;
    assert!(first_empty);
    for reader in [&mut first_reader, &mut second_reader] {
        let ReadOutcome::Record(record) = reader.try_read().map_err(io::Error::other)? else {
            return Err(io::Error::other("Fanout target did not receive its record"));
        };
        assert_eq!(
            EgressRecord::decode(record.payload())
                .map_err(io::Error::other)?
                .payload,
            [1]
        );
        reader.release(1).map_err(io::Error::other)?;
    }
    Ok(())
}

#[test]
fn stop_interrupts_a_full_target_without_committing_to_any_target() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let bell = writer_bell(directory.path())?;
    let (first, mut first_reader) = queue(&directory.path().join("first"), 16, &bell)?;
    let second_path = directory.path().join("second");
    let (mut second, mut second_reader) = queue(&second_path, 16, &bell)?;
    assert!(matches!(
        second
            .try_write_with(
                [9; 8].len(),
                |destination| std::io::Write::write_all(destination, &[9; 8]),
                || {}
            )
            .map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    let wake = second.wait_interrupter();
    let control = Arc::new(FlowChannelControl::new());
    let worker_control = Arc::clone(&control);
    let mut routes = routes(first, second)?;
    let outputs = [output(encoded_record(&[1])?, None)?];
    let writer = thread::spawn(move || {
        send_batch(
            &mut routes,
            &outputs,
            &bell,
            &ChannelMetrics::default(),
            || worker_control.is_stopping(),
        )
    });

    let armed = wait_for_armed_loop(
        &second_path,
        &writer_bell_path(&second_path),
        QueueWaiter::Writer,
    );
    control.request_stop();
    wake.interrupt().map_err(io::Error::other)?;
    assert!(matches!(
        writer
            .join()
            .map_err(|_| io::Error::other("Fanout writer panicked"))?
            .map_err(io::Error::other)?,
        BatchSendOutcome::Interrupted
    ));
    armed?;
    assert!(matches!(
        first_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    ));
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    ));
    Ok(())
}

fn encoded_record(payload: &[u8]) -> io::Result<EncodedEgressRecord> {
    EncodedEgressRecord::try_encode::<std::collections::TryReserveError>(payload.len(), |bytes| {
        bytes.extend_from_slice(payload);
        Ok(())
    })
    .map_err(io::Error::other)
}

fn writer_bell(directory: &Path) -> io::Result<Arc<LoopBell>> {
    let path = directory.join("writer.bells");
    create_bell_region(&path, NonZeroU32::MIN, 0).map_err(io::Error::other)?;
    BellRegion::open(&path)
        .and_then(|region| region.loop_bell(0))
        .map_err(io::Error::other)
}

fn queue(path: &Path, bytes: u64, bell: &Arc<LoopBell>) -> io::Result<(QueueWriter, QueueReader)> {
    let capacity = DataCapacity::try_from(bytes).map_err(io::Error::other)?;
    let maximum = NonZeroU64::new(capacity.get() - tenon_ipc::queue::FRAME_HEADER_LEN as u64)
        .ok_or_else(|| io::Error::other("Fixture capacity must hold a payload"))?;
    create_queue_file(path, capacity, maximum).map_err(io::Error::other)?;
    let writer_region = writer_bell_path(path);
    let reader_region = reader_bell_path(path);
    create_bell_region(&reader_region, NonZeroU32::MIN, 0).map_err(io::Error::other)?;
    Ok((
        QueueWriter::open(
            path,
            Arc::clone(bell),
            BellRegion::open(&reader_region).map_err(io::Error::other)?,
        )
        .map_err(io::Error::other)?,
        open_reader(path, &reader_region, 0, &writer_region).map_err(io::Error::other)?,
    ))
}

/// Returns the shared writer Bell Region for this fixture.
fn writer_bell_path(queue: &Path) -> PathBuf {
    queue.with_file_name("writer.bells")
}

/// Returns the Bell Region holding the reading loop's own doorbell of one fixture Queue.
fn reader_bell_path(queue: &Path) -> PathBuf {
    queue.with_extension("reader.bells")
}

fn route(first: QueueWriter, second: QueueWriter) -> io::Result<EgressRoute> {
    Ok(EgressRoute::new(
        BTreeMap::from([
            (
                PluginInstanceId::try_from("first").map_err(io::Error::other)?,
                first,
            ),
            (
                PluginInstanceId::try_from("second").map_err(io::Error::other)?,
                second,
            ),
        ]),
        &ChannelMetrics::default(),
    ))
}

fn routes(first: QueueWriter, second: QueueWriter) -> io::Result<EgressRoutes> {
    let contract = SinkContractId::try_from("com.example.test@1.0.0").map_err(io::Error::other)?;
    Ok(HashMap::from([(contract, route(first, second)?)]))
}

fn output(
    record: EncodedEgressRecord,
    instance_id: Option<PluginInstanceId>,
) -> io::Result<EmitBoundary> {
    Ok(EmitBoundary::Payload {
        sink_contract_id: SinkContractId::try_from("com.example.test@1.0.0")
            .map_err(io::Error::other)?,
        instance_id,
        record,
    })
}

#[test]
#[ignore = "run in release mode with CRITERION_HOME set and no concurrent builds"]
fn direct_fanout_benchmark() -> io::Result<()> {
    use criterion::{BenchmarkId, Criterion};
    use std::hint::black_box;
    use std::time::Duration;

    if std::env::var_os("CRITERION_HOME").is_none() {
        return Err(io::Error::other(
            "CRITERION_HOME must be set for Egress benchmarks",
        ));
    }
    let mut criterion = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .noise_threshold(0.10)
        .without_plots();
    for target_count in [1, 4, 16] {
        let directory = tempfile::tempdir()?;
        let bell = writer_bell(directory.path())?;
        let mut targets = BTreeMap::new();
        let mut readers = Vec::new();
        for index in 0..target_count {
            let id = format!("target-{index}");
            let (writer, reader) = queue(&directory.path().join(&id), 64 * 1024, &bell)?;
            targets.insert(
                PluginInstanceId::try_from(id).map_err(io::Error::other)?,
                writer,
            );
            readers.push(reader);
        }
        let metrics = ChannelMetrics::default();
        let contract =
            SinkContractId::try_from("com.example.benchmark@1.0.0").map_err(io::Error::other)?;
        let mut routes =
            EgressRoutes::from([(contract.clone(), EgressRoute::new(targets, &metrics))]);
        let payload = vec![7; 1024];
        for selection in [
            None,
            Some(PluginInstanceId::try_from("target-0").map_err(io::Error::other)?),
        ] {
            let name = if selection.is_some() {
                "selected_egress/1024_bytes"
            } else {
                "direct_egress/1024_bytes"
            };
            criterion.bench_with_input(
                BenchmarkId::new(name, target_count),
                &target_count,
                |bencher, _| {
                    // One thread releases real Queues immediately, isolating CPU
                    // cost from scheduling, downstream latency, and capacity waits.
                    let mut round_trip = || -> io::Result<()> {
                        let outputs = [EmitBoundary::Payload {
                            sink_contract_id: contract.clone(),
                            instance_id: selection.clone(),
                            record: encoded_record(black_box(&payload))?,
                        }];
                        let BatchSendOutcome::Committed(pending) =
                            send_batch(&mut routes, &outputs, &bell, &metrics, || false)
                                .map_err(io::Error::other)?
                        else {
                            return Err(io::Error::other("Benchmark batch did not commit"));
                        };
                        let selected_count = if selection.is_some() { 1 } else { target_count };
                        for reader in readers.iter_mut().take(selected_count) {
                            let ReadOutcome::Record(record) =
                                reader.try_read().map_err(io::Error::other)?
                            else {
                                return Err(io::Error::other(
                                    "benchmark target missed the committed record",
                                ));
                            };
                            black_box(record.payload());
                            reader.release(1).map_err(io::Error::other)?;
                        }
                        assert!(is_released(&routes, &pending).map_err(io::Error::other)?);
                        Ok(())
                    };
                    bencher.iter(|| {
                        let result = round_trip();
                        assert!(
                            result.is_ok(),
                            "benchmark Queue operation failed: {result:?}"
                        );
                    });
                },
            );
        }
    }
    criterion.final_summary();
    Ok(())
}

#[test]
fn batch_waits_across_contracts_without_draining_a_queue() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let bell = writer_bell(directory.path())?;
    let (first, mut first_reader) = queue(&directory.path().join("first"), 32, &bell)?;
    let second_path = directory.path().join("second");
    let (mut second, mut second_reader) = queue(&second_path, 48, &bell)?;
    for _ in 0..2 {
        assert!(matches!(
            second.try_write_with(
                8,
                |destination| Write::write_all(destination, &[9; 8]),
                || {}
            )?,
            WriteOutcome::Committed(_)
        ));
        assert!(matches!(second_reader.try_read()?, ReadOutcome::Record(_)));
    }
    let first_contract =
        SinkContractId::try_from("com.example.first@1.0.0").map_err(io::Error::other)?;
    let second_contract =
        SinkContractId::try_from("com.example.second@2.0.0").map_err(io::Error::other)?;
    let outputs = ["first", "second", "second"]
        .into_iter()
        .map(|name| {
            Ok(EmitBoundary::Payload {
                sink_contract_id: if name == "first" {
                    first_contract.clone()
                } else {
                    second_contract.clone()
                },
                instance_id: Some(PluginInstanceId::try_from(name).map_err(io::Error::other)?),
                record: encoded_record(&[1])?,
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    let mut routes = EgressRoutes::new();
    for (contract, name, writer) in [
        (first_contract, "first", first),
        (second_contract, "second", second),
    ] {
        routes.insert(
            contract,
            EgressRoute::new(
                BTreeMap::from([(
                    PluginInstanceId::try_from(name).map_err(io::Error::other)?,
                    writer,
                )]),
                &ChannelMetrics::default(),
            ),
        );
    }
    let wake = bell.interrupter();
    let control = Arc::new(FlowChannelControl::new());
    let worker_control = Arc::clone(&control);
    let (done, result) = std::sync::mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let outcome = send_batch(
            &mut routes,
            &outputs,
            &bell,
            &ChannelMetrics::default(),
            || worker_control.is_stopping(),
        );
        let _ = done.send(outcome);
    });
    let armed = wait_for_armed_loop(
        &second_path,
        &writer_bell_path(&second_path),
        QueueWaiter::Writer,
    );
    let first_empty = matches!(
        first_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    let second_empty = matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    // One older record remains. The batch must proceed without draining it.
    second_reader.release(1).map_err(io::Error::other)?;
    let outcome = result.recv_timeout(std::time::Duration::from_secs(5));
    control.request_stop();
    wake.interrupt().map_err(io::Error::other)?;
    worker
        .join()
        .map_err(|_| io::Error::other("batch writer panicked"))?;
    let outcome = outcome
        .map_err(io::Error::other)?
        .map_err(io::Error::other)?;
    armed?;
    assert!(first_empty && second_empty);
    let BatchSendOutcome::Committed(pending) = outcome else {
        return Err(io::Error::other("batch did not commit"));
    };
    assert_eq!(pending.targets.len(), 2);
    assert!(matches!(
        first_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    for _ in 0..2 {
        assert!(matches!(
            second_reader.try_read().map_err(io::Error::other)?,
            ReadOutcome::Record(_)
        ));
    }
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    ));
    Ok(())
}

#[test]
fn batch_includes_wrap_gaps_even_when_queue_is_empty() -> io::Result<()> {
    for (start, count, fits) in [(0, 2, true), (32, 2, false), (32, 1, true)] {
        let directory = tempfile::tempdir()?;
        let bell = writer_bell(directory.path())?;
        let (mut writer, mut reader) = queue(&directory.path().join("queue"), 48, &bell)?;
        if start != 0 {
            writer.try_write_with(
                24,
                |destination| Write::write_all(destination, &[0; 24]),
                || {},
            )?;
            assert!(matches!(reader.try_read()?, ReadOutcome::Record(_)));
            reader.release(1)?;
        }
        let metrics = ChannelMetrics::default();
        let contract =
            SinkContractId::try_from("com.example.test@1.0.0").map_err(io::Error::other)?;
        let instance = PluginInstanceId::try_from("target").map_err(io::Error::other)?;
        let mut routes = EgressRoutes::from([(
            contract.clone(),
            EgressRoute::new(BTreeMap::from([(instance.clone(), writer)]), &metrics),
        )]);
        let outputs = (0..count)
            .map(|_| output(encoded_record(&[1; 8])?, Some(instance.clone())))
            .collect::<io::Result<Vec<_>>>()?;
        let outcome = send_batch(&mut routes, &outputs, &bell, &metrics, || false)
            .map_err(io::Error::other)?;
        assert_eq!(matches!(outcome, BatchSendOutcome::Committed(_)), fits);
        if fits {
            for _ in 0..count {
                assert!(matches!(reader.try_read()?, ReadOutcome::Record(_)));
            }
            reader.release(count)?;
        } else {
            assert!(matches!(outcome, BatchSendOutcome::TooLarge));
            let writer = &routes[&contract].targets[&instance].writer;
            assert_eq!(writer.committed_position()?.get(), start);
            assert_eq!(writer.free_bytes()?, 48);
        }
        assert!(matches!(reader.try_read()?, ReadOutcome::Empty));
    }
    Ok(())
}
