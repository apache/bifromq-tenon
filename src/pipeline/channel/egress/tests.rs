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
use tenon_ipc::queue::contract_test_support::{open_reader, open_writer};
use tenon_ipc::queue::{DataCapacity, QueueReader, QueueWaiter, ReadOutcome, create_queue_file};

#[test]
fn a_full_last_target_prevents_any_partial_fanout_until_real_release() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let (first, mut first_reader) = queue(&directory.path().join("first"), 16)?;
    let second_path = directory.path().join("second");
    let (mut second, mut second_reader) = queue(&second_path, 16)?;
    assert!(matches!(
        second.try_write(&[9; 8]).map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    let mut route = route(first, second)?;
    let record = encoded_record(&[1])?;
    let writer = thread::spawn(move || route.send(record, &ChannelMetrics::default(), || false));

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
        SendOutcome::Committed(_)
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
    let (first, mut first_reader) = queue(&directory.path().join("first"), 16)?;
    let second_path = directory.path().join("second");
    let (mut second, mut second_reader) = queue(&second_path, 16)?;
    assert!(matches!(
        second.try_write(&[9; 8]).map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    let wake = second.wait_interrupter();
    let control = Arc::new(FlowChannelControl::new());
    let worker_control = Arc::clone(&control);
    let mut route = route(first, second)?;
    let record = encoded_record(&[1])?;
    let writer = thread::spawn(move || {
        route.send(record, &ChannelMetrics::default(), || {
            worker_control.is_stopping()
        })
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
        SendOutcome::Interrupted
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

fn queue(path: &Path, bytes: u64) -> io::Result<(QueueWriter, QueueReader)> {
    let capacity = DataCapacity::try_from(bytes).map_err(io::Error::other)?;
    let maximum = NonZeroU64::new(capacity.get() - tenon_ipc::queue::FRAME_HEADER_LEN as u64)
        .ok_or_else(|| io::Error::other("Fixture capacity must hold a payload"))?;
    create_queue_file(path, capacity, maximum).map_err(io::Error::other)?;
    let writer_region = writer_bell_path(path);
    let reader_region = reader_bell_path(path);
    for region in [&writer_region, &reader_region] {
        create_bell_region(region, NonZeroU32::MIN, 0).map_err(io::Error::other)?;
    }
    Ok((
        open_writer(path, &writer_region, 0, &reader_region).map_err(io::Error::other)?,
        open_reader(path, &reader_region, 0, &writer_region).map_err(io::Error::other)?,
    ))
}

/// Returns the Bell Region holding the writing loop's own doorbell of one fixture Queue.
fn writer_bell_path(queue: &Path) -> PathBuf {
    queue.with_extension("writer.bells")
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
        let mut targets = BTreeMap::new();
        let mut readers = Vec::new();
        for index in 0..target_count {
            let id = format!("target-{index}");
            let (writer, reader) = queue(&directory.path().join(&id), 64 * 1024)?;
            targets.insert(
                PluginInstanceId::try_from(id).map_err(io::Error::other)?,
                writer,
            );
            readers.push(reader);
        }
        let metrics = ChannelMetrics::default();
        let contract = SinkContractId::try_from("benchmark").map_err(io::Error::other)?;
        let mut routes =
            EgressRoutes::from([(contract.clone(), EgressRoute::new(targets, &metrics))]);
        let payload = vec![7; 1024];
        criterion.bench_with_input(
            BenchmarkId::new("direct_egress/1024_bytes", target_count),
            &target_count,
            |bencher, _| {
                // One thread releases real Queues immediately, isolating CPU
                // cost from scheduling, downstream latency, and capacity waits.
                let mut round_trip = || -> io::Result<()> {
                    let route = routes
                        .get_mut(&contract)
                        .ok_or_else(|| io::Error::other("Benchmark route is bound"))?;
                    let SendOutcome::Committed(committed) = route
                        .send(encoded_record(black_box(&payload))?, &metrics, || false)
                        .map_err(io::Error::other)?
                    else {
                        return Err(io::Error::other("Benchmark send was interrupted"));
                    };
                    for reader in &mut readers {
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
                    assert!(
                        is_released(
                            &routes,
                            &PendingRelease::new(contract.clone(), committed, Vec::new()),
                        )
                        .map_err(io::Error::other)?
                    );
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
    criterion.final_summary();
    Ok(())
}
