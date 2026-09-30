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

//! Real child processes, UDS gRPC and mapped Source Queue pairs.

use tenon_ipc::queue::{QueueReader, WriteOutcome};
#[path = "support/process_peer.rs"]
mod peer;
use peer::{ExpectedExit, Peer, QueueOccupancy};
use std::path::Path;
use tenon_plugin_sdk::Error;
use tenon_plugin_sdk::prost::Message;
use tenon_plugin_sdk::repository_test_support::{LOOPS_BELL_FILE_NAME, plugin, source};
use tokio::time::{Duration, timeout};

/// The Submission loop's doorbell in the Source's own-loop Bell Region.
const SUBMISSION_SLOT: u32 = 0;
/// The Completion loop's doorbell in the Source's own-loop Bell Region.
const COMPLETION_SLOT: u32 = 1;
/// The doorbell word meaning a waiting loop armed the slot and was never rung.
const SLOT_ARMED: u32 = 0;

/// Reads one doorbell word out of a Bell Region the way a peer would.
fn slot_word(path: &Path, slot: u32) -> Result<u32, Error> {
    let bytes = std::fs::read(path)?;
    let start = 64 + 64 * slot as usize;
    let word = bytes
        .get(start..start + 4)
        .ok_or("Bell Region is shorter than its slot table")?;
    Ok(u32::from_le_bytes(word.try_into()?))
}

/// Waits until `slot` reports that a waiting loop has armed it.
async fn await_armed(path: &Path, slot: u32) -> Result<(), Error> {
    timeout(Duration::from_secs(10), async {
        loop {
            if slot_word(path, slot)? == SLOT_ARMED {
                return Ok::<(), Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}

async fn submission(reader: &mut QueueReader) -> Result<source::IngressRecord, Error> {
    timeout(Duration::from_secs(10), async {
        loop {
            if let tenon_ipc::queue::ReadOutcome::Record(record) = reader.try_read()? {
                let record = source::IngressRecord::decode(record.payload())?;
                reader.release(1)?;
                return Ok(record);
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

#[tokio::test]
async fn one_source_preserves_completion_and_cleanup() -> Result<(), Error> {
    let mut peer = Peer::start(serde_json::json!({}), QueueOccupancy::Empty).await?;
    peer.ready().await?;
    let mut reader = peer.reader()?;
    let record = submission(&mut reader).await?;
    assert_eq!(Vec::<u8>::decode(record.payload)?, vec![0; 8]);
    peer.quiesce().await?;
    peer.quiesced().await?;
    assert!(matches!(
        {
            let record = source::IngressCompletion {
                record_id: record.record_id,
                status: 1,
            };
            peer.writer()?.try_write_with(
                record.encoded_len(),
                |destination| record.encode(destination).map_err(std::io::Error::other),
                || {},
            )
        }?,
        WriteOutcome::Committed(_)
    ));
    peer.event("source-result Ok(Ok)").await?;
    let events = peer.finish().await?;
    assert!(events.iter().any(|event| event == "source-quiesce"));
    assert!(events.iter().any(|event| event == "source-close"));
    Ok(())
}

#[tokio::test]
async fn source_start_panic_exits_before_close() -> Result<(), Error> {
    let peer = Peer::start(
        serde_json::json!({"mode": "failed-start"}),
        QueueOccupancy::Empty,
    )
    .await?;
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert!(!events.iter().any(|event| event == "source-close"));
    assert!(
        events
            .iter()
            .any(|event| event.contains("Business start failed"))
    );
    Ok(())
}

#[tokio::test]
async fn source_quiesce_panic_exits_before_close() -> Result<(), Error> {
    let mut peer = Peer::start(
        serde_json::json!({"mode": "failed-quiesce", "send": false}),
        QueueOccupancy::Empty,
    )
    .await?;
    peer.ready().await?;
    peer.quiesce().await?;
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert!(events.iter().any(|event| event == "source-quiesce"));
    assert!(!events.iter().any(|event| event == "source-close"));
    Ok(())
}

#[tokio::test]
async fn factory_failure_and_abandoned_program_exit_without_business_cleanup() -> Result<(), Error>
{
    for mode in ["failed-factory", "drop-program"] {
        let peer = Peer::start(
            serde_json::json!({"mode": mode, "send": false}),
            QueueOccupancy::Empty,
        )
        .await?;
        let events = peer.exit(ExpectedExit::Failure).await?;
        assert!(
            !events.iter().any(|event| event == "source-close"),
            "{events:?}"
        );
        assert!(
            !events.iter().any(|event| event.starts_with("failure ")),
            "error must not return to author: {events:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn source_close_panic_ends_the_process_at_the_callback() -> Result<(), Error> {
    let mut peer = Peer::start(
        serde_json::json!({"mode": "panic-close", "send": false}),
        QueueOccupancy::Empty,
    )
    .await?;
    peer.ready().await?;
    peer.quiesce().await?;
    peer.quiesced().await?;
    peer.shutdown().await?;
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert!(events.iter().any(|event| event == "source-close-enter"));
    assert!(!events.iter().any(|event| event == "source-close"));
    Ok(())
}

#[tokio::test]
async fn one_source_can_send_to_every_flow_channel() -> Result<(), Error> {
    let parallelism = 3;
    let mut peer = Peer::start(
        serde_json::json!({"mode": "all-channels", "parallelism": parallelism, "sends": parallelism}),
        QueueOccupancy::Empty,
    )
    .await?;
    peer.ready().await?;
    for channel in 0..parallelism {
        let mut reader = peer.submission(channel)?;
        let mut writer = peer.completion(channel)?;
        let record = submission(&mut reader).await?;
        assert_eq!(Vec::<u8>::decode(record.payload)?, vec![0; 8]);
        assert!(matches!(
            {
                let record = source::IngressCompletion {
                    record_id: record.record_id,
                    status: 1,
                };
                writer.try_write_with(
                    record.encoded_len(),
                    |destination| record.encode(destination).map_err(std::io::Error::other),
                    || {},
                )
            }?,
            WriteOutcome::Committed(_)
        ));
    }
    peer.quiesce().await?;
    peer.quiesced().await?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test]
async fn malformed_lifecycle_command_fails_the_single_source_program() -> Result<(), Error> {
    let mut peer = Peer::start(serde_json::json!({"send": false}), QueueOccupancy::Empty).await?;
    peer.ready().await?;
    peer.outgoing
        .as_ref()
        .ok_or("missing stream")?
        .send(Ok(plugin::PipelineToPlugin { message: None }))
        .await?;
    peer.exit(ExpectedExit::Failure).await?;
    Ok(())
}

#[tokio::test]
async fn closing_stdin_terminates_a_source_whose_loops_are_parked() -> Result<(), Error> {
    let mut peer = Peer::start(serde_json::json!({"send": false}), QueueOccupancy::Empty).await?;
    peer.ready().await?;
    // Nothing rings these two doorbells: the Source admitted no Submission and no
    // Completion is pending, so both loops stay parked on their own Region.
    let loops = peer.working.join("source").join(LOOPS_BELL_FILE_NAME);
    await_armed(&loops, SUBMISSION_SLOT).await?;
    await_armed(&loops, COMPLETION_SLOT).await?;
    assert!(
        peer.child.try_wait()?.is_none(),
        "the process must stay alive while its loops are parked"
    );
    // The Runner ends a Plugin by closing stdin; the Plugin owns no force wake
    // and must not need one to end, so the parked loops cannot hold it open.
    peer.stdin.take();
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert!(
        !events.iter().any(|event| event == "source-close"),
        "a parked loop must not be waited for: {events:?}"
    );
    Ok(())
}

#[tokio::test]
async fn owner_loss_interrupts_every_blocked_source_callback() -> Result<(), Error> {
    for mode in ["blocked-start", "blocked-quiesce", "blocked-close"] {
        for stdin_loss in [true, false] {
            let mut peer = Peer::start(
                serde_json::json!({"mode": mode, "send": false}),
                QueueOccupancy::Empty,
            )
            .await?;
            if mode != "blocked-start" {
                peer.ready().await?;
                peer.quiesce().await?;
            }
            if mode == "blocked-close" {
                peer.quiesced().await?;
                peer.shutdown().await?;
            }
            peer.event(match mode {
                "blocked-start" => "source-start",
                "blocked-quiesce" => "source-quiesce",
                _ => "source-close-enter",
            })
            .await?;
            if stdin_loss {
                peer.stdin.take();
            } else {
                peer.outgoing.take();
            }
            let events = peer.exit(ExpectedExit::Failure).await?;
            assert!(
                !events.iter().any(|line| line == "source-close"),
                "{mode}: {events:?}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn completion_queue_failure_interrupts_start_and_backpressured_quiesce() -> Result<(), Error>
{
    for starting in [true, false] {
        let mut peer = Peer::start(
            serde_json::json!({"mode": if starting { "blocked-start" } else { "normal" }, "parallelism": 2}),
            if starting { QueueOccupancy::Empty } else { QueueOccupancy::Full }).await?;
        if starting {
            peer.event("source-start").await?;
        } else {
            peer.ready().await?;
            peer.quiesce().await?;
            peer.event("source-quiesce").await?;
        }
        let mut writer = peer.completion(1)?;
        assert!(matches!(
            {
                let record = source::IngressCompletion {
                    record_id: 1,
                    status: 0,
                };
                writer.try_write_with(
                    record.encoded_len(),
                    |destination| record.encode(destination).map_err(std::io::Error::other),
                    || {},
                )
            }?,
            WriteOutcome::Committed(_)
        ));
        let events = peer.exit(ExpectedExit::Failure).await?;
        assert!(
            !events.iter().any(|line| line == "source-close"),
            "{events:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn supervisor_can_force_stop_a_blocked_source_quiesce() -> Result<(), Error> {
    let mut peer = Peer::start(
        serde_json::json!({"mode": "blocked-quiesce", "send": false}),
        QueueOccupancy::Empty,
    )
    .await?;
    peer.ready().await?;
    peer.quiesce().await?;
    peer.event("source-quiesce").await?;
    peer.child.start_kill()?;
    let events = peer.exit(ExpectedExit::Forced).await?;
    assert!(!events.iter().any(|line| line == "source-close"));
    Ok(())
}
