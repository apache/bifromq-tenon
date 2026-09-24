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

//! The shared business owner through real processes, lifecycle and both Queue directions.

use tenon_ipc::queue::WriteOutcome;
#[path = "support/process_peer.rs"]
mod peer;
use peer::{DEADLINE, ExpectedExit, Peer};
use std::path::Path;
use tenon_plugin_sdk::prost::Message;
use tenon_plugin_sdk::repository_test_support::{sink, source};
use tenon_plugin_sdk::{Error, FlowChannel};
use tokio::time::timeout;

/// The one Flow whose Channel loops this fixture plays beside the shared Sink.
///
/// The Flow Region path travels with the input because the Core runtime owns
/// where that Region lives; a Side only learns it from its startup record.
fn channels(_working: &Path) -> Vec<FlowChannel> {
    vec![FlowChannel {
        flow_id: "input".into(),
        channel_id: 0,
    }]
}

fn write(peer: &Peer, value: &str) -> Result<(), Error> {
    let mut writer = peer.egress(0)?;
    assert!(matches!(
        writer.try_write(
            &sink::EgressRecord {
                payload: value.to_owned().encode_to_vec().into()
            }
            .encode_to_vec()
        )?,
        WriteOutcome::Committed(_)
    ));
    Ok(())
}

async fn released(peer: &Peer) -> Result<(), Error> {
    timeout(DEADLINE, async {
        loop {
            let bytes = std::fs::read(peer.egress_path(0)?)?;
            if bytes[64..72] == bytes[128..136] {
                return Ok::<_, Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

#[tokio::test]
async fn one_shared_owner_keeps_sink_and_completion_alive_through_source_quiesce()
-> Result<(), Error> {
    let vector = peer::lifecycle_vector("shared-owner")?;
    let mut peer =
        Peer::start_source_and_sink(serde_json::json!({"mode": "held-quiesce"}), channels).await?;
    let pid = peer.child.id().ok_or("missing child pid")?;
    peer.ready().await?;
    let mut reader = peer.reader()?;
    let record = timeout(DEADLINE, async {
        loop {
            if let tenon_ipc::queue::ReadOutcome::Record(record) = reader.try_read()? {
                let record = source::IngressRecord::decode(record.payload())?;
                reader.release(1)?;
                return Ok::<_, Error>(record);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(Vec::<u8>::decode(record.payload)?, vec![0; 8]);
    peer.quiesce().await?;
    peer.event("source-quiesce").await?;
    write(&peer, "during-quiesce")?;
    released(&peer).await?;
    std::fs::write(peer.working.join("callback-gate"), [])?;
    peer.quiesced().await?;
    write(
        &peer,
        vector["sinkWriteAfterQuiesce"]
            .as_str()
            .ok_or("missing post-quiesce write")?,
    )?;
    released(&peer).await?;
    assert!(matches!(
        peer.writer()?.try_write(
            &source::IngressCompletion {
                record_id: record.record_id,
                status: 1
            }
            .encode_to_vec()
        )?,
        WriteOutcome::Committed(_)
    ));
    let completion_event = match vector["completionAfterQuiesce"].as_str() {
        Some("OK") => "result 0 Ok(Ok)",
        _ => return Err("unknown completion outcome".into()),
    };
    peer.event(completion_event).await?;
    assert_eq!(peer.child.id(), Some(pid));
    let events = peer.finish().await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| event.starts_with("factory "))
            .count(),
        1
    );
    for event in ["opened", "shared-start", "source-close", "shared-close"] {
        assert_eq!(
            events.iter().filter(|line| *line == event).count(),
            1,
            "{events:?}"
        );
    }
    assert_eq!(events.last().map(String::as_str), Some("shared-close"));
    peer::assert_business_events(&vector, &events)?;
    Ok(())
}

#[tokio::test]
async fn combined_program_with_only_sink_bound_writes_and_closes_without_source()
-> Result<(), Error> {
    let mut peer =
        Peer::start_source_and_sink(serde_json::json!({"boundSource": false}), channels).await?;
    peer.ready().await?;
    assert!(!peer.working.join("source").exists());
    write(&peer, "sink-only")?;
    released(&peer).await?;
    let events = peer.finish().await?;
    assert!(events.iter().any(|line| line == "factory channels 0"));
    assert!(
        !events
            .iter()
            .any(|line| line.starts_with("source-") || line == "create 0")
    );
    assert_eq!(
        events.iter().filter(|line| *line == "shared-close").count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn combined_program_with_only_source_bound_submits_and_closes_without_sink()
-> Result<(), Error> {
    let mut peer = Peer::start_source_and_sink(serde_json::json!({}), |_| Vec::new()).await?;
    peer.ready().await?;
    assert!(!peer.working.join("sink").exists());
    let mut reader = peer.reader()?;
    let record = timeout(DEADLINE, async {
        loop {
            if let tenon_ipc::queue::ReadOutcome::Record(record) = reader.try_read()? {
                let record = source::IngressRecord::decode(record.payload())?;
                reader.release(1)?;
                return Ok::<_, Error>(record);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert!(matches!(
        peer.writer()?.try_write(
            &source::IngressCompletion {
                record_id: record.record_id,
                status: 1
            }
            .encode_to_vec()
        )?,
        WriteOutcome::Committed(_)
    ));
    peer.event("result 0 Ok(Ok)").await?;
    peer.quiesce().await?;
    peer.quiesced().await?;
    let events = peer.finish().await?;
    assert!(events.iter().any(|line| line == "sink channels 0"));
    assert_eq!(
        events.iter().filter(|line| *line == "shared-close").count(),
        1
    );
    assert!(!events.iter().any(|line| line.starts_with("write ")));
    Ok(())
}

#[tokio::test]
async fn sink_only_combined_write_failure_and_unexpected_quiesce_terminate() -> Result<(), Error> {
    for fail_write in [true, false] {
        let mut peer =
            Peer::start_source_and_sink(serde_json::json!({"boundSource": false}), channels)
                .await?;
        peer.ready().await?;
        if fail_write {
            write(&peer, "fail")?;
        } else {
            peer.quiesce().await?;
        }
        let events = peer.exit(ExpectedExit::Failure).await?;
        assert!(!events.iter().any(|line| line == "shared-close"));
    }
    Ok(())
}

#[tokio::test]
async fn combined_close_failure_stops_before_shared_resource_cleanup() -> Result<(), Error> {
    let mut peer = Peer::start_source_and_sink(
        serde_json::json!({"failClose": true, "send": false}),
        channels,
    )
    .await?;
    peer.ready().await?;
    peer.quiesce().await?;
    peer.quiesced().await?;
    peer.shutdown().await?;
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert_eq!(
        events
            .iter()
            .filter(|event| *event == "source-close")
            .count(),
        1
    );
    assert!(
        !events.iter().any(|event| event == "shared-close"),
        "{events:?}"
    );
    Ok(())
}

#[tokio::test]
async fn sink_failure_interrupts_source_quiesce_waiting_for_queue_space() -> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    std::fs::create_dir(&source)?;
    peer::queue(&source.join("submission-0.queue"), 144, 64)?;
    peer::queue(&source.join("completion-0.queue"), 48, 13)?;
    let channels = channels(directory.path());
    let bells = peer::shared_bells(directory.path(), &channels)?;
    let sink = peer::sink_queue(directory.path(), &channels[0]);
    std::fs::create_dir_all(sink.parent().ok_or("missing Queue directory")?)?;
    peer::queue(&sink, 256, 64)?;
    let mut reader = bells.submission_reader(directory.path(), 0)?;
    // Configuration replacement cancels Starting instances without draining their
    // sessions. Two real cancelled starts leave one admitted frame each behind.
    for _ in 0..2 {
        let mut previous = Peer::start_source_and_sink_in(
            serde_json::json!({"mode": "held-ready", "payloadBytes": 56}),
            channels.clone(),
            directory.path(),
        )
        .await?;
        previous.event("waiting-before-ready").await?;
        timeout(DEADLINE, async {
            loop {
                if matches!(reader.try_read()?, tenon_ipc::queue::ReadOutcome::Record(_)) {
                    return Ok::<_, Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        previous.stdin.take();
        let events = previous.exit(ExpectedExit::Failure).await?;
        assert!(!events.iter().any(|line| line == "shared-close"));
    }
    let mut peer =
        Peer::start_source_and_sink_in(serde_json::json!({}), channels.clone(), directory.path())
            .await?;
    peer.ready().await?;
    peer.quiesce().await?;
    peer.event("source-quiesce").await?;
    let mut writer = bells.egress(directory.path(), &channels[0])?;
    assert!(matches!(
        writer.try_write(
            &sink::EgressRecord {
                payload: "fail".to_owned().encode_to_vec().into()
            }
            .encode_to_vec()
        )?,
        WriteOutcome::Committed(_)
    ));
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert!(!events.iter().any(|line| line == "source-close"));
    assert!(!events.iter().any(|line| line == "shared-close"));
    assert!(
        events
            .iter()
            .any(|line| line.starts_with("Plugin process cannot continue:")
                && line.contains("Shared write failed")),
        "{events:?}"
    );
    Ok(())
}

#[tokio::test]
async fn one_shared_owner_starts_one_source_and_closes_shared_resources() -> Result<(), Error> {
    let mut peer =
        Peer::start_source_and_sink(serde_json::json!({"send": false}), channels).await?;
    peer.ready().await?;
    peer.quiesce().await?;
    peer.quiesced().await?;
    let events = peer.finish().await?;
    assert_eq!(events.iter().filter(|line| *line == "create 0").count(), 1);
    assert_eq!(
        events.iter().filter(|line| *line == "source-start").count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|line| *line == "source-quiesce")
            .count(),
        1
    );
    assert_eq!(
        events.iter().filter(|line| *line == "source-close").count(),
        1
    );
    assert_eq!(
        events.iter().filter(|line| *line == "shared-close").count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn owner_loss_interrupts_shared_owner_before_ready_during_quiesce_and_close()
-> Result<(), Error> {
    for mode in ["held-ready", "held-quiesce", "held-close"] {
        for stdin_loss in [true, false] {
            let mut peer = Peer::start_source_and_sink(
                serde_json::json!({"mode": mode, "send": false}),
                channels,
            )
            .await?;
            if mode != "held-ready" {
                peer.ready().await?;
                peer.quiesce().await?;
            }
            if mode == "held-close" {
                peer.quiesced().await?;
                peer.shutdown().await?;
            }
            peer.event(match mode {
                "held-ready" => "waiting-before-ready",
                "held-quiesce" => "source-quiesce",
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
                !events.iter().any(|line| line == "shared-close"),
                "{mode}: {events:?}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn shared_sink_failure_is_fatal_before_and_after_source_quiesce() -> Result<(), Error> {
    for quiesced in [false, true] {
        let mut peer =
            Peer::start_source_and_sink(serde_json::json!({"send": false}), channels).await?;
        peer.ready().await?;
        if quiesced {
            peer.quiesce().await?;
            peer.quiesced().await?;
        }
        write(&peer, "fail")?;
        let events = peer.exit(ExpectedExit::Failure).await?;
        assert!(!events.iter().any(|line| line == "shared-close"));
    }
    Ok(())
}

#[tokio::test]
async fn shutdown_drops_borrowed_pending_sink_result_before_closing_owner() -> Result<(), Error> {
    let mut peer = Peer::start_source_and_sink(
        serde_json::json!({"mode": "pending-write", "send": false}),
        channels,
    )
    .await?;
    peer.ready().await?;
    write(&peer, "pending")?;
    peer.event("write input 0 [\"pending\"]").await?;
    peer.quiesce().await?;
    peer.quiesced().await?;
    let path = peer.egress_path(0)?;
    let mut final_queue = std::fs::File::open(&path)?;
    let events = peer.finish().await?;
    let position = |event| events.iter().position(|line| line == event);
    assert!(
        position("observer-dropped").ok_or("missing observer drop")?
            < position("source-close").ok_or("missing Source close")?
    );
    assert!(
        position("source-close").ok_or("missing Source close")?
            < position("shared-close").ok_or("missing owner close")?
    );
    let mut header = [0; 136];
    std::io::Read::read_exact(&mut final_queue, &mut header)?;
    assert_ne!(&header[64..72], &[0; 8], "write was committed");
    assert_eq!(
        &header[128..136],
        &[0; 8],
        "abandoned write remains unreleased"
    );
    Ok(())
}

#[tokio::test]
async fn shared_invalid_lifecycle_vectors_fail_real_sdk_processes() -> Result<(), Error> {
    let vectors = peer::lifecycle_vectors()?;
    for vector in vectors["lifecycleInvalid"]
        .as_array()
        .ok_or("missing invalid lifecycle vectors")?
    {
        if vector["receiver"] != "sdk" {
            continue;
        }
        let config = serde_json::json!({"send": false});
        let mut peer = match vector["interface"].as_str().ok_or("missing interface")? {
            "source" => Peer::start(config, peer::QueueOccupancy::Empty).await?,
            "sink" => Peer::start_sink(config, channels).await?,
            "source-and-sink" => Peer::start_source_and_sink(config, channels).await?,
            _ => return Err("unknown lifecycle interface".into()),
        };
        for event in vector["events"]
            .as_array()
            .ok_or("missing lifecycle events")?
        {
            match event.as_str().ok_or("invalid event")? {
                "control.attach" => {} // Peer::start already verified the actual first message.
                "control.ready" => peer.ready().await?,
                "control.quiesce-source" => peer.quiesce().await?,
                "control.shutdown" => peer.shutdown().await?,
                "stdin.eof" => {
                    peer.stdin.take();
                }
                "control.stream-eof" => {
                    peer.outgoing.take();
                }
                _ => return Err("unhandled lifecycle event".into()),
            }
        }
        let expected = match vector["exitCode"].as_i64() {
            Some(1) => ExpectedExit::Failure,
            _ => return Err("invalid lifecycle failure exit code".into()),
        };
        let events = peer.exit(expected).await?;
        let closed = events
            .iter()
            .filter(|line| matches!(line.as_str(), "source-close" | "close" | "shared-close"))
            .count();
        assert_eq!(
            closed as u64,
            vector["businessCloseCount"]
                .as_u64()
                .ok_or("missing close count")?,
            "{}: {events:?}",
            vector["name"]
        );
    }
    Ok(())
}

#[tokio::test]
async fn missing_sink_queue_rejects_shared_program_before_factory_creation() -> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    std::fs::create_dir(&source)?;
    peer::queue(&source.join("submission-0.queue"), 144, 64)?;
    peer::queue(&source.join("completion-0.queue"), 48, 13)?;
    let peer = Peer::start_source_and_sink_in(
        serde_json::json!({"send": false}),
        channels(directory.path()),
        directory.path(),
    )
    .await?;
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert!(
        !events.iter().any(|event| event.starts_with("factory ")),
        "{events:?}"
    );
    Ok(())
}
