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

//! Real Sink process lifecycle, Queue release, replay and failure boundaries.

use tenon_ipc::queue::WriteOutcome;
#[path = "support/process_peer.rs"]
mod peer;
use peer::{DEADLINE, ExpectedExit, Peer};
use std::io::Read;
use std::path::Path;
use tenon_plugin_sdk::prost::Message;
use tenon_plugin_sdk::repository_test_support::{plugin, sink};
use tenon_plugin_sdk::{Error, FlowChannel};
use tokio::time::timeout;

fn channels(_working: &Path) -> Vec<FlowChannel> {
    vec![
        FlowChannel {
            flow_id: "main".into(),
            channel_id: 0,
        },
        FlowChannel {
            flow_id: "other".into(),
            channel_id: 0,
        },
    ]
}

fn write(peer: &Peer, channel: usize, text: &str) -> Result<(), Error> {
    let mut writer = peer.egress(channel)?;
    let bytes = sink::EgressRecord {
        payload: text.to_owned().encode_to_vec().into(),
    }
    .encode_to_vec();
    assert!(matches!(
        writer.try_write(&bytes)?,
        WriteOutcome::Committed(_)
    ));
    Ok(())
}

async fn released(peer: &Peer, channel: usize) -> Result<(), Error> {
    released_path(&peer.egress_path(channel)?).await
}

async fn released_path(path: &std::path::Path) -> Result<(), Error> {
    timeout(DEADLINE, async {
        loop {
            let bytes = std::fs::read(path)?;
            if bytes[64..72] == bytes[128..136] {
                return Ok::<_, Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

fn assert_unreleased(mut queue: std::fs::File) -> Result<(), Error> {
    // Read the final shared positions after process exit, not a reader's old cursor.
    let mut header = [0; 136];
    queue.read_exact(&mut header)?;
    assert_ne!(&header[64..72], &[0; 8], "the frame was committed");
    assert_eq!(&header[128..136], &[0; 8], "the frame remains unreleased");
    Ok(())
}

#[tokio::test]
async fn normal_sink_process_releases_each_queue_and_closes_one_owner() -> Result<(), Error> {
    let vector = peer::lifecycle_vector("sink-shutdown")?;
    let mut peer = Peer::start_sink(serde_json::json!({}), channels).await?;
    peer.ready().await?;
    write(&peer, 0, "first")?;
    peer.event("write main 0 [\"first\"]").await?;
    released(&peer, 0).await?;
    write(&peer, 1, "second")?;
    peer.event("write other 0 [\"second\"]").await?;
    released(&peer, 1).await?;
    let events = peer.finish().await?;
    peer::assert_business_events(&vector, &events)?;
    assert_eq!(events.iter().filter(|line| *line == "factory").count(), 1);
    assert_eq!(events.iter().filter(|line| *line == "start").count(), 1);
    assert_eq!(events.iter().filter(|line| *line == "close").count(), 1);
    assert_eq!(events.last().map(String::as_str), Some("close"));
    Ok(())
}

#[tokio::test]
async fn a_missing_queue_prevents_factory_and_business_start() -> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    let identities = channels(directory.path());
    let path = peer::sink_queue(directory.path(), &identities[0]);
    std::fs::create_dir_all(path.parent().ok_or("missing Queue directory")?)?;
    peer::queue(&path, 256, 64)?;
    let peer = Peer::start_sink_in(serde_json::json!({}), identities, directory.path()).await?;
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert!(
        !events
            .iter()
            .any(|line| matches!(line.as_str(), "factory" | "start" | "close"))
    );
    assert!(
        events
            .iter()
            .any(|line| line.starts_with("Plugin process cannot continue:"))
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_waits_for_entered_write_until_the_parent_forces_termination() -> Result<(), Error>
{
    let mut peer = Peer::start_sink(serde_json::json!({"mode": "blocked-write"}), channels).await?;
    peer.ready().await?;
    write(&peer, 0, "blocked")?;
    peer.event("write main 0 [\"blocked\"]").await?;
    peer.shutdown().await?;
    assert!(
        timeout(std::time::Duration::from_millis(200), peer.child.wait())
            .await
            .is_err()
    );
    peer.child.start_kill()?;
    let events = peer.exit(ExpectedExit::Forced).await?;
    assert!(!events.iter().any(|line| line == "close"));
    Ok(())
}

#[tokio::test]
async fn self_waking_results_make_progress_on_every_input() -> Result<(), Error> {
    let mut peer = Peer::start_sink(serde_json::json!({"mode": "self-wake"}), channels).await?;
    peer.ready().await?;
    write(&peer, 0, "first")?;
    write(&peer, 1, "second")?;
    released(&peer, 0).await?;
    released(&peer, 1).await?;
    peer.finish().await?;
    Ok(())
}

#[tokio::test]
async fn one_input_holds_an_unfinished_result_while_another_input_makes_progress()
-> Result<(), Error> {
    let mut peer =
        Peer::start_sink(serde_json::json!({"mode": "hold-first-input"}), channels).await?;
    peer.ready().await?;
    write(&peer, 0, "held")?;
    peer.event("write main 0 [\"held\"]").await?;
    write(&peer, 1, "free")?;
    peer.event("write other 0 [\"free\"]").await?;
    released(&peer, 1).await?;
    let held = std::fs::File::open(peer.egress_path(0)?)?;
    peer.finish().await?;
    assert_unreleased(held)?;
    Ok(())
}

#[tokio::test]
async fn pending_results_are_dropped_before_close_without_advancing_release() -> Result<(), Error> {
    let mut peer = Peer::start_sink(serde_json::json!({"mode": "pending"}), channels).await?;
    peer.ready().await?;
    write(&peer, 0, "replay")?;
    peer.event("write main 0 [\"replay\"]").await?;
    let path = peer.egress_path(0)?;
    let queue = std::fs::File::open(&path)?;
    let events = peer.finish().await?;
    let drop = events
        .iter()
        .position(|line| line == "observer-dropped")
        .ok_or("observer was not dropped")?;
    let close = events
        .iter()
        .position(|line| line == "close")
        .ok_or("business was not closed")?;
    assert!(drop < close);
    assert_unreleased(queue)
}

#[tokio::test]
async fn malformed_record_exits_before_business_close_and_keeps_the_record_unreleased()
-> Result<(), Error> {
    let mut peer = Peer::start_sink(serde_json::json!({}), channels).await?;
    peer.ready().await?;
    let path = peer.egress_path(0)?;
    let queue = std::fs::File::open(&path)?;
    assert!(matches!(
        peer.egress(0)?.try_write(&[10, 2, 1])?,
        WriteOutcome::Committed(_)
    ));
    let events = peer.exit(ExpectedExit::Failure).await?;
    assert!(
        events
            .iter()
            .any(|line| line.contains("sink.egress_record_invalid"))
    );
    assert!(!events.iter().any(|line| line == "close"));
    assert_unreleased(queue)
}

#[tokio::test]
async fn sink_rejects_source_commands_and_shutdown_before_ready() -> Result<(), Error> {
    for mode in ["quiesce", "early-shutdown", "empty-command"] {
        let mut peer = Peer::start_sink(serde_json::json!({"mode": if mode == "early-shutdown" { "blocked-start" } else { "normal" }}), channels).await?;
        if mode == "early-shutdown" {
            peer.event("start").await?;
            peer.shutdown().await?;
        } else {
            peer.ready().await?;
            if mode == "quiesce" {
                peer.quiesce().await?;
            } else {
                peer.outgoing
                    .as_ref()
                    .ok_or("missing stream")?
                    .send(Ok(plugin::PipelineToPlugin { message: None }))
                    .await?;
            }
        }
        let events = peer.exit(ExpectedExit::Failure).await?;
        assert!(!events.iter().any(|line| line == "close"));
    }
    Ok(())
}

#[tokio::test]
async fn owner_loss_interrupts_blocked_business_methods_without_cleanup() -> Result<(), Error> {
    for mode in ["blocked-start", "blocked-write", "blocked-close"] {
        for loss in ["stdin", "stream"] {
            let mut peer = Peer::start_sink(serde_json::json!({"mode": mode}), channels).await?;
            match mode {
                "blocked-start" => peer.event("start").await?,
                "blocked-write" => {
                    peer.ready().await?;
                    write(&peer, 0, "blocked")?;
                    peer.event("write main 0 [\"blocked\"]").await?;
                }
                _ => {
                    peer.ready().await?;
                    peer.shutdown().await?;
                    peer.event("close").await?;
                }
            }
            if loss == "stdin" {
                peer.stdin.take();
            } else {
                peer.outgoing.take();
            }
            let events = peer.exit(ExpectedExit::Failure).await?;
            assert_eq!(
                events.iter().filter(|line| *line == "close").count(),
                usize::from(mode == "blocked-close")
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn sink_panics_terminate_before_unwinding_or_later_callbacks() -> Result<(), Error> {
    for mode in [
        "panic-factory",
        "panic-start",
        "panic-thread",
        "panic-write",
        "panic-poll",
        "panic-close",
    ] {
        let mut peer = Peer::start_sink(serde_json::json!({"mode": mode}), channels).await?;
        if matches!(mode, "panic-write" | "panic-poll" | "panic-close") {
            peer.ready().await?;
            if mode == "panic-close" {
                peer.shutdown().await?;
            } else {
                write(&peer, 0, "panic")?;
            }
        }
        let events = peer.exit(ExpectedExit::Failure).await?;
        assert!(!events.iter().any(|line| line == "panic-unwound"), "{mode}");
        assert_eq!(
            events.iter().filter(|line| *line == "close").count(),
            usize::from(mode == "panic-close"),
            "{mode}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_replacement_process_replays_only_each_queues_unreleased_suffix() -> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    let identities = channels(directory.path());
    let bells = peer::sink_bells(directory.path(), &identities)?;
    let mut writers = Vec::new();
    for channel in &identities {
        let path = peer::sink_queue(directory.path(), channel);
        std::fs::create_dir_all(path.parent().ok_or("missing Queue directory")?)?;
        peer::queue(&path, 256, 64)?;
        writers.push(bells.egress(directory.path(), channel)?);
    }
    let encoded = |text: &str| {
        sink::EgressRecord {
            payload: text.to_owned().encode_to_vec().into(),
        }
        .encode_to_vec()
    };
    let mut first =
        Peer::start_sink_in(serde_json::json!({}), identities.clone(), directory.path()).await?;
    first.ready().await?;
    assert!(matches!(
        writers[0].try_write(&encoded("confirmed"))?,
        WriteOutcome::Committed(_)
    ));
    first.event("write main 0 [\"confirmed\"]").await?;
    released_path(&peer::sink_queue(directory.path(), &identities[0])).await?;
    first.finish().await?;
    let mut pending = Peer::start_sink_in(
        serde_json::json!({"mode": "pending"}),
        identities.clone(),
        directory.path(),
    )
    .await?;
    pending.ready().await?;
    assert!(matches!(
        writers[0].try_write(&encoded("replay-a"))?,
        WriteOutcome::Committed(_)
    ));
    pending.event("write main 0 [\"replay-a\"]").await?;
    assert!(matches!(
        writers[1].try_write(&encoded("replay-b"))?,
        WriteOutcome::Committed(_)
    ));
    pending.event("write other 0 [\"replay-b\"]").await?;
    pending.finish().await?;
    let mut replacement =
        Peer::start_sink_in(serde_json::json!({}), identities.clone(), directory.path()).await?;
    replacement.ready().await?;
    let expected = [
        "write main 0 [\"replay-a\"]",
        "write other 0 [\"replay-b\"]",
    ];
    let mut lines = Vec::new();
    while !expected
        .iter()
        .all(|expected| lines.iter().any(|line| line == expected))
    {
        lines.push(
            timeout(DEADLINE, replacement.stdout.next_line())
                .await??
                .ok_or("missing replay output")?,
        );
    }
    // Both files and their original live writers survive the process replacement.
    for (index, writer) in writers.iter_mut().enumerate() {
        let value = format!("after-replacement-{index}");
        assert!(matches!(
            writer.try_write(&encoded(&value))?,
            WriteOutcome::Committed(_)
        ));
        released_path(&peer::sink_queue(directory.path(), &identities[index])).await?;
    }
    let events = replacement.finish().await?;
    lines.extend(events);
    assert!(!lines.iter().any(|line| line.contains("confirmed")));
    assert!(
        lines
            .iter()
            .any(|line| line.contains("after-replacement-0"))
    );
    assert!(
        lines
            .iter()
            .any(|line| line.contains("after-replacement-1"))
    );
    Ok(())
}
