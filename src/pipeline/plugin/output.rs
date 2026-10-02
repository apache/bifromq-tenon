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

//! Drains and frames one Plugin process's standard output streams.
//!
//! Both pipes are drained for the whole child lifetime, independent of live
//! diagnostic interest. Interest only controls whether a completed line is
//! rendered and offered to the best-effort diagnostics transport.

use std::fmt;
use std::mem;

use tokio::io::{AsyncRead, AsyncReadExt as _};
use tokio::process::{ChildStderr, ChildStdout};
use tokio::task::JoinHandle;

use crate::contracts::core::DIAGNOSTIC_TEXT_MAXIMUM_BYTES;
use crate::contracts::core::PluginDiagnosticStream;
use crate::pipeline::diagnostics::PluginDiagnosticPublisher;

const READ_BUFFER_BYTES: usize = 4 * 1_024;

pub(super) struct PluginOutput {
    stdout: JoinHandle<()>,
    stderr: JoinHandle<()>,
}

impl PluginOutput {
    pub(super) fn new(
        stdout: ChildStdout,
        stderr: ChildStderr,
        publisher: PluginDiagnosticPublisher,
    ) -> Self {
        Self {
            stdout: spawn_drain(stdout, PluginDiagnosticStream::Stdout, publisher.clone()),
            stderr: spawn_drain(stderr, PluginDiagnosticStream::Stderr, publisher),
        }
    }
}

impl Drop for PluginOutput {
    fn drop(&mut self) {
        self.stdout.abort();
        self.stderr.abort();
    }
}

impl fmt::Debug for PluginOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginOutput")
            .field("stdout_finished", &self.stdout.is_finished())
            .field("stderr_finished", &self.stderr.is_finished())
            .finish()
    }
}

fn spawn_drain(
    stream: impl AsyncRead + Unpin + Send + 'static,
    output_stream: PluginDiagnosticStream,
    publisher: PluginDiagnosticPublisher,
) -> JoinHandle<()> {
    tokio::spawn(drain(stream, output_stream, publisher))
}

async fn drain(
    mut stream: impl AsyncRead + Unpin,
    output_stream: PluginDiagnosticStream,
    publisher: PluginDiagnosticPublisher,
) {
    let mut framing = LineFraming::new(output_stream, publisher);
    let mut buffer = [0_u8; READ_BUFFER_BYTES];
    loop {
        match stream.read(&mut buffer).await {
            Ok(0) => {
                framing.finish();
                return;
            }
            Ok(length) => framing.accept(&buffer[..length]),
            Err(_) => return,
        }
    }
}

struct LineFraming {
    bytes: Vec<u8>,
    truncated: bool,
    output_stream: PluginDiagnosticStream,
    publisher: PluginDiagnosticPublisher,
}

impl LineFraming {
    fn new(output_stream: PluginDiagnosticStream, publisher: PluginDiagnosticPublisher) -> Self {
        Self {
            bytes: Vec::new(),
            truncated: false,
            output_stream,
            publisher,
        }
    }

    fn accept(&mut self, mut input: &[u8]) {
        while !input.is_empty() {
            let newline = input.iter().position(|byte| *byte == b'\n');
            let segment = newline.map_or(input, |index| &input[..index]);
            let remaining = DIAGNOSTIC_TEXT_MAXIMUM_BYTES - self.bytes.len();
            if segment.len() > remaining {
                self.bytes.extend_from_slice(&segment[..remaining]);
                self.truncated = true;
            } else {
                self.bytes.extend_from_slice(segment);
            }
            if newline.is_some() {
                self.publish();
            }

            let Some(newline) = newline else {
                return;
            };
            input = &input[newline + 1..];
        }
    }

    fn finish(&mut self) {
        if !self.bytes.is_empty() {
            self.publish();
        }
    }

    fn publish(&mut self) {
        let mut truncated = self.truncated;
        self.truncated = false;
        if !self.publisher.is_enabled() {
            self.bytes.clear();
            return;
        }
        let (mut text, invalid_utf8) = match String::from_utf8(mem::take(&mut self.bytes)) {
            Ok(text) => (text, false),
            Err(error) => (error.into_utf8_lossy(), true),
        };
        if text.len() > DIAGNOSTIC_TEXT_MAXIMUM_BYTES {
            let mut boundary = DIAGNOSTIC_TEXT_MAXIMUM_BYTES;
            while !text.is_char_boundary(boundary) {
                boundary -= 1;
            }
            text.truncate(boundary);
            truncated = true;
        }
        self.publisher.publish(
            self.output_stream,
            text.into_boxed_str(),
            truncated,
            invalid_utf8,
        );
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::contracts::core::{
        PipelineDiagnosticRecord, PluginDiagnosticRecord, pipeline_diagnostic_record,
    };
    use crate::pipeline::diagnostics::test_support;
    use tokio::io::AsyncWriteExt as _;

    #[tokio::test(flavor = "current_thread")]
    async fn instance_output_uses_the_same_framing_and_preserves_stream_identity() -> io::Result<()>
    {
        use crate::contracts::core::pipeline_diagnostic_record::Record;
        use crate::identifiers::PluginInstanceId;
        let id = PluginInstanceId::try_from(String::from("device/a")).map_err(io::Error::other)?;
        let (publisher, mut records) = test_support::interested_instance(id);
        let (mut writer, reader) = tokio::io::duplex(64);
        let task = spawn_drain(reader, PluginDiagnosticStream::Stderr, publisher);
        writer.write_all(b"prefix-").await?;
        writer.write_all(&[0xff, b'\n']).await?;
        drop(writer);
        task.await.map_err(io::Error::other)?;
        let record = records
            .recv()
            .await
            .ok_or_else(|| io::Error::other("Instance output is missing"))?;
        assert!(matches!(record.record, Some(Record::Plugin(record))
            if record.plugin_instance_id == "device/a"
                && record.plugin_process_instance_id == 1
                && record.stream == PluginDiagnosticStream::Stderr as i32
                && record.sequence == 0
                && record.text == "prefix-\u{fffd}"
                && record.invalid_utf8 && !record.truncated));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn frames_lines_and_publishes_the_unterminated_tail() -> io::Result<()> {
        let (publisher, mut records) = test_support::interested_source();
        let (mut writer, reader) = tokio::io::duplex(64);
        let task = spawn_drain(reader, PluginDiagnosticStream::Stdout, publisher);

        writer.write_all(b"first\nsecond").await?;
        drop(writer);
        task.await.map_err(io::Error::other)?;

        assert_eq!(plugin_text(records.recv().await).as_deref(), Some("first"));
        assert_eq!(plugin_text(records.recv().await).as_deref(), Some("second"));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn bounds_one_oversized_line_and_resumes_at_the_next_line() -> io::Result<()> {
        let (publisher, mut records) = test_support::interested_source();
        let (mut writer, reader) = tokio::io::duplex(DIAGNOSTIC_TEXT_MAXIMUM_BYTES * 2);
        let task = spawn_drain(reader, PluginDiagnosticStream::Stderr, publisher);

        writer
            .write_all(&vec![b'x'; DIAGNOSTIC_TEXT_MAXIMUM_BYTES + 1])
            .await?;
        writer.write_all(b"\nafter\n").await?;
        drop(writer);
        task.await.map_err(io::Error::other)?;

        let first = plugin_record(records.recv().await)?;
        assert_eq!(first.text.len(), DIAGNOSTIC_TEXT_MAXIMUM_BYTES);
        assert!(first.truncated);
        let second = plugin_record(records.recv().await)?;
        assert_eq!(second.text, "after");
        assert!(!second.truncated);
        Ok(())
    }

    #[test]
    fn exact_maximum_line_is_not_truncated() -> io::Result<()> {
        let (publisher, mut records) = test_support::interested_source();
        let mut framing = LineFraming::new(PluginDiagnosticStream::Stdout, publisher);

        framing.accept(&vec![b'x'; DIAGNOSTIC_TEXT_MAXIMUM_BYTES]);
        framing.accept(b"\n");

        let record = plugin_record(records.try_recv().ok())?;
        assert_eq!(record.text.len(), DIAGNOSTIC_TEXT_MAXIMUM_BYTES);
        assert!(!record.truncated);
        Ok(())
    }

    #[test]
    fn oversized_line_is_published_only_after_its_boundary() -> io::Result<()> {
        let (publisher, mut records) = test_support::interested_source();
        let mut framing = LineFraming::new(PluginDiagnosticStream::Stdout, publisher);

        framing.accept(&vec![b'x'; DIAGNOSTIC_TEXT_MAXIMUM_BYTES + 1]);
        assert!(records.try_recv().is_err());
        framing.accept(b"\n");

        let record = plugin_record(records.try_recv().ok())?;
        assert_eq!(record.text.len(), DIAGNOSTIC_TEXT_MAXIMUM_BYTES);
        assert!(record.truncated);
        Ok(())
    }

    #[test]
    fn invalid_utf8_replacement_stays_within_the_text_boundary() -> io::Result<()> {
        let (publisher, mut records) = test_support::interested_source();
        let mut framing = LineFraming::new(PluginDiagnosticStream::Stderr, publisher);

        framing.accept(&vec![0xff; DIAGNOSTIC_TEXT_MAXIMUM_BYTES]);
        framing.accept(b"\n");

        let record = plugin_record(records.try_recv().ok())?;
        assert!(record.text.len() <= DIAGNOSTIC_TEXT_MAXIMUM_BYTES);
        assert!(record.truncated);
        assert!(record.invalid_utf8);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn drains_without_interest_without_publishing() -> io::Result<()> {
        let (publisher, mut records) = test_support::uninterested_source();
        let (mut writer, reader) = tokio::io::duplex(64);
        let task = spawn_drain(reader, PluginDiagnosticStream::Stdout, publisher);

        writer
            .write_all(&vec![b'x'; DIAGNOSTIC_TEXT_MAXIMUM_BYTES * 4])
            .await?;
        writer.write_all(b"\n").await?;
        drop(writer);
        task.await.map_err(io::Error::other)?;

        assert!(records.try_recv().is_err());
        Ok(())
    }

    fn plugin_text(record: Option<PipelineDiagnosticRecord>) -> Option<String> {
        plugin_record(record).ok().map(|record| record.text)
    }

    fn plugin_record(
        record: Option<PipelineDiagnosticRecord>,
    ) -> io::Result<PluginDiagnosticRecord> {
        match record.and_then(|record| record.record) {
            Some(pipeline_diagnostic_record::Record::Plugin(record)) => Ok(record),
            Some(pipeline_diagnostic_record::Record::Channel(_)) | None => {
                Err(io::Error::other("Expected one Plugin diagnostic record"))
            }
        }
    }
}
