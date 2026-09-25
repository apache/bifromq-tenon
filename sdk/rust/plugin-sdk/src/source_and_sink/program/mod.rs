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

//! One shared Source-and-sink business object and its SDK-owned process lifetime.

#![expect(
    clippy::expect_used,
    reason = "Program owns its business object and joined sessions"
)]

use super::TenonSourceAndSink;
use crate::process::{
    ControlConnection, Event, FailureBoundary, Lifecycle, Publish, fatal, install_panic_hook,
    resolve, startup,
};
use crate::sink::session::{BatchWriter, Queues, Session as SinkSession};
use crate::source::session::Session as SourceSession;
use crate::{Error, FlowChannel, Ingress, Value};
use prost::Message;
use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::{Arc, mpsc};

/// Owns one shared business object and both queue sessions.
pub struct SourceAndSinkProgram<P, B: TenonSourceAndSink<P>> {
    business: Arc<SharedWriter<B>>,
    source: Option<SourceSession>,
    sink: Option<SinkSession>,
    control: ControlConnection,
    received: mpsc::Receiver<Event>,
    config: Value,
    finished: bool,
    payload: PhantomData<fn(P)>,
}

impl<P: Message + Default, B: TenonSourceAndSink<P>> SourceAndSinkProgram<P, B> {
    /// Starts one owner with the directions bound to this Instance.
    pub fn run<S: Message>(
        factory: impl FnOnce(Value, Option<Ingress<S>>, BTreeSet<FlowChannel>) -> Result<B, Error>,
    ) -> Self {
        install_panic_hook();
        let startup = {
            let mut input = std::io::stdin().lock();
            resolve(startup::read(std::env::args_os().skip(1), &mut input))
        };
        let source_path = startup.bells.source_channel_region;
        let sink_inputs = startup
            .bells
            .sink_inputs
            .filter(|inputs| !inputs.is_empty());
        if source_path.is_none() && sink_inputs.is_none() {
            fatal(&"Source-and-sink Program has no bound direction");
        }
        let channels = sink_inputs
            .as_ref()
            .map(|inputs| inputs.iter().map(|input| input.channel.clone()).collect())
            .unwrap_or_default();
        let (events, received) = mpsc::channel();
        let failed: FailureBoundary = Arc::new(|error| fatal(error.as_ref()));
        let lifecycle = if source_path.is_some() {
            Lifecycle::SourceCapable
        } else {
            Lifecycle::SinkOnly
        };
        let control = resolve(ControlConnection::start(
            startup.control_socket,
            startup.launch_id,
            lifecycle,
            events,
            failed.clone(),
        ));
        let source = source_path.map(|path| {
            resolve(SourceSession::open(
                &startup.working_directory.join("source"),
                &path,
                failed.clone(),
            ))
        });
        let ingress = source.as_ref().map(|source| Ingress {
            parallelism: source.parallelism(),
            sender: source.sender(),
        });
        let queues =
            sink_inputs.map(|inputs| resolve(Queues::open(&startup.working_directory, inputs)));
        let mut owner = resolve(factory(startup.config.clone(), ingress, channels));
        owner.start();
        let business = Arc::new(SharedWriter(owner));
        let sink = queues.map(|queues| {
            if let Some(source) = &source {
                resolve(SinkSession::start(
                    queues,
                    business.clone(),
                    source.failure_handler(),
                ))
            } else {
                resolve(SinkSession::start(queues, business.clone(), move |error| {
                    fatal(error.as_ref())
                }))
            }
        });
        Self {
            business,
            source,
            sink,
            control,
            received,
            config: startup.config,
            finished: false,
            payload: PhantomData,
        }
    }

    /// Returns the validated business configuration.
    pub fn config(&self) -> &Value {
        &self.config
    }

    /// Publishes Ready and owns Source quiesce and final shutdown.
    pub fn await_shutdown(mut self) {
        if let Some(source) = &self.source {
            resolve(source.check_running());
        }
        self.control.publish(Publish::Ready);
        if let Some(sink) = &self.sink {
            sink.activate();
        }
        while let Event::Quiesce = self.received.recv().expect("control owns lifecycle events") {
            let source = self
                .source
                .as_mut()
                .expect("control permits quiesce only with Source bound");
            source.stop_accepting();
            self.business.0.quiesce();
            resolve(source.quiesce());
            self.control.publish(Publish::Quiesced);
        }
        if let Some(source) = &mut self.source {
            resolve(source.close());
        }
        if let Some(sink) = &mut self.sink {
            resolve(sink.close());
        }
        Arc::get_mut(&mut self.business)
            .expect("all Sink workers joined")
            .0
            .close();
        self.control.finish();
        self.finished = true;
    }
}

impl<P, B: TenonSourceAndSink<P>> fmt::Debug for SourceAndSinkProgram<P, B> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output
            .debug_struct("SourceAndSinkProgram")
            .finish_non_exhaustive()
    }
}

impl<P, B: TenonSourceAndSink<P>> Drop for SourceAndSinkProgram<P, B> {
    fn drop(&mut self) {
        if !self.finished {
            fatal(&"Source-and-sink Program was dropped before shutdown completed");
        }
    }
}

struct SharedWriter<B>(B);
impl<P, B: TenonSourceAndSink<P>> BatchWriter<P> for SharedWriter<B> {
    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[P]>,
    ) -> impl Future<Output = Result<(), Error>> {
        self.0.write(channel, records)
    }
}
