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

//! One external resource shared by real Source producers and Sink callbacks.

#![expect(
    clippy::panic,
    reason = "These subprocess scenarios deliberately inject fatal lifecycle failures"
)]
#![expect(
    clippy::expect_used,
    reason = "Fixture logging must remain available until all callbacks end"
)]

mod business_panic;

use business_panic::panic_in_callback;
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use tenon_plugin_sdk::{
    Completion, Error, FlowChannel, Ingress, PayloadSender, SourceAndSinkProgram, TenonSource,
    TenonSourceAndSink, Value,
};

struct Connection(File);

impl Connection {
    fn event(&self, event: &str) -> Result<(), Error> {
        writeln!(&self.0, "{event}")?;
        println!("{event}");
        Ok(())
    }
}

struct Bridge {
    connection: Arc<Connection>,
    config: Value,
    sender: Option<PayloadSender<Vec<u8>>>,
    source: Mutex<Option<Producer>>,
    starting: Option<Completion>,
}

impl TenonSourceAndSink<String> for Bridge {
    fn start(&mut self) {
        self.connection
            .event("shared-start")
            .expect("event log failed");
        match self.config["mode"].as_str() {
            Some("failed-shared-start") => {
                self.starting = Some(
                    self.sender
                        .as_ref()
                        .expect("Source is bound")
                        .send(0, &vec![])
                        .expect("Source send failed"),
                );
                panic!("Shared start failed")
            }
            Some("panic-shared-start") => panic_in_callback(),
            _ if self.config["sourceEnabled"].as_bool().unwrap_or(true) => {
                if let Some(source) = self.source.get_mut().expect("source mutex").as_mut() {
                    source.start();
                }
            }
            _ => {}
        }
    }

    fn quiesce(&self) {
        if !self.config["sourceEnabled"].as_bool().unwrap_or(true) {
            return;
        }
        if let Some(source) = self.source.lock().expect("source mutex").as_mut() {
            source.quiesce();
        }
    }

    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[String]>,
    ) -> impl Future<Output = Result<(), Error>> {
        let entered = self.connection.event(&format!(
            "write {} {} {records:?}",
            channel.flow_id, channel.channel_id
        ));
        let observer = Observer {
            connection: &self.connection,
            local: Rc::new(()),
        };
        async move {
            entered?;
            if self.config["mode"] == "panic-write" {
                panic_in_callback();
            }
            if self.config["mode"] == "pending-write" {
                std::future::pending::<()>().await;
            }
            let _observer = observer;
            if records.iter().any(|record| record == "fail") {
                Err("Shared write failed".into())
            } else {
                Ok(())
            }
        }
    }

    fn close(&mut self) {
        if let Some(starting) = self.starting.take() {
            assert!(starting.wait().is_err());
            self.connection
                .event("shared-result-ended")
                .expect("event log failed");
        }
        if let Some(mut source) = self
            .source
            .get_mut()
            .expect("source mutex is not poisoned")
            .take()
        {
            source.close();
        }
        self.connection
            .event("shared-close")
            .expect("event log failed");
        if self.config["failClose"] == true {
            panic!("Shared close failed");
        }
    }
}

struct Observer<'owner> {
    connection: &'owner Connection,
    local: Rc<()>,
}

impl Drop for Observer<'_> {
    fn drop(&mut self) {
        assert_eq!(Rc::strong_count(&self.local), 1);
        self.connection
            .event("observer-dropped")
            .expect("business connection remains open");
    }
}

struct Producer {
    connection: Arc<Connection>,
    config: Value,
    sender: PayloadSender<Vec<u8>>,
    results: Vec<JoinHandle<()>>,
}

impl TenonSource for Producer {
    fn start(&mut self) {
        self.connection
            .event("source-start")
            .expect("event log failed");
        if self.config["send"].as_bool().unwrap_or(true) {
            let length = self.config["payloadBytes"].as_u64().unwrap_or(8) as usize;
            let completion = self
                .sender
                .send(0, &vec![0; length])
                .expect("Source send failed");
            let connection = self.connection.clone();
            self.results.push(thread::spawn(move || {
                connection
                    .event(&format!("result 0 {:?}", completion.wait()))
                    .expect("shared connection remains until all Source results end");
            }));
        }
    }

    fn quiesce(&mut self) {
        self.connection
            .event("source-quiesce")
            .expect("event log failed");
        assert!(
            self.sender
                .send(0, &vec![])
                .expect("Source send failed")
                .wait()
                .expect_err("quiesced Source rejects sends")
                .is_session_closed()
        );
        self.connection
            .event("quiesced-send SessionClosed")
            .expect("record admission outcome");
        if self.config["mode"] == "held-quiesce" {
            let gate = Path::new(self.config["callbackGate"].as_str().expect("missing gate"));
            while !gate.exists() {
                thread::yield_now();
            }
        }
        if self.config["mode"] == "failed-quiesce" {
            panic!("Source quiesce failed");
        }
    }

    fn close(&mut self) {
        self.results.clear();
        if self.config["mode"] == "held-close" {
            self.connection
                .event("source-close-enter")
                .expect("event log failed");
            let gate = Path::new(self.config["callbackGate"].as_str().expect("missing gate"));
            while !gate.exists() {
                thread::yield_now();
            }
        }
        self.connection
            .event("source-close")
            .expect("event log failed");
        if self.config["failClose"] == true {
            panic!("Source close failed");
        }
    }
}

fn main() {
    let program = SourceAndSinkProgram::run(|config: Value, ingress, _channels| {
        let (parallelism, sender) = ingress
            .map(
                |Ingress {
                     parallelism,
                     sender,
                 }| (parallelism, Some(sender)),
            )
            .unwrap_or((0, None));
        println!("factory channels {parallelism}");
        if config["mode"] == "failed-factory" {
            return Err("Shared factory failed".into());
        }
        let connection = Arc::new(Connection(File::create_new(
            config["resource"].as_str().ok_or("missing resource path")?,
        )?));
        connection.event("opened")?;
        let source = sender.as_ref().map(|sender| {
            connection.event("create 0").expect("event log");
            Producer {
                connection: connection.clone(),
                config: config.clone(),
                sender: sender.clone(),
                results: Vec::new(),
            }
        });
        println!("sink channels {}", _channels.len());
        Ok(Bridge {
            connection: connection.clone(),
            config,
            sender,
            source: Mutex::new(source),
            starting: None,
        })
    });
    let mode = program.config()["mode"].as_str().unwrap_or("normal");
    if mode == "drop-program" {
        drop(program);
        return;
    }
    if mode == "held-ready" {
        println!("waiting-before-ready");
        loop {
            thread::park();
        }
    }
    program.await_shutdown();
}
