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

use serde::Deserialize;
use std::error::Error;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    pub endpoint: String,
    pub client_id_prefix: String,
    /// How many OS threads drive every MQTT client this instance owns.
    ///
    /// Clients are pinned to a thread by index, so the real thread count is
    /// The runtime uses `min(eventLoopThreads, client_count)` workers, where
    /// `client_count` covers the bound Source channels and Sink channel IDs.
    /// A larger value never creates another thread.
    #[serde(default = "default_event_loop_threads")]
    pub event_loop_threads: usize,
    #[serde(default)]
    pub auth: Option<Auth>,
    #[serde(default)]
    pub session: SessionConfig,
    #[serde(default)]
    pub source: Option<SourceConfig>,
    #[serde(default)]
    pub sink: SinkConfig,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    pub username: String,
    #[serde(default)]
    pub password: String,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SessionConfig {
    #[serde(default)]
    pub clean_start: bool,
    #[serde(default = "default_session_expiry")]
    pub expiry_interval_seconds: u32,
}
impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            clean_start: false,
            expiry_interval_seconds: default_session_expiry(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SourceConfig {
    pub subscriptions: Vec<Subscription>,
    /// Records this Channel may hold unacknowledged before the broker must wait.
    #[serde(default = "default_pending_messages")]
    pub max_pending_messages: usize,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub filter: String,
    #[serde(default = "default_qos")]
    pub qos: u8,
}
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SinkConfig {
    #[serde(default = "default_qos")]
    pub default_qos: u8,
    #[serde(default)]
    pub default_retain: bool,
}
fn default_qos() -> u8 {
    1
}
fn default_session_expiry() -> u32 {
    86_400
}
fn default_pending_messages() -> usize {
    32
}
fn default_event_loop_threads() -> usize {
    1
}
impl Config {
    /// The MQTT 5 receive window this Plugin offers the broker on one Channel.
    ///
    /// `maxPendingMessages` is how many QoS>0 Publish packets one connection
    /// holds unacknowledged, so it is exactly the window the broker must stay
    /// inside. `parse` keeps it within the protocol range of a `u16`.
    pub(crate) fn receive_maximum(&self) -> u16 {
        self.source
            .as_ref()
            .map_or_else(default_pending_messages, |source| {
                source.max_pending_messages
            }) as u16
    }

    pub fn parse(value: &serde_json::Value) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let c: Self = serde_json::from_value(value.clone())?;
        let u = Url::parse(&c.endpoint)?;
        if !matches!(u.scheme(), "mqtt" | "mqtts")
            || u.host_str().is_none()
            || !u.username().is_empty()
            || u.password().is_some()
            || !(u.path().is_empty() || u.path() == "/")
            || u.query().is_some()
            || u.fragment().is_some()
        {
            return Err("mqtt endpoint must be mqtt:// or mqtts:// host URL".into());
        }
        if c.client_id_prefix.is_empty() {
            return Err("clientIdPrefix must not be empty".into());
        }
        if c.event_loop_threads == 0 {
            return Err("eventLoopThreads must be at least 1".into());
        }
        let mut filters = std::collections::BTreeSet::new();
        for s in c.source.iter().flat_map(|s| &s.subscriptions) {
            if s.qos > 2 || !rumqttc::valid_filter(&s.filter) {
                return Err("invalid source subscription".into());
            }
            if !filters.insert(&s.filter) {
                return Err("duplicate source subscription filter".into());
            }
        }
        if c.sink.default_qos > 2 {
            return Err("sink defaultQos must be 0, 1, or 2".into());
        }
        if let Some(source) = &c.source
            && !(1..=65_535).contains(&source.max_pending_messages)
        {
            return Err("source maxPendingMessages must be between 1 and 65535".into());
        }
        Ok(c)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_and_rejects_invalid_qos() {
        let value = serde_json::json!({"endpoint":"mqtt://localhost:1883","clientIdPrefix":"site-","source":{"subscriptions":[{"filter":"a/#"}]}});
        let config = Config::parse(&value).expect("valid config");
        assert_eq!(config.client_id_prefix, "site-");
        assert_eq!(config.event_loop_threads, 1);
        let qos_two = serde_json::json!({
            "endpoint": "mqtt://localhost:1883",
            "clientIdPrefix": "site-",
            "source": {"subscriptions": [{"filter": "a/#", "qos": 2}]}
        });
        assert!(Config::parse(&qos_two).is_ok());
        let invalid = serde_json::json!({"endpoint":"mqtt://localhost:1883","clientIdPrefix":"site-","sink":{"defaultQos":3}});
        assert!(Config::parse(&invalid).is_err());
        let invalid_pending = serde_json::json!({"endpoint":"mqtt://localhost","clientIdPrefix":"site-","source":{"subscriptions":[{"filter":"a/#"}],"maxPendingMessages":0}});
        assert!(Config::parse(&invalid_pending).is_err());
    }

    #[test]
    fn event_loop_threads_must_be_a_positive_integer() {
        let threads = |value: serde_json::Value| {
            Config::parse(&serde_json::json!({
                "endpoint": "mqtt://localhost",
                "clientIdPrefix": "site-",
                "eventLoopThreads": value,
            }))
        };
        assert_eq!(
            threads(serde_json::json!(4))
                .expect("valid config")
                .event_loop_threads,
            4
        );
        assert!(threads(serde_json::json!(0)).is_err());
        assert!(threads(serde_json::json!(1.5)).is_err());
        assert!(threads(serde_json::json!(-1)).is_err());
    }

    #[test]
    fn rejects_endpoint_userinfo_and_duplicate_filters() {
        let userinfo = serde_json::json!({
            "endpoint": "mqtt://user:pass@localhost",
            "clientIdPrefix": "site-"
        });
        assert!(Config::parse(&userinfo).is_err());
        let duplicate = serde_json::json!({
            "endpoint": "mqtt://localhost",
            "clientIdPrefix": "site-",
            "source": {"subscriptions": [{"filter": "a/#"}, {"filter": "a/#"}]}
        });
        assert!(Config::parse(&duplicate).is_err());
        let invalid_filter = serde_json::json!({
            "endpoint": "mqtt://localhost",
            "clientIdPrefix": "site-",
            "source": {"subscriptions": [{"filter": "input/#/invalid"}]}
        });
        assert!(Config::parse(&invalid_filter).is_err());
    }
}
