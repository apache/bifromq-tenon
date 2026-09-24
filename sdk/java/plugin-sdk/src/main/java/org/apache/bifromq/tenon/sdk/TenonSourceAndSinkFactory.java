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

package org.apache.bifromq.tenon.sdk;

import com.google.protobuf.MessageLite;
import java.util.Optional;
import java.util.Set;
import tools.jackson.databind.JsonNode;

/** Creates the single shared business owner of one source-and-sink Plugin Program process. */
public interface TenonSourceAndSinkFactory<S extends MessageLite, T extends MessageLite> {
  /**
   * Creates one shared owner from the validated process configuration and Source transport.
   *
   * <p>The SDK invokes this method exactly once per process, then calls {@link
   * TenonSourceAndSink#start()} on the returned owner. The owner receives Sink batches through
   * {@link TenonSourceAndSink#write(FlowChannel, java.util.List)}.
   *
   * @param config the validated process configuration
   * @param source the bound Source transport, or empty when no Flow uses this Instance as Source
   * @param egressChannels the actual Sink input identities; empty when Sink is not bound
   * @return the shared business owner that the SDK starts
   * @throws Exception when business construction fails
   */
  TenonSourceAndSink<T> create(
      JsonNode config, Optional<Ingress<S>> source, Set<FlowChannel> egressChannels)
      throws Exception;
}
