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

import static org.apache.bifromq.tenon.sdk.PluginProgramTestVectors.arguments;
import static org.apache.bifromq.tenon.sdk.PluginProgramTestVectors.input;
import static org.apache.bifromq.tenon.sdk.PluginProgramTestVectors.load;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.Base64;
import org.junit.jupiter.api.Test;

final class PluginProgramProtocolContractTest {
  @Test
  void startupInputsMatchTheCurrentLanguageNeutralContract() throws Exception {
    var startup = load("process_protocol_test_vectors.json").required("startup");
    for (var vector : startup.required("valid")) {
      var stream = new ByteArrayInputStream(input(vector));
      var parsed = PluginProgramProtocol.readStartup(arguments(vector), stream);
      var document = vector.required("sdkConfig");
      var name = vector.required("name").stringValue();

      assertEquals(
          Path.of(document.required("workingDirectory").stringValue()),
          parsed.workingDirectory(),
          name);
      assertEquals(
          Path.of(document.required("controlSocket").stringValue()), parsed.controlSocket(), name);
      assertArrayEquals(
          Base64.getDecoder().decode(document.required("launchId").stringValue()),
          parsed.launchId(),
          name);
      assertEquals(vector.required("config"), parsed.config(), name);
      var expectedChannelBellPath =
          document.has("sourceChannelRegion")
              ? Path.of(document.required("sourceChannelRegion").stringValue())
              : null;
      assertEquals(expectedChannelBellPath, parsed.bells().sourceChannelRegion(), name);
      if (document.has("sinkInputs")) {
        var channels = PluginProgramProtocol.requireSinkInputs(parsed);
        var expected = document.required("sinkInputs");
        assertEquals(expected.size(), channels.size());
        for (int index = 0; index < channels.size(); index++) {
          var channel = channels.get(index);
          assertEquals(
              expected.get(index).required("flowId").stringValue(), channel.channel().flowId());
          assertEquals(
              expected.get(index).required("channelId").intValue(), channel.channel().channelId());
          assertEquals(
              Path.of(expected.get(index).required("channelBellPath").stringValue()),
              channel.channelBellPath());
          assertEquals(
              parsed
                  .workingDirectory()
                  .resolve(vector.required("relativeQueuePaths").get(index).stringValue()),
              EgressQueueLayout.queue(parsed.workingDirectory(), channel.channel()));
        }
      }
      assertEquals(-1, stream.read());
    }

    for (var vector : startup.required("invalid")) {
      var error =
          assertThrows(
              PluginProgramStartupException.class,
              () -> {
                var stream = new ByteArrayInputStream(input(vector));
                var parsed = PluginProgramProtocol.readStartup(arguments(vector), stream);
                var iface =
                    vector.has("interface") ? vector.required("interface").stringValue() : "";
                if (iface.equals("source") || iface.equals("source-and-sink")) {
                  PluginProgramProtocol.requireChannelBellPath(parsed);
                }
                if (iface.equals("sink") || iface.equals("source-and-sink")) {
                  PluginProgramProtocol.requireSinkInputs(parsed);
                }
              },
              vector.required("name").stringValue());
      assertEquals(
          vector.required("expectedErrorCode").stringValue(),
          error.code(),
          vector.required("name").stringValue());
    }
  }
}
