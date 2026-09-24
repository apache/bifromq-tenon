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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import tools.jackson.core.JacksonException;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Parses the language-neutral startup bytes shared by every Java Plugin Program. */
final class PluginProgramProtocol {
  private static final int LAUNCH_ID_LENGTH = 16;

  /** The arguments the Pipeline appends after every Program argument and extraArgs entry. */
  private static final int RESERVED_ARGUMENTS = 2;

  private static final String SDK_CONFIG_OPTION = "--sdk-config";

  private static final JsonMapper JSON =
      JsonMapper.builder(
              JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .build();

  private PluginProgramProtocol() {}

  static Startup readStartup(String[] arguments, InputStream input) throws IOException {
    // The Pipeline appends its reserved option after every Program argument, so a Program that
    // declares extraArgs still starts. Everything before the block belongs to the Program.
    var reserved = arguments.length - RESERVED_ARGUMENTS;
    if (reserved < 0 || !arguments[reserved].equals(SDK_CONFIG_OPTION)) {
      throw new PluginProgramStartupException(
          "plugin.startup.arguments_invalid", "Expected the reserved startup option");
    }
    var document = readSdkConfig(arguments[reserved + 1]);
    return new Startup(
        absolutePath(
            document.workingDirectory(),
            "plugin.startup.working_directory_invalid",
            "Working directory must be an absolute path"),
        absolutePath(
            document.controlSocket(),
            "plugin.startup.control_socket_invalid",
            "Control socket must be an absolute path"),
        launchId(document.launchId()),
        bells(document),
        readJsonLine(input, "config"));
  }

  private static Path absolutePath(String value, String code, String message)
      throws PluginProgramStartupException {
    try {
      var path = Path.of(value);
      if (path.isAbsolute()) {
        return path;
      }
    } catch (InvalidPathException ignored) {
      // The stable contract error below owns every invalid path representation.
    }
    throw new PluginProgramStartupException(code, message);
  }

  private static byte[] launchId(String encoded) throws PluginProgramStartupException {
    try {
      var decoded = Base64.getDecoder().decode(encoded);
      if (decoded.length == LAUNCH_ID_LENGTH
          && Base64.getEncoder().encodeToString(decoded).equals(encoded)) {
        return decoded;
      }
    } catch (IllegalArgumentException ignored) {
      // The stable contract error below owns malformed Base64.
    }
    throw new PluginProgramStartupException(
        "plugin.startup.launch_id_invalid",
        "Launch id must be canonical padded Base64 for exactly 16 bytes");
  }

  /**
   * Reads the one document that names everything the Pipeline told this Instance.
   *
   * <p>The Instance directory, the lifecycle endpoint and the launch identity are always named. An
   * unknown key is rejected rather than ignored, so a misspelled direction fails loudly instead of
   * idling.
   */
  private static SdkConfigValue readSdkConfig(String argument)
      throws PluginProgramStartupException {
    final SdkConfigValue document;
    try {
      document = JSON.convertValue(JSON.readTree(argument), SdkConfigValue.class);
    } catch (JacksonException error) {
      throw new PluginProgramStartupException(
          "plugin.startup.sdk_config_invalid", "Startup document must be one JSON object", error);
    }
    if (document.workingDirectory() == null
        || document.controlSocket() == null
        || document.launchId() == null) {
      throw new PluginProgramStartupException(
          "plugin.startup.sdk_config_invalid",
          "Startup document must name the Instance directory, the control socket and the launch id");
    }
    return document;
  }

  /**
   * Reads the Channel doorbell Regions one Instance rings from its startup document.
   *
   * <p>A direction is absent exactly when the Interface does not include it; a direction this
   * Interface does implement but was not told about is a startup failure rather than a fallback.
   */
  private static Bells bells(SdkConfigValue document) throws PluginProgramStartupException {
    Path sourceChannelRegion = null;
    if (document.sourceChannelRegion() != null) {
      sourceChannelRegion =
          absolutePath(
              document.sourceChannelRegion(),
              "plugin.startup.channel_bell_path_invalid",
              "Channel doorbell Region must be an absolute path");
    }
    var inputs = document.sinkInputs();
    List<SinkInput> sinkInputs = null;
    if (inputs != null) {
      var converted = new ArrayList<SinkInput>(inputs.size());
      for (var input : inputs) {
        if (input == null
            || input.flowId() == null
            || input.channelId() == null
            || input.channelBellPath() == null
            || input.channelId() < 0) {
          throw new PluginProgramStartupException(
              "plugin.startup.sdk_config_invalid",
              "Every Sink input must name its Flow, its Channel index and its doorbell Region");
        }
        if (!input.channelBellPath().isAbsolute()) {
          throw new PluginProgramStartupException(
              "plugin.startup.sink_channels_invalid",
              "Every Sink input must name an absolute Channel doorbell Region");
        }
        converted.add(
            new SinkInput(
                new FlowChannel(input.flowId(), input.channelId()), input.channelBellPath()));
      }
      sinkInputs = List.copyOf(converted);
    }
    return new Bells(sourceChannelRegion, sinkInputs);
  }

  /**
   * Returns the Channel doorbell Region an Interface that Sources a Flow must ring.
   *
   * <p>A Source can neither ring the Channels that read its Submissions nor publish the ordinals
   * those Channels ring in return, so the Pipeline always appends this option for those Interfaces.
   * Its absence is a startup failure rather than a fallback.
   */
  static Path requireChannelBellPath(Startup startup) throws PluginProgramStartupException {
    var path = startup.bells().sourceChannelRegion();
    if (path == null) {
      throw new PluginProgramStartupException(
          "plugin.startup.channel_bell_path_invalid",
          "A Source must be told the Channel doorbell Region it rings");
    }
    return path;
  }

  /**
   * Returns every Sink input an Interface that consumes Flows must be told about.
   *
   * <p>A Sink takes each Egress Queue from a list element and picks its own-loop Bell slot itself,
   * so the Pipeline always appends the list for those Interfaces. Its absence is a startup failure
   * rather than an idle Sink.
   */
  static List<SinkInput> requireSinkInputs(Startup startup) throws PluginProgramStartupException {
    var inputs = startup.bells().sinkInputs();
    if (inputs == null) {
      throw new PluginProgramStartupException(
          "plugin.startup.sink_channels_invalid", "A Sink must be told every input it releases");
    }
    return inputs;
  }

  private static JsonNode readJsonLine(InputStream input, String kind) throws IOException {
    var bytes = new ByteArrayOutputStream();
    while (true) {
      var value = input.read();
      if (value == -1) {
        throw invalidInput(kind, "Startup input ended before LF", null);
      }
      if (value == '\n') {
        break;
      }
      if (value == '\r') {
        throw invalidInput(kind, "Startup input must not contain CR", null);
      }
      bytes.write(value);
    }
    if (bytes.size() == 0) {
      throw invalidInput(kind, "Startup input is empty", null);
    }
    final String json;
    try {
      json = decodeUtf8(bytes.toByteArray());
    } catch (CharacterCodingException error) {
      throw invalidInput(kind, "Startup input is not valid UTF-8", error);
    }
    try (var parser = JSON.createParser(json)) {
      var config = JSON.readTree(parser);
      if (config == null || parser.nextToken() != null) {
        throw invalidInput(kind, "Startup input must contain exactly one JSON value", null);
      }
      return config;
    } catch (PluginProgramStartupException error) {
      throw error;
    } catch (JacksonException error) {
      throw invalidInput(kind, "Startup input is not valid JSON", error);
    }
  }

  private static PluginProgramStartupException invalidInput(
      String kind, String message, Throwable cause) {
    return new PluginProgramStartupException("plugin.startup." + kind + "_invalid", message, cause);
  }

  private static String decodeUtf8(byte[] bytes) throws CharacterCodingException {
    return StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString();
  }

  record Startup(
      Path workingDirectory, Path controlSocket, byte[] launchId, Bells bells, JsonNode config) {
    Startup {
      launchId = launchId.clone();
    }

    @Override
    public byte[] launchId() {
      return launchId.clone();
    }
  }

  /** The Channel doorbell Regions one Instance rings, as its startup document carries them. */
  record Bells(Path sourceChannelRegion, List<SinkInput> sinkInputs) {}

  /** Absent directions stay null; an unknown key is malformed startup rather than an extension. */
  private record SdkConfigValue(
      String workingDirectory,
      String controlSocket,
      String launchId,
      String sourceChannelRegion,
      List<SinkInputValue> sinkInputs) {}

  /** One Sink input as the document carries it, before this reader validates and copies it. */
  private record SinkInputValue(String flowId, Integer channelId, Path channelBellPath) {}
}
