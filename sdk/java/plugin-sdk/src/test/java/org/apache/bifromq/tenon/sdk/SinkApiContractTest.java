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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

final class SinkApiContractTest {
  @Test
  void publicSinkAndFactoryApisStayMinimal() throws Exception {
    var methods =
        Arrays.stream(TenonSink.class.getDeclaredMethods())
            .map(method -> method.getName())
            .toList();
    assertEquals(List.of("close", "start", "write"), methods.stream().sorted().toList());

    var start = TenonSink.class.getMethod("start");
    assertEquals(void.class, start.getReturnType());
    assertArrayEquals(new Class<?>[0], start.getExceptionTypes());

    var channel = Class.forName("org.apache.bifromq.tenon.sdk.FlowChannel");
    assertEquals(true, channel.isRecord());
    assertEquals(
        List.of("flowId", "channelId"),
        Arrays.stream(channel.getRecordComponents())
            .map(component -> component.getName())
            .toList());
    var write = TenonSink.class.getMethod("write", channel, List.class);
    assertEquals(CompletionStage.class, write.getReturnType());
    assertArrayEquals(new Class<?>[0], write.getExceptionTypes());

    var close = TenonSink.class.getMethod("close");
    assertEquals(void.class, close.getReturnType());
    assertArrayEquals(new Class<?>[0], close.getExceptionTypes());

    var factoryMethods = TenonSinkFactory.class.getDeclaredMethods();
    assertEquals(1, factoryMethods.length);
    assertEquals("create", factoryMethods[0].getName());
    assertEquals(TenonSink.class, factoryMethods[0].getReturnType());
    assertArrayEquals(new Class<?>[] {JsonNode.class}, factoryMethods[0].getParameterTypes());
    assertArrayEquals(new Class<?>[] {Exception.class}, factoryMethods[0].getExceptionTypes());
  }

  @Test
  void programApiExposesOnlyTheCompleteRunOperation() throws Exception {
    var methods =
        Arrays.stream(SinkProgram.class.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .toList();

    assertEquals(3, methods.size());
    assertEquals(
        Set.of("run", "config", "awaitShutdown"),
        methods.stream().map(method -> method.getName()).collect(Collectors.toUnmodifiableSet()));
    assertFalse(
        Arrays.stream(SinkProgram.class.getDeclaredConstructors())
            .anyMatch(constructor -> Modifier.isPublic(constructor.getModifiers())));
    assertFalse(Modifier.isPublic(SinkCoordinator.class.getModifiers()));
  }
}
