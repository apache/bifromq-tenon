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

package org.apache.bifromq.tenon.sdk.ipc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import tools.jackson.databind.ObjectMapper;

final class IpcQueueFormatContractTest {
  private static final Path TEST_VECTORS =
      Path.of(System.getProperty("tenon.contracts.ipc.directory"), "queue_v1_test_vectors.json");

  @Test
  void sharedHeaderVectorsFixExactBytesAndErrors() throws Exception {
    var vectors = vectors();
    assertEquals(vectors.formatVersion(), IpcQueueFormat.FORMAT_VERSION);
    assertEquals(vectors.headerLength(), IpcQueueFormat.HEADER_LENGTH);
    assertEquals(vectors.frameHeaderLength(), IpcQueueFormat.FRAME_HEADER_LENGTH);
    assertEquals(vectors.frameAlignment(), IpcQueueFormat.FRAME_ALIGNMENT);
    assertEquals(
        0xFFFFFFFFL,
        vectors.unboundBellSlot(),
        "Unbound bell slot must be the unsigned all-ones value");
    assertEquals(IpcQueueFormat.UNBOUND_BELL_SLOT, (int) vectors.unboundBellSlot());

    for (var vector : vectors.headers().valid()) {
      var capacity =
          IpcQueueFormat.DataCapacity.fromFileLength(parseSignedLong(vector.fileLength()));
      var header = header(vector, capacity);
      var encoded = bytes(vector.encodedHex());
      assertArrayEquals(encoded, IpcQueueFormat.encodeHeader(header), vector.name());
      assertEquals(header, IpcQueueFormat.decodeHeader(capacity, encoded), vector.name());
    }

    for (var vector : vectors.headers().invalidFileLengths()) {
      assertErrorCode(
          vector.expectedErrorCode(),
          () -> IpcQueueFormat.DataCapacity.fromFileLength(parseSignedLong(vector.fileLength())),
          vector.name());
    }

    for (var vector : vectors.headers().invalid()) {
      var capacity =
          IpcQueueFormat.DataCapacity.fromFileLength(parseSignedLong(vector.fileLength()));
      assertErrorCode(
          vector.expectedErrorCode(),
          () -> IpcQueueFormat.decodeHeader(capacity, bytes(vector.encodedHex())),
          vector.name());
    }
  }

  @Test
  void sharedFrameVectorsFixRecordWrapAndErrorBytes() throws Exception {
    var vectors = vectors();
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    var maxPayloadSize = parseSignedLong(vectors.frames().maxPayloadSize());

    for (var vector : vectors.frames().validRecords()) {
      var record = bytes(vector.recordHex());
      var encoded = bytes(vector.encodedHex());
      assertArrayEquals(
          encoded,
          IpcQueueFormat.encodeRecordFrame(capacity, maxPayloadSize, record),
          vector.name());
      var frame = IpcQueueFormat.decodeOwnedFrame(capacity, maxPayloadSize, encoded);
      var recordFrame = assertInstanceOf(IpcQueueFormat.RecordFrame.class, frame, vector.name());
      assertArrayEquals(record, recordFrame.recordBytes().toByteArray(), vector.name());
    }

    var wrap = bytes(vectors.frames().wrap().encodedHex());
    assertArrayEquals(wrap, IpcQueueFormat.encodeWrapFrame(), vectors.frames().wrap().name());
    assertEquals(
        IpcQueueFormat.WrapFrame.INSTANCE,
        IpcQueueFormat.decodeOwnedFrame(capacity, maxPayloadSize, wrap),
        vectors.frames().wrap().name());

    for (var vector : vectors.frames().invalid()) {
      assertErrorCode(
          vector.expectedErrorCode(),
          () ->
              IpcQueueFormat.decodeOwnedFrame(capacity, maxPayloadSize, bytes(vector.encodedHex())),
          vector.name());
    }
  }

  @Test
  void sharedAppendVectorsFixWrapFullAndExhaustion() throws Exception {
    for (var vector : vectors().append()) {
      var capacity = IpcQueueFormat.DataCapacity.of(parseSignedLong(vector.dataCapacity()));
      var maxPayloadSize = parseSignedLong(vector.maxPayloadSize());
      switch (vector.result()) {
        case "ready" -> {
          var plan =
              assertInstanceOf(
                  IpcQueueFormat.AppendPlan.class,
                  IpcQueueFormat.planAppend(
                      capacity,
                      maxPayloadSize,
                      parseUnsignedLong(vector.commit()),
                      parseUnsignedLong(vector.release()),
                      vector.recordLength()),
                  vector.name());
          assertEquals(parseUnsignedLong(vector.writeOffset()), plan.writeOffset(), vector.name());
          assertEquals(parseUnsignedLong(vector.frameOffset()), plan.frameOffset(), vector.name());
          assertEquals(vector.frameLength().intValue(), plan.frameLength(), vector.name());
          assertEquals(parseUnsignedLong(vector.wrapLength()), plan.wrapLength(), vector.name());
          assertEquals(parseUnsignedLong(vector.nextCommit()), plan.nextCommit(), vector.name());
        }
        case "full" ->
            assertEquals(
                IpcQueueFormat.Full.INSTANCE,
                IpcQueueFormat.planAppend(
                    capacity,
                    maxPayloadSize,
                    parseUnsignedLong(vector.commit()),
                    parseUnsignedLong(vector.release()),
                    vector.recordLength()),
                vector.name());
        case "error" ->
            assertErrorCode(
                vector.expectedErrorCode(),
                () ->
                    IpcQueueFormat.planAppend(
                        capacity,
                        maxPayloadSize,
                        parseUnsignedLong(vector.commit()),
                        parseUnsignedLong(vector.release()),
                        vector.recordLength()),
                vector.name());
        default -> throw new AssertionError("Unknown append result: " + vector.result());
      }
    }
  }

  @Test
  void physicalOffsetsMatchUnsignedRemainderForEveryCapacityShape() throws Exception {
    var positions = new long[] {0, 8, 64, Long.MAX_VALUE - 7, Long.MIN_VALUE, -8};
    for (var capacityBytes : new long[] {64, 72}) {
      var capacity = IpcQueueFormat.DataCapacity.of(capacityBytes);
      for (var position : positions) {
        assertEquals(
            Long.remainderUnsigned(position, capacityBytes),
            IpcQueueFormat.physicalOffset(capacity, position));
      }
    }
  }

  @Test
  void queueLimitsAndReaderOwnershipCoverBoundaryBehavior() throws Exception {
    var smallCapacity = IpcQueueFormat.DataCapacity.fromFileLength(4288);
    var maxPayloadSize = 4088;
    var largestSmallRecord = new byte[4088];
    assertEquals(
        4096,
        IpcQueueFormat.encodeRecordFrame(smallCapacity, maxPayloadSize, largestSmallRecord).length);
    assertErrorCode(
        "ipc.queue.record_too_large",
        () -> IpcQueueFormat.encodeRecordFrame(smallCapacity, maxPayloadSize, new byte[4089]),
        "Record larger than Queue capacity");
    assertErrorCode(
        "ipc.queue.record_empty",
        () -> IpcQueueFormat.encodeRecordFrame(smallCapacity, maxPayloadSize, new byte[0]),
        "Empty record body");

    var source =
        IpcQueueFormat.encodeRecordFrame(smallCapacity, maxPayloadSize, new byte[] {1, 2, 3});
    var recordFrame =
        assertInstanceOf(
            IpcQueueFormat.RecordFrame.class,
            IpcQueueFormat.decodeOwnedFrame(smallCapacity, maxPayloadSize, source));
    var firstRead = recordFrame.recordBytes();
    var secondRead = recordFrame.recordBytes();
    assertSame(firstRead, secondRead);
    assertArrayEquals(new byte[] {1, 2, 3}, secondRead.toByteArray());
  }

  @Test
  void configuredRecordAboveSixteenMebibytesRoundTrips() throws Exception {
    var maximum = 17 * 1024 * 1024;
    var capacity = IpcQueueFormat.dataCapacityForRecordLimit(1, maximum);
    var record = new byte[maximum];
    java.util.Arrays.fill(record, (byte) 42);
    var encoded = IpcQueueFormat.encodeRecordFrame(capacity, maximum, record);
    assertEquals(maximum + 8, encoded.length);
    var decoded =
        assertInstanceOf(
            IpcQueueFormat.RecordFrame.class,
            IpcQueueFormat.decodeOwnedFrame(capacity, maximum, encoded));
    assertArrayEquals(record, decoded.recordBytes().toByteArray());
  }

  @Test
  void genericRecordLimitSizingRoundTripsWithoutQueueRoleKnowledge() throws Exception {
    var capacity = IpcQueueFormat.dataCapacityForRecordLimit(3, 1016);

    assertEquals(4096, capacity.bytes());
    assertEquals(3, IpcQueueFormat.recordLimitFromDataCapacity(capacity, 1016));
    assertErrorCode(
        "ipc.queue.capacity_invalid",
        () ->
            IpcQueueFormat.recordLimitFromDataCapacity(IpcQueueFormat.DataCapacity.of(4088), 1016),
        "Capacity must contain an exact frame count and one wrap reserve");
  }

  private static TestVectors vectors() throws Exception {
    return new ObjectMapper().readValue(TEST_VECTORS.toFile(), TestVectors.class);
  }

  private static IpcQueueFormat.Header header(
      ValidHeaderVector vector, IpcQueueFormat.DataCapacity capacity) throws Exception {
    return IpcQueueFormat.Header.of(
        parseSignedLong(vector.maxPayloadSize()),
        parseUnsignedLong(vector.commit()),
        (int) vector.readerBellSlot(),
        parseUnsignedLong(vector.release()),
        (int) vector.writerBellSlot(),
        capacity);
  }

  private static void assertErrorCode(String expected, Executable operation, String name) {
    var error = assertThrows(IpcQueueFormatException.class, operation, name);
    assertEquals(expected, error.code(), name);
  }

  private static long parseSignedLong(String value) {
    return Long.parseLong(value);
  }

  private static long parseUnsignedLong(String value) {
    return Long.parseUnsignedLong(value);
  }

  private static byte[] bytes(String value) {
    return HexFormat.of().parseHex(value);
  }

  private record TestVectors(
      int formatVersion,
      int headerLength,
      int frameHeaderLength,
      int frameAlignment,
      long unboundBellSlot,
      HeaderVectors headers,
      FrameVectors frames,
      List<AppendVector> append) {}

  private record HeaderVectors(
      List<ValidHeaderVector> valid,
      List<InvalidFileLengthVector> invalidFileLengths,
      List<InvalidHeaderVector> invalid) {}

  private record ValidHeaderVector(
      String name,
      String fileLength,
      String maxPayloadSize,
      String commit,
      long readerBellSlot,
      String release,
      long writerBellSlot,
      String encodedHex) {}

  private record InvalidFileLengthVector(
      String name, String fileLength, String expectedErrorCode) {}

  private record InvalidHeaderVector(
      String name, String fileLength, String encodedHex, String expectedErrorCode) {}

  private record FrameVectors(
      String maxPayloadSize,
      List<ValidRecordVector> validRecords,
      WrapVector wrap,
      List<InvalidFrameVector> invalid) {}

  private record ValidRecordVector(String name, String recordHex, String encodedHex) {}

  private record WrapVector(String name, String encodedHex) {}

  private record InvalidFrameVector(String name, String encodedHex, String expectedErrorCode) {}

  private record AppendVector(
      String name,
      String dataCapacity,
      String maxPayloadSize,
      String commit,
      String release,
      int recordLength,
      String result,
      String writeOffset,
      String frameOffset,
      Integer frameLength,
      String wrapLength,
      String nextCommit,
      String expectedErrorCode) {}
}
