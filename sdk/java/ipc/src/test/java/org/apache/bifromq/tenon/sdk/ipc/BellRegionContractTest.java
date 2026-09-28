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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;

final class BellRegionContractTest {
  private static final Path TEST_VECTORS =
      Path.of(System.getProperty("tenon.contracts.ipc.directory"), "bell_v1_test_vectors.json");

  @TempDir Path directory;

  @Test
  void sharedVectorsFixCreationBytesAndArmedReopen() throws Exception {
    var vectors = vectors();
    assertEquals(vectors.formatVersion(), BellRegion.FORMAT_VERSION);
    assertEquals(vectors.headerLength(), BellRegion.HEADER_LENGTH);
    assertEquals(vectors.slotLength(), BellRegion.SLOT_LENGTH);
    assertEquals(vectors.pageLength(), BellRegion.PAGE_LENGTH);
    assertEquals(vectors.armedValue(), BellRegion.ARMED_VALUE);
    assertEquals(vectors.notifiedValue(), BellRegion.NOTIFIED_VALUE);
    for (var index = 0; index < vectors.valid().size(); index++) {
      var vector = vectors.valid().get(index);
      var path = directory.resolve("valid-" + index);
      var expected = HexFormat.of().parseHex(vector.encodedHex());
      BellRegion.create(path, vector.slotCount(), Long.parseUnsignedLong(vector.epoch()));
      assertArrayEquals(expected, Files.readAllBytes(path), vector.name());
      try (var region = BellRegion.open(path)) {
        assertEquals(vector.slotCount(), region.slotCount(), vector.name());
        region.slot(0).arm();
        ByteBuffer.wrap(expected)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(vectors.headerLength(), vectors.armedValue());
        assertArrayEquals(expected, Files.readAllBytes(path), vector.name());
      }
      try (var reopened = BellRegion.open(path)) {
        assertEquals(vector.slotCount(), reopened.slotCount(), vector.name());
      }
    }
  }

  @Test
  void sharedInvalidVectorsFixErrorsWithoutChangingBytes() throws Exception {
    var vectors = vectors();
    for (var index = 0; index < vectors.invalid().size(); index++) {
      var vector = vectors.invalid().get(index);
      var path = directory.resolve("invalid-" + index);
      var encoded = HexFormat.of().parseHex(vector.encodedHex());
      Files.write(path, encoded);
      var error =
          assertThrows(BellRegionException.class, () -> BellRegion.open(path), vector.name());
      assertEquals(vector.expectedErrorCode(), error.code(), vector.name());
      assertArrayEquals(encoded, Files.readAllBytes(path), vector.name());
    }
  }

  private static TestVectors vectors() throws Exception {
    return new ObjectMapper().readValue(TEST_VECTORS.toFile(), TestVectors.class);
  }

  private record TestVectors(
      int formatVersion,
      int headerLength,
      int slotLength,
      int pageLength,
      int armedValue,
      int notifiedValue,
      List<ValidVector> valid,
      List<InvalidVector> invalid) {}

  private record ValidVector(String name, int slotCount, String epoch, String encodedHex) {}

  private record InvalidVector(String name, String encodedHex, String expectedErrorCode) {}
}
