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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Test fixture for the Core-created Bell Region files used by SDK tests. */
public final class BellRegionTestSupport {
  private static final byte[] MAGIC = {'T', 'E', 'N', 'O', 'N', 'B', 'E', 'L'};

  private BellRegionTestSupport() {}

  public static void create(Path path, int slotCount, long epoch) throws IOException {
    if (slotCount == 0) {
      throw new IOException("Bell Region slot count must be non-zero");
    }
    var length = BellRegion.regionLength(slotCount);
    if (length > Integer.MAX_VALUE) {
      throw new IOException("Bell Region slot count has no contract length");
    }
    var bytes = new byte[(int) length];
    System.arraycopy(MAGIC, 0, bytes, 0, MAGIC.length);
    writeInt(bytes, 8, BellRegion.FORMAT_VERSION);
    writeInt(bytes, 12, slotCount);
    writeLong(bytes, 16, epoch);
    for (var index = 0; index < slotCount; index++) {
      writeInt(bytes, Math.toIntExact(BellRegion.slotOffset(index)), BellRegion.NOTIFIED_VALUE);
    }
    try (var channel =
        FileChannel.open(
            path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE)) {
      var buffer = ByteBuffer.wrap(bytes);
      while (buffer.hasRemaining()) {
        if (channel.write(buffer) <= 0) {
          throw new IOException("Bell Region file write made no progress");
        }
      }
    }
  }

  private static void writeInt(byte[] bytes, int offset, int value) {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, value);
  }

  private static void writeLong(byte[] bytes, int offset, long value) {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(offset, value);
  }
}
