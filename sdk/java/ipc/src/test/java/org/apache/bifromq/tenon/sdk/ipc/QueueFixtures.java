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

/**
 * Plays the peer process that owns the Bell Region of a Queue test.
 *
 * <p>One test plays both ends, so the writer's loop parks in slot zero and the reader's loop in
 * slot one of the same Region, exactly as the Rust SDK's fixtures do. The observable half of the
 * fixture is public because a Source test in another package watches the same Queue file and Region
 * pair.
 */
public final class QueueFixtures {
  private static final int WRITER_SLOT = 0;
  private static final int READER_SLOT = 1;

  private QueueFixtures() {}

  /** Creates one Region file beside the Queue and opens it, as the Core runtime does. */
  public static BellRegion bellsFor(Path queue, int slots) throws IOException {
    var path = queue.resolveSibling(queue.getFileName() + ".bells");
    BellRegionTestSupport.create(path, slots, 0);
    return BellRegion.open(path);
  }

  /** Returns the Region path {@link #bellsFor} created for the same Queue. */
  public static Path bellsPathFor(Path queue) {
    return queue.resolveSibling(queue.getFileName() + ".bells");
  }

  public static IpcQueue.Writer writer(Path queue, BellRegion bells)
      throws IOException, IpcQueueFormatException {
    return IpcQueue.openWriter(queue, bells.loopBell(WRITER_SLOT), bells);
  }

  public static IpcQueue.Reader reader(Path queue, BellRegion bells)
      throws IOException, IpcQueueFormatException {
    return IpcQueue.openReader(queue, bells.loopBell(READER_SLOT), bells);
  }

  /** Reports whether the reader's loop has parked on its doorbell, as its peer observes it. */
  public static boolean readerIsArmed(Path queue, Path region) throws IOException {
    return slotIsArmed(queue, region, IpcQueueFormat.READER_BELL_SLOT_OFFSET);
  }

  /** Reports whether the writer's loop has parked on its doorbell, as its peer observes it. */
  public static boolean writerIsArmed(Path queue, Path region) throws IOException {
    return slotIsArmed(queue, region, IpcQueueFormat.WRITER_BELL_SLOT_OFFSET);
  }

  private static boolean slotIsArmed(Path queue, Path region, long offset) throws IOException {
    var slot = readInt(queue, offset);
    if (slot == IpcQueueFormat.UNBOUND_BELL_SLOT) {
      return false;
    }
    return readInt(region, BellRegion.slotOffset(slot)) == BellRegion.ARMED_VALUE;
  }

  static int readInt(Path path, long offset) throws IOException {
    try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
      var bytes = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      var position = offset;
      while (bytes.hasRemaining()) {
        var read = channel.read(bytes, position);
        if (read <= 0) {
          throw new IOException("Mapped test read made no progress");
        }
        position += read;
      }
      bytes.flip();
      return bytes.getInt();
    }
  }
}
