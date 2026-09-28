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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * The Bell Region that one Plugin side's own waiting loops park in.
 *
 * <p>A Bell Region is one page-aligned mapping of doorbell slots. A slot holds no record, position,
 * or business fact: it is one atomic word whose only values are zero (armed) and one (notified).
 * Exactly one waiting loop owns a slot, so a peer's ring wakes that loop and no other.
 *
 * <p>A Queue header never holds a doorbell word. It publishes the ordinal of the slot a waiting
 * loop parked in, and that ordinal only means something inside the Region that loop's own process
 * opened. Ringing a peer therefore means reading its ordinal from the header and ringing that slot
 * of the peer's Region. The format is the language-neutral contract's, so the Core runtime and this
 * SDK ring the same addresses.
 *
 * <p>One opener owns a Region and closes it after every endpoint and loop bell that parks in it or
 * rings it has been closed.
 */
public final class BellRegion implements AutoCloseable {
  /** The fixed file name of one Plugin side's own-loop Bell Region. */
  public static final String LOOPS_BELL_FILE_NAME = "loops.bells";

  static final int FORMAT_VERSION = 1;
  static final int HEADER_LENGTH = 64;
  static final int SLOT_LENGTH = 64;
  static final int PAGE_LENGTH = 4096;

  /** Doorbell slot value meaning "the owner is about to sleep on this slot". */
  static final int ARMED_VALUE = 0;

  /** Doorbell slot value meaning "some peer rang this slot". */
  static final int NOTIFIED_VALUE = 1;

  private static final byte[] MAGIC = {'T', 'E', 'N', 'O', 'N', 'B', 'E', 'L'};
  private static final int VERSION_OFFSET = 8;
  private static final int SLOT_COUNT_OFFSET = 12;
  private static final int EPOCH_OFFSET = 16;
  private static final int HEADER_PADDING_START = 24;
  private static final VarHandle INT =
      ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN).varHandle();

  private final Arena arena;
  private final MemorySegment mapping;
  private final int slotCount;
  private volatile boolean closed;

  private BellRegion(Arena arena, MemorySegment mapping, int slotCount) {
    this.arena = arena;
    this.mapping = mapping;
    this.slotCount = slotCount;
  }

  /**
   * Opens and validates the Bell Region at {@code path}.
   *
   * @throws BellRegionException when the file is not a well-formed Region, carrying {@code
   *     ipc.bell.region_too_short}, {@code ipc.bell.magic_invalid}, {@code
   *     ipc.bell.format_version_unsupported}, {@code ipc.bell.slot_count_invalid}, {@code
   *     ipc.bell.padding_nonzero}, or {@code ipc.bell.region_length_invalid}
   */
  public static BellRegion open(Path path) throws IOException {
    Objects.requireNonNull(path, "path");
    var arena = Arena.ofShared();
    try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      var fileLength = channel.size();
      if (fileLength < HEADER_LENGTH) {
        throw invalid("ipc.bell.region_too_short", "Bell Region is too short to hold its header");
      }
      var mapping = channel.map(FileChannel.MapMode.READ_WRITE, 0, fileLength, arena);
      var header = new byte[HEADER_LENGTH];
      copyOut(mapping, 0, header);
      for (var index = 0; index < MAGIC.length; index++) {
        if (header[index] != MAGIC[index]) {
          throw invalid("ipc.bell.magic_invalid", "Invalid Bell Region magic");
        }
      }
      if (readInt(header, VERSION_OFFSET) != FORMAT_VERSION) {
        throw invalid(
            "ipc.bell.format_version_unsupported", "Unsupported Bell Region format version");
      }
      var slotCount = readInt(header, SLOT_COUNT_OFFSET);
      if (slotCount == 0) {
        throw invalid("ipc.bell.slot_count_invalid", "Bell Region slot count must be non-zero");
      }
      if (anyNonZero(header, HEADER_PADDING_START, HEADER_LENGTH)) {
        throw invalid("ipc.bell.padding_nonzero", "Bell Region header padding must be zero");
      }
      if (fileLength != regionLength(slotCount)) {
        throw invalid(
            "ipc.bell.region_length_invalid", "Bell Region length does not match its slot count");
      }
      for (var index = 0; index < slotCount; index++) {
        var state = (int) INT.getVolatile(mapping, slotOffset(index));
        if (Integer.compareUnsigned(state, NOTIFIED_VALUE) > 0) {
          throw invalid(
              "ipc.bell.slot_state_invalid", "Bell Region slot word is neither armed nor notified");
        }
        if (anyNonZero(mapping, slotOffset(index) + Integer.BYTES, slotOffset(index + 1))) {
          throw invalid("ipc.bell.padding_nonzero", "Bell Region slot padding must be zero");
        }
      }
      return new BellRegion(arena, mapping, slotCount);
    } catch (IOException | RuntimeException error) {
      arena.close();
      throw error;
    }
  }

  /**
   * Creates one Bell Region file holding {@code slotCount} notified slots.
   *
   * <p>The Core runtime creates every production Region; repository tests and process fixtures use
   * this to play that peer side. {@code epoch} is diagnostic only and never participates in a
   * correctness decision.
   */
  static void create(Path path, int slotCount, long epoch) throws IOException {
    Objects.requireNonNull(path, "path");
    if (slotCount == 0) {
      throw invalid("ipc.bell.slot_count_invalid", "Bell Region slot count must be non-zero");
    }
    var length = regionLength(slotCount);
    if (length > Integer.MAX_VALUE) {
      throw invalid("ipc.bell.slot_count_invalid", "Bell Region slot count has no contract length");
    }
    var bytes = new byte[(int) length];
    System.arraycopy(MAGIC, 0, bytes, 0, MAGIC.length);
    writeInt(bytes, VERSION_OFFSET, FORMAT_VERSION);
    writeInt(bytes, SLOT_COUNT_OFFSET, slotCount);
    writeLong(bytes, EPOCH_OFFSET, epoch);
    for (var index = 0; index < slotCount; index++) {
      writeInt(bytes, Math.toIntExact(slotOffset(index)), NOTIFIED_VALUE);
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

  /** Returns how many doorbells this Region holds. */
  public int slotCount() {
    return slotCount;
  }

  /** Builds the doorbell of one waiting loop inside this Region. */
  public LoopBell loopBell(int index) throws BellRegionException {
    return new LoopBell(slot(index));
  }

  /** Resolves one in-range slot of this Region. */
  BellSlot slot(int index) throws BellRegionException {
    if (Integer.compareUnsigned(index, slotCount) >= 0) {
      throw invalid("ipc.bell.slot_out_of_range", "Bell Region slot index is out of range");
    }
    return new BellSlot(this, index);
  }

  @Override
  public void close() {
    closed = true;
    arena.close();
  }

  /** Returns the addressable four-byte word of one slot. */
  MemorySegment word(int index) throws IOException {
    requireOpen();
    return mapping.asSlice(slotOffset(index), Integer.BYTES);
  }

  /** Arms one slot for the next ring and returns its previous word. */
  int arm(int index) throws IOException {
    return swap(index, ARMED_VALUE, true);
  }

  /** Publishes one ring on one slot and returns its previous word. */
  int notify(int index) throws IOException {
    return swap(index, NOTIFIED_VALUE, false);
  }

  private int swap(int index, int value, boolean acquireRelease) throws IOException {
    requireOpen();
    var offset = slotOffset(index);
    var previous =
        acquireRelease
            ? (int) INT.getAndSetAcquire(mapping, offset, value)
            : (int) INT.getAndSetRelease(mapping, offset, value);
    if (Integer.compareUnsigned(previous, NOTIFIED_VALUE) > 0) {
      throw invalid(
          "ipc.bell.slot_state_invalid", "Bell Region slot word is neither armed nor notified");
    }
    return previous;
  }

  private void requireOpen() throws IOException {
    if (closed) {
      throw invalid("ipc.bell.region_closed", "Bell Region is closed");
    }
  }

  /** Returns the exact file length of a Region holding {@code slotCount} slots. */
  static long regionLength(int slotCount) {
    var unaligned = HEADER_LENGTH + (long) SLOT_LENGTH * Integer.toUnsignedLong(slotCount);
    var remainder = unaligned % PAGE_LENGTH;
    return remainder == 0 ? unaligned : unaligned + (PAGE_LENGTH - remainder);
  }

  /** Returns the byte offset of one slot's state word. */
  static long slotOffset(int index) {
    return HEADER_LENGTH + (long) SLOT_LENGTH * index;
  }

  private static boolean anyNonZero(MemorySegment segment, long offset, long end) {
    var length = Math.toIntExact(end - offset);
    var bytes = new byte[length];
    copyOut(segment, offset, bytes);
    for (var value : bytes) {
      if (value != 0) {
        return true;
      }
    }
    return false;
  }

  private static boolean anyNonZero(byte[] bytes, int from, int to) {
    for (var index = from; index < to; index++) {
      if (bytes[index] != 0) {
        return true;
      }
    }
    return false;
  }

  private static void copyOut(MemorySegment segment, long offset, byte[] destination) {
    MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, destination, 0, destination.length);
  }

  private static int readInt(byte[] bytes, int offset) {
    return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(offset);
  }

  private static void writeInt(byte[] bytes, int offset, int value) {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, value);
  }

  private static void writeLong(byte[] bytes, int offset, long value) {
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).putLong(offset, value);
  }

  private static BellRegionException invalid(String code, String message) {
    return new BellRegionException(code, message);
  }
}
