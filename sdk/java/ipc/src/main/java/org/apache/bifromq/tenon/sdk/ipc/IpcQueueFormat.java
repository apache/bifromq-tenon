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

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Encodes, decodes, and plans stable Tenon IPC Queue version 1 bytes.
 *
 * <p>The header holds no doorbell word. It publishes the ordinal of the bell slot each waiting loop
 * parks in, and a peer rings that slot inside the waiting loop's own Bell Region.
 */
public final class IpcQueueFormat {
  public static final int FORMAT_VERSION = 1;
  public static final int HEADER_LENGTH = 192;
  public static final int FRAME_HEADER_LENGTH = 8;
  public static final int FRAME_ALIGNMENT = 8;

  private static final byte[] MAGIC = {'T', 'E', 'N', 'O', 'N', 'Q', 0, 0};
  private static final int FORMAT_VERSION_OFFSET = 8;
  private static final int MAX_PAYLOAD_SIZE_OFFSET = 16;
  private static final int IMMUTABLE_PADDING_OFFSET = 24;
  static final int COMMIT_OFFSET = 64;
  static final int READER_BELL_SLOT_OFFSET = 72;
  private static final int WRITER_SLOT_PADDING_OFFSET = 76;
  static final int RELEASE_OFFSET = 128;
  static final int WRITER_BELL_SLOT_OFFSET = 136;
  private static final int READER_SLOT_PADDING_OFFSET = 140;
  private static final int MIN_DATA_CAPACITY = 16;

  /**
   * Header value of a bell slot that its owning loop has not published yet.
   *
   * <p>A Queue file is created before either endpoint opens it, so both slot fields start here. No
   * ordinal is valid, and a peer that reads this value must not ring anything: the loop it would
   * wake has not bound this Queue yet, and a ring published before that binding would be lost
   * anyway.
   */
  public static final int UNBOUND_BELL_SLOT = 0xFFFFFFFF;

  private IpcQueueFormat() {}

  /** Encodes the exact 192-byte version 1 header snapshot. */
  public static byte[] encodeHeader(Header header) {
    Objects.requireNonNull(header, "header");
    var encoded = new byte[HEADER_LENGTH];
    System.arraycopy(MAGIC, 0, encoded, 0, MAGIC.length);
    var buffer = littleEndian(encoded);
    buffer.putInt(FORMAT_VERSION_OFFSET, FORMAT_VERSION);
    buffer.putLong(MAX_PAYLOAD_SIZE_OFFSET, header.maxPayloadSize());
    buffer.putLong(COMMIT_OFFSET, header.commit());
    buffer.putInt(READER_BELL_SLOT_OFFSET, header.readerBellSlot());
    buffer.putLong(RELEASE_OFFSET, header.release());
    buffer.putInt(WRITER_BELL_SLOT_OFFSET, header.writerBellSlot());
    return encoded;
  }

  /** Decodes and validates a version 1 header snapshot using actual file capacity. */
  public static Header decodeHeader(DataCapacity capacity, byte[] input)
      throws IpcQueueFormatException {
    Objects.requireNonNull(capacity, "capacity");
    Objects.requireNonNull(input, "input");
    if (input.length < HEADER_LENGTH) {
      throw error("ipc.queue.header_too_short", "IPC Queue header is too short");
    }
    if (!hasMagic(input)) {
      throw error("ipc.queue.magic_invalid", "Invalid IPC Queue magic");
    }

    var buffer = littleEndian(input);
    if (buffer.getInt(FORMAT_VERSION_OFFSET) != FORMAT_VERSION) {
      throw error("ipc.queue.format_version_unsupported", "Unsupported IPC Queue format version");
    }
    if (containsNonZero(input, FORMAT_VERSION_OFFSET + Integer.BYTES, MAX_PAYLOAD_SIZE_OFFSET)
        || containsNonZero(input, IMMUTABLE_PADDING_OFFSET, COMMIT_OFFSET)
        || containsNonZero(input, WRITER_SLOT_PADDING_OFFSET, RELEASE_OFFSET)
        || containsNonZero(input, READER_SLOT_PADDING_OFFSET, HEADER_LENGTH)) {
      throw error(
          "ipc.queue.header_padding_nonzero", "IPC Queue header padding bytes must be zero");
    }
    return Header.of(
        buffer.getLong(MAX_PAYLOAD_SIZE_OFFSET),
        buffer.getLong(COMMIT_OFFSET),
        buffer.getInt(READER_BELL_SLOT_OFFSET),
        buffer.getLong(RELEASE_OFFSET),
        buffer.getInt(WRITER_BELL_SLOT_OFFSET),
        capacity);
  }

  /** Computes the only legal placement for one record without changing Queue state. */
  public static AppendDecision planAppend(
      DataCapacity capacity, long maxPayloadSize, long commit, long release, int recordLength)
      throws IpcQueueFormatException {
    Objects.requireNonNull(capacity, "capacity");
    validatePositions(capacity, commit, release);
    var frameLength = recordFrameLength(capacity, maxPayloadSize, recordLength);
    var occupied = commit - release;
    var free = capacity.bytes() - occupied;
    var physicalCommit = physicalOffset(capacity, commit);
    var tailLength = capacity.bytes() - physicalCommit;
    var wrapLength = tailLength < frameLength ? tailLength : 0;
    // A complete frame fits an int array length and wrap is smaller, so this fits long.
    var required = wrapLength + frameLength;
    if (required > free) {
      return Full.INSTANCE;
    }
    var nextCommit = commit + required;
    if (Long.compareUnsigned(nextCommit, commit) < 0) {
      throw error("ipc.queue.position_exhausted", "IPC Queue logical position is exhausted");
    }
    return new AppendPlan(
        physicalCommit, wrapLength == 0 ? physicalCommit : 0, frameLength, wrapLength, nextCommit);
  }

  /** Encodes one complete aligned record frame. */
  public static byte[] encodeRecordFrame(DataCapacity capacity, long maxPayloadSize, byte[] record)
      throws IpcQueueFormatException {
    Objects.requireNonNull(capacity, "capacity");
    Objects.requireNonNull(record, "record");
    var frameLength = recordFrameLength(capacity, maxPayloadSize, record.length);
    var encoded = new byte[frameLength];
    littleEndian(encoded).putInt(0, record.length);
    System.arraycopy(record, 0, encoded, FRAME_HEADER_LENGTH, record.length);
    return encoded;
  }

  /** Returns the exact eight-byte wrap marker. */
  public static byte[] encodeWrapFrame() {
    return new byte[FRAME_HEADER_LENGTH];
  }

  /** Decodes one complete frame after taking ownership of the reader's sole byte-array copy. */
  public static Frame decodeOwnedFrame(DataCapacity capacity, long maxPayloadSize, byte[] input)
      throws IpcQueueFormatException {
    Objects.requireNonNull(capacity, "capacity");
    Objects.requireNonNull(input, "input");
    var recordLength = inspectRecordLength(input);
    if (recordLength == 0) {
      return WrapFrame.INSTANCE;
    }
    var frameLength = recordFrameLength(capacity, maxPayloadSize, recordLength);
    return new RecordFrame(ownedRecordBytes(input, recordLength, frameLength));
  }

  /** Returns one Queue-owned record body without another array copy. */
  static ByteString decodeOwnedRecord(DataCapacity capacity, long maxPayloadSize, byte[] input)
      throws IpcQueueFormatException {
    var recordLength = inspectRecordLength(input);
    if (recordLength == 0) {
      throw error("ipc.queue.frame_truncated", "IPC Queue frame changed while being read");
    }
    var frameLength = recordFrameLength(capacity, maxPayloadSize, recordLength);
    return ownedRecordBytes(input, recordLength, frameLength);
  }

  /** Validates one fixed frame header and returns zero for wrap or the complete frame length. */
  static int inspectFrameLength(DataCapacity capacity, long maxPayloadSize, byte[] input)
      throws IpcQueueFormatException {
    var recordLength = inspectRecordLength(input);
    return recordLength == 0 ? 0 : recordFrameLength(capacity, maxPayloadSize, recordLength);
  }

  /** Returns data capacity for a positive record limit plus one maximum-frame wrap reserve. */
  public static DataCapacity dataCapacityForRecordLimit(long recordLimit, long maxPayloadSize)
      throws IpcQueueFormatException {
    if (recordLimit <= 0 || maxPayloadSize <= 0) {
      throw error("ipc.queue.capacity_invalid", "Invalid IPC Queue data capacity");
    }
    try {
      var frameCount = Math.addExact(recordLimit, 1);
      var capacity = Math.multiplyExact(frameCount, alignedFrameLength(maxPayloadSize));
      return DataCapacity.of(capacity);
    } catch (ArithmeticException ignored) {
      throw error("ipc.queue.capacity_invalid", "Invalid IPC Queue data capacity");
    }
  }

  /** Recovers a positive record limit from capacity with one maximum-frame wrap reserve. */
  public static long recordLimitFromDataCapacity(DataCapacity capacity, long maxPayloadSize)
      throws IpcQueueFormatException {
    Objects.requireNonNull(capacity, "capacity");
    validateMaxPayloadSize(capacity, maxPayloadSize);
    var maximumFrameLength = alignedFrameLength(maxPayloadSize);
    if (capacity.bytes() % maximumFrameLength != 0) {
      throw error("ipc.queue.capacity_invalid", "Invalid IPC Queue data capacity");
    }
    var recordLimit = capacity.bytes() / maximumFrameLength - 1;
    if (recordLimit <= 0) {
      throw error("ipc.queue.capacity_invalid", "Invalid IPC Queue data capacity");
    }
    return recordLimit;
  }

  /** Validates capacity and payload size against an exact record limit and payload size. */
  public static void validateRecordLimitLayout(
      DataCapacity capacity,
      long actualMaxPayloadSize,
      long recordLimit,
      long expectedMaxPayloadSize)
      throws IpcQueueFormatException {
    Objects.requireNonNull(capacity, "capacity");
    if (actualMaxPayloadSize != expectedMaxPayloadSize
        || !capacity.equals(dataCapacityForRecordLimit(recordLimit, expectedMaxPayloadSize))) {
      throw error("ipc.queue.capacity_invalid", "Invalid IPC Queue data capacity");
    }
  }

  private static int inspectRecordLength(byte[] input) throws IpcQueueFormatException {
    if (input.length < FRAME_HEADER_LENGTH) {
      throw error("ipc.queue.frame_truncated", "IPC Queue frame is truncated");
    }
    if (containsNonZero(input, Integer.BYTES, FRAME_HEADER_LENGTH)) {
      throw error(
          "ipc.queue.frame_header_padding_nonzero", "IPC Queue frame header padding must be zero");
    }

    var recordLength = readUnsignedLittleEndianInt(input);
    if (recordLength == 0) {
      return 0;
    }
    if (recordLength > Integer.MAX_VALUE) {
      throw error("ipc.queue.record_too_large", "IPC Queue record body exceeds the current limit");
    }
    return (int) recordLength;
  }

  private static ByteString ownedRecordBytes(byte[] input, int recordLength, int frameLength)
      throws IpcQueueFormatException {
    if (input.length < frameLength) {
      throw error("ipc.queue.frame_truncated", "IPC Queue frame is truncated");
    }
    var recordEnd = FRAME_HEADER_LENGTH + recordLength;
    if (containsNonZero(input, recordEnd, frameLength)) {
      throw error("ipc.queue.frame_padding_nonzero", "IPC Queue frame padding must be zero");
    }
    var ownedBytes = UnsafeByteOperations.unsafeWrap(input);
    return ownedBytes.substring(FRAME_HEADER_LENGTH, recordEnd);
  }

  static void validatePositions(DataCapacity capacity, long commit, long release)
      throws IpcQueueFormatException {
    if (!isAligned(commit) || !isAligned(release)) {
      throw error("ipc.queue.position_unaligned", "IPC Queue logical position is not aligned");
    }
    if (Long.compareUnsigned(release, commit) > 0) {
      throw error(
          "ipc.queue.release_ahead_of_commit", "IPC Queue release position is ahead of commit");
    }
    var occupied = commit - release;
    if (Long.compareUnsigned(occupied, capacity.bytes()) > 0) {
      throw error(
          "ipc.queue.occupancy_exceeds_capacity", "IPC Queue occupancy exceeds data capacity");
    }
  }

  static long physicalOffset(DataCapacity capacity, long position) {
    var capacityBytes = capacity.bytes();
    var mask = capacityBytes - 1;
    return (capacityBytes & mask) == 0
        ? position & mask
        : Long.remainderUnsigned(position, capacityBytes);
  }

  private static int recordFrameLength(DataCapacity capacity, long maxPayloadSize, int recordLength)
      throws IpcQueueFormatException {
    validateMaxPayloadSize(capacity, maxPayloadSize);
    if (recordLength == 0) {
      throw error("ipc.queue.record_empty", "IPC Queue record body must not be empty");
    }
    if (recordLength < 0 || recordLength > maxPayloadSize) {
      throw error("ipc.queue.record_too_large", "IPC Queue record body exceeds the current limit");
    }
    return alignedFrameLength(recordLength);
  }

  private static void validateMaxPayloadSize(DataCapacity capacity, long maxPayloadSize)
      throws IpcQueueFormatException {
    try {
      if (maxPayloadSize > 0 && alignedFrameLength(maxPayloadSize) <= capacity.bytes()) {
        return;
      }
    } catch (ArithmeticException ignored) {
      throw error("ipc.queue.max_payload_size_invalid", "Invalid IPC Queue maximum payload size");
    }
    throw error("ipc.queue.max_payload_size_invalid", "Invalid IPC Queue maximum payload size");
  }

  private static int alignedFrameLength(long recordLength) {
    var unpadded = Math.addExact(FRAME_HEADER_LENGTH, recordLength);
    var padding = (FRAME_ALIGNMENT - unpadded % FRAME_ALIGNMENT) % FRAME_ALIGNMENT;
    return Math.toIntExact(Math.addExact(unpadded, padding));
  }

  private static ByteBuffer littleEndian(byte[] bytes) {
    return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static boolean hasMagic(byte[] bytes) {
    for (var index = 0; index < MAGIC.length; index++) {
      if (bytes[index] != MAGIC[index]) {
        return false;
      }
    }
    return true;
  }

  private static boolean containsNonZero(byte[] bytes, int start, int end) {
    for (var index = start; index < end; index++) {
      if (bytes[index] != 0) {
        return true;
      }
    }
    return false;
  }

  private static long readUnsignedLittleEndianInt(byte[] bytes) {
    var value =
        Byte.toUnsignedInt(bytes[0])
            | Byte.toUnsignedInt(bytes[1]) << 8
            | Byte.toUnsignedInt(bytes[2]) << 16
            | Byte.toUnsignedInt(bytes[3]) << 24;
    return Integer.toUnsignedLong(value);
  }

  private static boolean isAligned(long value) {
    return (value & (FRAME_ALIGNMENT - 1)) == 0;
  }

  private static IpcQueueFormatException error(String code, String message) {
    return new IpcQueueFormatException(code, message);
  }

  /** A validated byte capacity for the Queue data region. */
  public static final class DataCapacity {
    private final long bytes;

    private DataCapacity(long bytes) {
      this.bytes = bytes;
    }

    public static DataCapacity of(long bytes) throws IpcQueueFormatException {
      if (bytes < MIN_DATA_CAPACITY
          || !isAligned(bytes)
          || bytes > Long.MAX_VALUE - HEADER_LENGTH) {
        throw error("ipc.queue.capacity_invalid", "Invalid IPC Queue data capacity");
      }
      return new DataCapacity(bytes);
    }

    public static DataCapacity fromFileLength(long fileLength) throws IpcQueueFormatException {
      if (fileLength < HEADER_LENGTH) {
        throw error("ipc.queue.capacity_invalid", "Invalid IPC Queue data capacity");
      }
      return of(fileLength - HEADER_LENGTH);
    }

    public long bytes() {
      return bytes;
    }

    public long fileLength() {
      return bytes + HEADER_LENGTH;
    }

    @Override
    public boolean equals(Object other) {
      return this == other || other instanceof DataCapacity capacity && bytes == capacity.bytes;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(bytes);
    }
  }

  /** One validated header snapshot. */
  public static final class Header {
    private final long maxPayloadSize;
    private final long commit;
    private final int readerBellSlot;
    private final long release;
    private final int writerBellSlot;

    private Header(
        long maxPayloadSize, long commit, int readerBellSlot, long release, int writerBellSlot) {
      this.maxPayloadSize = maxPayloadSize;
      this.commit = commit;
      this.readerBellSlot = readerBellSlot;
      this.release = release;
      this.writerBellSlot = writerBellSlot;
    }

    /**
     * Validates one header snapshot.
     *
     * <p>Both slot fields carry an ordinal the owning loop published, so every unsigned 32-bit
     * value is legal here; only a loop that reads the ordinal can reject it as unbound or out of
     * range for the Region it opened.
     */
    public static Header of(
        long maxPayloadSize,
        long commit,
        int readerBellSlot,
        long release,
        int writerBellSlot,
        DataCapacity capacity)
        throws IpcQueueFormatException {
      Objects.requireNonNull(capacity, "capacity");
      validateMaxPayloadSize(capacity, maxPayloadSize);
      validatePositions(capacity, commit, release);
      return new Header(maxPayloadSize, commit, readerBellSlot, release, writerBellSlot);
    }

    public long maxPayloadSize() {
      return maxPayloadSize;
    }

    public long commit() {
      return commit;
    }

    /** Returns the ordinal the reader loop published inside its own Bell Region. */
    public int readerBellSlot() {
      return readerBellSlot;
    }

    public long release() {
      return release;
    }

    /** Returns the ordinal the writer loop published inside its own Bell Region. */
    public int writerBellSlot() {
      return writerBellSlot;
    }

    @Override
    public boolean equals(Object other) {
      return this == other
          || other instanceof Header header
              && maxPayloadSize == header.maxPayloadSize
              && commit == header.commit
              && readerBellSlot == header.readerBellSlot
              && release == header.release
              && writerBellSlot == header.writerBellSlot;
    }

    @Override
    public int hashCode() {
      return Objects.hash(maxPayloadSize, commit, readerBellSlot, release, writerBellSlot);
    }
  }

  /** Normal append planning result. */
  public sealed interface AppendDecision permits AppendPlan, Full {}

  /** Exact physical and logical range reserved for one successful append. */
  public record AppendPlan(
      long writeOffset, long frameOffset, int frameLength, long wrapLength, long nextCommit)
      implements AppendDecision {}

  /** Normal Full result that exposes no writable range. */
  public enum Full implements AppendDecision {
    INSTANCE
  }

  /** A decoded IPC Queue frame. */
  public sealed interface Frame permits RecordFrame, WrapFrame {}

  /** One complete caller-selected record body owned by the reader. */
  public record RecordFrame(ByteString recordBytes) implements Frame {}

  /** Marker that tells a reader to skip the complete physical tail. */
  public enum WrapFrame implements Frame {
    INSTANCE
  }
}
