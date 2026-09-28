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
import java.util.Optional;

/**
 * Opens the unique reader or writer of one mapped Tenon IPC Queue.
 *
 * <p>A Queue file holds no doorbell word. Its header publishes the ordinal of the bell slot each
 * waiting loop parks in, and a peer rings that slot inside the waiting loop's own Bell Region. Each
 * endpoint therefore opens the Region its own loop parks in and the Region of the loop it must
 * ring.
 */
public final class IpcQueue {
  private static final VarHandle LONG =
      ValueLayout.JAVA_LONG.withOrder(ByteOrder.LITTLE_ENDIAN).varHandle();
  private static final VarHandle INT =
      ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN).varHandle();

  private IpcQueue() {}

  /** Creates one new zero-position Queue file. */
  public static void create(Path path, IpcQueueFormat.DataCapacity capacity, long maxPayloadSize)
      throws IOException {
    var header =
        IpcQueueFormat.Header.of(
            maxPayloadSize,
            0,
            IpcQueueFormat.UNBOUND_BELL_SLOT,
            0,
            IpcQueueFormat.UNBOUND_BELL_SLOT,
            capacity);
    try (var channel =
        FileChannel.open(
            path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE)) {
      channel.position(capacity.fileLength() - 1);
      writeFully(channel, ByteBuffer.wrap(new byte[] {0}));
      channel.position(0);
      writeFully(channel, ByteBuffer.wrap(IpcQueueFormat.encodeHeader(header)));
    }
  }

  /**
   * Opens the unique writer endpoint and publishes its own loop's bell slot.
   *
   * @param bell the doorbell of the loop that waits for available space
   * @param peerRegion the Region of the loop that waits for data
   */
  public static Writer openWriter(Path path, LoopBell bell, BellRegion peerRegion)
      throws IOException, IpcQueueFormatException {
    return new Writer(Mapping.open(path, OpenRole.WRITER, bell, peerRegion));
  }

  /**
   * Opens the unique reader endpoint and publishes its own loop's bell slot.
   *
   * @param bell the doorbell of the loop that waits for data
   * @param peerRegion the Region of the loop that waits for available space
   */
  public static Reader openReader(Path path, LoopBell bell, BellRegion peerRegion)
      throws IOException, IpcQueueFormatException {
    return new Reader(Mapping.open(path, OpenRole.READER, bell, peerRegion));
  }

  private static void writeFully(FileChannel channel, ByteBuffer bytes) throws IOException {
    while (bytes.hasRemaining()) {
      if (channel.write(bytes) <= 0) {
        throw new IOException("IPC Queue file write made no progress");
      }
    }
  }

  /** Normal result of one nonblocking write attempt. */
  public sealed interface WriteOutcome permits Committed, Full {}

  /** One complete record was published at this release boundary. */
  public record Committed(WriteReceipt receipt) implements WriteOutcome {}

  /** Released capacity cannot hold the requested record. */
  public enum Full implements WriteOutcome {
    INSTANCE
  }

  /** Opaque release boundary owned by the exact writer that committed it. */
  public static final class WriteReceipt {
    private final long exclusiveEnd;
    private final Object writerIdentity;

    private WriteReceipt(long exclusiveEnd, Object writerIdentity) {
      this.exclusiveEnd = exclusiveEnd;
      this.writerIdentity = writerIdentity;
    }
  }

  /** Normal result of one Queue wait. */
  public enum WaitResult {
    READY,
    INTERRUPTED
  }

  /** The unique mapped Queue writer. */
  public static final class Writer implements AutoCloseable {
    private final Mapping mapping;
    private final Object identity = new Object();

    private Writer(Mapping mapping) {
      this.mapping = mapping;
    }

    public IpcQueueFormat.DataCapacity capacity() {
      return mapping.capacity;
    }

    public long maxPayloadSize() {
      return mapping.maxPayloadSize;
    }

    /**
     * Returns the Queue-lifetime sequence reserved by the next successful write position.
     *
     * <p>Repeated calls return the same value until this unique writer commits another record.
     */
    public long nextWriteSequence() throws IpcQueueFormatException {
      var commit = mapping.loadLongAcquire(IpcQueueFormat.COMMIT_OFFSET);
      var release = mapping.loadLongAcquire(IpcQueueFormat.RELEASE_OFFSET);
      IpcQueueFormat.validatePositions(mapping.capacity, commit, release);
      return commit + 1;
    }

    /** Tries once to publish one complete record. */
    public WriteOutcome tryWrite(byte[] record) throws IOException, IpcQueueFormatException {
      var commit = mapping.loadLongAcquire(IpcQueueFormat.COMMIT_OFFSET);
      var release = mapping.loadLongAcquire(IpcQueueFormat.RELEASE_OFFSET);
      var decision =
          IpcQueueFormat.planAppend(
              mapping.capacity, mapping.maxPayloadSize, commit, release, record.length);
      if (decision == IpcQueueFormat.Full.INSTANCE) {
        return Full.INSTANCE;
      }
      var plan = (IpcQueueFormat.AppendPlan) decision;
      if (plan.wrapLength() != 0) {
        mapping.writeWrap(plan.writeOffset());
      }
      mapping.writeRecord(plan.frameOffset(), plan.frameLength(), record);
      mapping.storeLongRelease(IpcQueueFormat.COMMIT_OFFSET, plan.nextCommit());
      mapping.ringPeer();
      return new Committed(new WriteReceipt(plan.nextCommit(), identity));
    }

    /** Returns whether this writer's committed record has been released. */
    public boolean isReleased(WriteReceipt receipt) throws IpcQueueFormatException {
      validateReceipt(receipt);
      return receiptReleased(receipt.exclusiveEnd);
    }

    /** Waits until this writer's committed record has been released. */
    public WaitResult waitReleased(WriteReceipt receipt)
        throws IOException, IpcQueueFormatException {
      validateReceipt(receipt);
      return mapping.waitFor(() -> receiptReleased(receipt.exclusiveEnd));
    }

    /** Waits until one record of this size can be written, without writing it. */
    public WaitResult waitWritable(int recordLength) throws IOException, IpcQueueFormatException {
      return mapping.waitFor(() -> writable(recordLength));
    }

    /**
     * Reports whether one record of this size can be written now.
     *
     * <p>A loop that parks over several Queues shares one doorbell, so its parked condition has to
     * cover every Queue it subscribes to. This is the condition half of {@link #waitWritable(int)},
     * without the parking.
     */
    public boolean writable(int recordLength) throws IpcQueueFormatException {
      var decision =
          IpcQueueFormat.planAppend(
              mapping.capacity,
              mapping.maxPayloadSize,
              mapping.loadLongAcquire(IpcQueueFormat.COMMIT_OFFSET),
              mapping.loadLongAcquire(IpcQueueFormat.RELEASE_OFFSET),
              recordLength);
      return decision != IpcQueueFormat.Full.INSTANCE;
    }

    public BellInterrupter interrupter() {
      return mapping.interrupter();
    }

    private void validateReceipt(WriteReceipt receipt) {
      if (receipt == null || receipt.writerIdentity != identity) {
        throw new IllegalArgumentException("Write receipt belongs to another Queue writer");
      }
    }

    private boolean receiptReleased(long exclusiveEnd) throws IpcQueueFormatException {
      var commit = mapping.loadLongAcquire(IpcQueueFormat.COMMIT_OFFSET);
      var release = mapping.loadLongAcquire(IpcQueueFormat.RELEASE_OFFSET);
      IpcQueueFormat.validatePositions(mapping.capacity, commit, release);
      return Long.compareUnsigned(release, exclusiveEnd) >= 0;
    }

    @Override
    public void close() {
      mapping.close();
    }
  }

  /** The unique mapped Queue reader. */
  public static final class Reader implements AutoCloseable {
    private final Mapping mapping;
    private final byte[] frameHeader = new byte[IpcQueueFormat.FRAME_HEADER_LENGTH];
    private final ReleaseBoundaryQueue releaseBoundaries = new ReleaseBoundaryQueue();
    private boolean snapshotActive;
    private long snapshotCommit;

    private Reader(Mapping mapping) {
      this.mapping = mapping;
    }

    public IpcQueueFormat.DataCapacity capacity() {
      return mapping.capacity;
    }

    public long maxPayloadSize() {
      return mapping.maxPayloadSize;
    }

    /** Tries once to copy the next complete record into process-owned memory. */
    public Optional<ByteString> tryRead() throws IpcQueueFormatException {
      var release = mapping.loadLongAcquire(IpcQueueFormat.RELEASE_OFFSET);
      var commit = mapping.loadLongAcquire(IpcQueueFormat.COMMIT_OFFSET);
      var read = readPosition(commit, release);
      snapshotActive = false;
      return tryReadUntil(read, commit);
    }

    /**
     * Reads one record from a stable snapshot of the current committed prefix.
     *
     * <p>The first call captures the current commit. Repeated calls return only records within that
     * boundary. An empty result ends the snapshot; the following call starts a new one. Records
     * committed by the writer while a snapshot is being drained therefore enter the next batch.
     */
    public Optional<ByteString> tryReadCommittedSnapshot() throws IpcQueueFormatException {
      var release = mapping.loadLongAcquire(IpcQueueFormat.RELEASE_OFFSET);
      if (!snapshotActive) {
        snapshotCommit = mapping.loadLongAcquire(IpcQueueFormat.COMMIT_OFFSET);
        snapshotActive = true;
      }
      var read = readPosition(snapshotCommit, release);
      if (read == snapshotCommit) {
        snapshotActive = false;
        return Optional.empty();
      }
      if (Long.compareUnsigned(read, snapshotCommit) > 0) {
        throw new IpcQueueFormatException(
            "ipc.queue.snapshot_regressed", "IPC Queue read position passed its snapshot");
      }
      return tryReadUntil(read, snapshotCommit);
    }

    private Optional<ByteString> tryReadUntil(long read, long commit)
        throws IpcQueueFormatException {
      if (commit == read) {
        return Optional.empty();
      }
      while (true) {
        var physical = IpcQueueFormat.physicalOffset(mapping.capacity, read);
        var tailLength = mapping.capacity.bytes() - physical;
        mapping.readData(physical, frameHeader);
        var frameLength =
            IpcQueueFormat.inspectFrameLength(
                mapping.capacity, mapping.maxPayloadSize, frameHeader);
        if (frameLength == 0) {
          read = advance(read, tailLength);
          if (Long.compareUnsigned(read, commit) >= 0) {
            throw truncatedFrame();
          }
          continue;
        }
        if (frameLength > tailLength) {
          throw truncatedFrame();
        }
        var next = advance(read, frameLength);
        if (Long.compareUnsigned(next, commit) > 0) {
          throw truncatedFrame();
        }
        var frameBytes = new byte[frameLength];
        mapping.readData(physical, frameBytes);
        var record =
            IpcQueueFormat.decodeOwnedRecord(mapping.capacity, mapping.maxPayloadSize, frameBytes);
        releaseBoundaries.addLast(next);
        return Optional.of(record);
      }
    }

    /** Waits until one record is readable, without reading or releasing it. */
    public WaitResult waitReadable() throws IOException, IpcQueueFormatException {
      return mapping.waitFor(this::readable);
    }

    /**
     * Reports whether a committed record is already waiting.
     *
     * <p>A loop that parks over several Queues shares one doorbell, so its parked condition has to
     * cover every Queue it subscribes to. This is the condition half of {@link #waitReadable()},
     * without the parking.
     */
    public boolean readable() throws IpcQueueFormatException {
      var release = mapping.loadLongAcquire(IpcQueueFormat.RELEASE_OFFSET);
      var commit = mapping.loadLongAcquire(IpcQueueFormat.COMMIT_OFFSET);
      return commit != readPosition(commit, release);
    }

    public BellInterrupter interrupter() {
      return mapping.interrupter();
    }

    /** Releases the requested continuous prefix of records returned by this reader. */
    public void release(int count) throws IOException, IpcQueueFormatException {
      if (count == 0) {
        return;
      }
      if (count < 0 || count > releaseBoundaries.size()) {
        throw new IllegalArgumentException("Invalid Queue release count");
      }
      var next = releaseBoundaries.removePrefix(count);
      mapping.storeLongRelease(IpcQueueFormat.RELEASE_OFFSET, next);
      mapping.ringPeer();
    }

    @Override
    public void close() {
      mapping.close();
    }

    private static long advance(long position, long amount) throws IpcQueueFormatException {
      var next = position + amount;
      if (Long.compareUnsigned(next, position) < 0) {
        throw new IpcQueueFormatException(
            "ipc.queue.position_exhausted", "IPC Queue logical position is exhausted");
      }
      return next;
    }

    private static IpcQueueFormatException truncatedFrame() {
      return new IpcQueueFormatException(
          "ipc.queue.frame_truncated", "IPC Queue frame is truncated");
    }

    private long readPosition(long commit, long release) throws IpcQueueFormatException {
      IpcQueueFormat.validatePositions(mapping.capacity, commit, release);
      var read = releaseBoundaries.isEmpty() ? release : releaseBoundaries.last();
      if (Long.compareUnsigned(commit, read) < 0) {
        throw new IpcQueueFormatException(
            "ipc.queue.commit_regressed", "IPC Queue commit moved behind the reader");
      }
      return read;
    }
  }

  private static final class ReleaseBoundaryQueue {
    private static final int INITIAL_CAPACITY = 16;

    private long[] boundaries = new long[0];
    private int head;
    private int size;

    private int size() {
      return size;
    }

    private boolean isEmpty() {
      return size == 0;
    }

    private long last() {
      return boundaries[index(size - 1)];
    }

    private void addLast(long boundary) {
      if (size == boundaries.length) {
        grow();
      }
      boundaries[index(size)] = boundary;
      size++;
    }

    private long removePrefix(int count) {
      var boundary = boundaries[index(count - 1)];
      size -= count;
      if (size == 0) {
        head = 0;
      } else {
        head = index(count);
      }
      return boundary;
    }

    private int index(int offset) {
      var tailLength = boundaries.length - head;
      return offset < tailLength ? head + offset : offset - tailLength;
    }

    private void grow() {
      var previous = boundaries;
      var nextCapacity =
          previous.length == 0 ? INITIAL_CAPACITY : Math.multiplyExact(previous.length, 2);
      boundaries = new long[nextCapacity];
      if (size == 0) {
        return;
      }
      var firstLength = Math.min(size, previous.length - head);
      System.arraycopy(previous, head, boundaries, 0, firstLength);
      System.arraycopy(previous, 0, boundaries, firstLength, size - firstLength);
      head = 0;
    }
  }

  private static final class Mapping implements AutoCloseable {
    private final Arena arena;
    private final MemorySegment segment;
    private final IpcQueueFormat.DataCapacity capacity;
    private final long maxPayloadSize;
    private final LoopBell bell;
    private final BellRegion peerRegion;
    private final int ownBellSlotOffset;
    private final int peerBellSlotOffset;

    private Mapping(
        Arena arena,
        MemorySegment segment,
        IpcQueueFormat.DataCapacity capacity,
        long maxPayloadSize,
        LoopBell bell,
        BellRegion peerRegion,
        int ownBellSlotOffset,
        int peerBellSlotOffset) {
      this.arena = arena;
      this.segment = segment;
      this.capacity = capacity;
      this.maxPayloadSize = maxPayloadSize;
      this.bell = bell;
      this.peerRegion = peerRegion;
      this.ownBellSlotOffset = ownBellSlotOffset;
      this.peerBellSlotOffset = peerBellSlotOffset;
    }

    /**
     * Maps one Queue file and publishes this endpoint's bell slot.
     *
     * <p>That store is the only publication of this endpoint's ordinal: until it lands, the peer
     * reads the unbound value and rings nothing.
     */
    private static Mapping open(Path path, OpenRole role, LoopBell bell, BellRegion peerRegion)
        throws IOException, IpcQueueFormatException {
      var arena = Arena.ofShared();
      try (var channel =
          FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
        var capacity = IpcQueueFormat.DataCapacity.fromFileLength(channel.size());
        var segment = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size(), arena);
        var snapshot = snapshotHeader(segment, role);
        var header = IpcQueueFormat.decodeHeader(capacity, snapshot);
        var mapping =
            new Mapping(
                arena,
                segment,
                capacity,
                header.maxPayloadSize(),
                bell,
                peerRegion,
                role.ownBellSlotOffset(),
                role.peerBellSlotOffset());
        INT.setRelease(segment, (long) mapping.ownBellSlotOffset, bell.slot().index());
        VarHandle.fullFence();
        return mapping;
      } catch (IOException | RuntimeException error) {
        arena.close();
        throw error;
      }
    }

    private static byte[] snapshotHeader(MemorySegment segment, OpenRole role) {
      var snapshot = new byte[IpcQueueFormat.HEADER_LENGTH];
      copyHeaderRange(segment, snapshot, 0, IpcQueueFormat.COMMIT_OFFSET);
      copyHeaderRange(
          segment,
          snapshot,
          IpcQueueFormat.READER_BELL_SLOT_OFFSET + Integer.BYTES,
          IpcQueueFormat.RELEASE_OFFSET);
      copyHeaderRange(
          segment,
          snapshot,
          IpcQueueFormat.WRITER_BELL_SLOT_OFFSET + Integer.BYTES,
          IpcQueueFormat.HEADER_LENGTH);
      var buffer = ByteBuffer.wrap(snapshot).order(ByteOrder.LITTLE_ENDIAN);
      if (role == OpenRole.WRITER) {
        buffer.putLong(
            IpcQueueFormat.COMMIT_OFFSET,
            (long) LONG.getAcquire(segment, (long) IpcQueueFormat.COMMIT_OFFSET));
        buffer.putLong(
            IpcQueueFormat.RELEASE_OFFSET,
            (long) LONG.getAcquire(segment, (long) IpcQueueFormat.RELEASE_OFFSET));
      } else {
        buffer.putLong(
            IpcQueueFormat.RELEASE_OFFSET,
            (long) LONG.getAcquire(segment, (long) IpcQueueFormat.RELEASE_OFFSET));
        buffer.putLong(
            IpcQueueFormat.COMMIT_OFFSET,
            (long) LONG.getAcquire(segment, (long) IpcQueueFormat.COMMIT_OFFSET));
      }
      return snapshot;
    }

    private static void copyHeaderRange(
        MemorySegment segment, byte[] snapshot, int start, int end) {
      MemorySegment.copy(segment, start, MemorySegment.ofArray(snapshot), start, end - start);
    }

    private long loadLongAcquire(long offset) {
      return (long) LONG.getAcquire(segment, offset);
    }

    private void storeLongRelease(long offset, long value) {
      LONG.setRelease(segment, offset, value);
    }

    /** Rings the peer loop's doorbell, or nothing at all while it is unbound. */
    private void ringPeer() throws IOException {
      VarHandle.fullFence();
      var index = (int) INT.getAcquire(segment, (long) peerBellSlotOffset);
      if (index == IpcQueueFormat.UNBOUND_BELL_SLOT) {
        return;
      }
      peerRegion.slot(index).ring();
    }

    private BellInterrupter interrupter() {
      return bell.interrupter();
    }

    private WaitResult waitFor(Condition condition) throws IOException {
      return switch (bell.until(condition::ready)) {
        case READY -> WaitResult.READY;
        case INTERRUPTED -> WaitResult.INTERRUPTED;
      };
    }

    private void readData(long offset, byte[] destination) {
      MemorySegment.copy(
          segment,
          ValueLayout.JAVA_BYTE,
          IpcQueueFormat.HEADER_LENGTH + offset,
          destination,
          0,
          destination.length);
    }

    private void writeWrap(long offset) {
      LONG.set(segment, IpcQueueFormat.HEADER_LENGTH + offset, 0L);
    }

    private void writeRecord(long offset, int frameLength, byte[] record) {
      var frameStart = IpcQueueFormat.HEADER_LENGTH + offset;
      LONG.set(segment, frameStart, Integer.toUnsignedLong(record.length));
      MemorySegment.copy(
          record,
          0,
          segment,
          ValueLayout.JAVA_BYTE,
          frameStart + IpcQueueFormat.FRAME_HEADER_LENGTH,
          record.length);
      var paddingLength = frameLength - IpcQueueFormat.FRAME_HEADER_LENGTH - record.length;
      if (paddingLength != 0) {
        segment
            .asSlice(frameStart + IpcQueueFormat.FRAME_HEADER_LENGTH + record.length, paddingLength)
            .fill((byte) 0);
      }
    }

    @Override
    public void close() {
      arena.close();
    }
  }

  private enum OpenRole {
    WRITER,
    READER;

    private int ownBellSlotOffset() {
      return this == WRITER
          ? IpcQueueFormat.WRITER_BELL_SLOT_OFFSET
          : IpcQueueFormat.READER_BELL_SLOT_OFFSET;
    }

    private int peerBellSlotOffset() {
      return this == WRITER
          ? IpcQueueFormat.READER_BELL_SLOT_OFFSET
          : IpcQueueFormat.WRITER_BELL_SLOT_OFFSET;
    }
  }

  @FunctionalInterface
  private interface Condition {
    boolean ready() throws IpcQueueFormatException;
  }
}
