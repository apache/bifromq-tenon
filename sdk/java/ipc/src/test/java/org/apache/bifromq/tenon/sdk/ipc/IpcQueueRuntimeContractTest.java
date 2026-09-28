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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class IpcQueueRuntimeContractTest {
  @TempDir Path directory;

  @Test
  void mappedWriterAndReaderTransferAndReleaseOneRecord() throws Exception {
    var path = directory.resolve("queue");
    var capacity = IpcQueueFormat.DataCapacity.of(64);
    IpcQueue.create(path, capacity, 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      var receipt = commit(writer, new byte[] {1, 2, 3});

      var record = reader.tryRead().orElseThrow();
      assertRecord(new byte[] {1, 2, 3}, record);
      reader.release(1);
      assertTrue(writer.isReleased(receipt));
    }

    assertEquals(256, Files.size(path));
  }

  @Test
  void committedSnapshotDoesNotChaseRecordsWrittenWhileTheBatchIsDraining() throws Exception {
    var path = directory.resolve("snapshot.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      commit(writer, new byte[] {1});
      assertRecord(new byte[] {1}, reader.tryReadCommittedSnapshot().orElseThrow());

      commit(writer, new byte[] {2});
      assertTrue(reader.tryReadCommittedSnapshot().isEmpty());
      assertRecord(new byte[] {2}, reader.tryReadCommittedSnapshot().orElseThrow());
      assertTrue(reader.tryReadCommittedSnapshot().isEmpty());
    }
  }

  @Test
  void committedSnapshotCrossesAQueueWrapWithoutChangingItsBoundary() throws Exception {
    var path = directory.resolve("wrapped-snapshot.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      commit(writer, new byte[9]);
      commit(writer, new byte[17]);
      assertRecord(new byte[9], reader.tryRead().orElseThrow());
      assertRecord(new byte[17], reader.tryRead().orElseThrow());
      reader.release(2);

      commit(writer, new byte[] {1});
      assertRecord(new byte[] {1}, reader.tryReadCommittedSnapshot().orElseThrow());
      commit(writer, new byte[] {2});
      assertTrue(reader.tryReadCommittedSnapshot().isEmpty());
      assertRecord(new byte[] {2}, reader.tryReadCommittedSnapshot().orElseThrow());
      assertTrue(reader.tryReadCommittedSnapshot().isEmpty());
    }
  }

  @Test
  void writeSequenceContinuesAfterWriterReopensTheSameQueue() throws Exception {
    var path = directory.resolve("sequence.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    long first;
    try (var writer = QueueFixtures.writer(path, bells)) {
      first = writer.nextWriteSequence();
      commit(writer, new byte[] {1});
    }
    try (var writer = QueueFixtures.writer(path, bells)) {
      var second = writer.nextWriteSequence();

      assertTrue(Long.compareUnsigned(second, first) > 0);
    }
  }

  @Test
  void emptyReadAndFullWriteDoNotChangeQueueBytes() throws Exception {
    var emptyPath = directory.resolve("empty.queue");
    IpcQueue.create(emptyPath, IpcQueueFormat.DataCapacity.of(32), 8);
    var emptyBells = QueueFixtures.bellsFor(emptyPath, 2);

    try (var reader = QueueFixtures.reader(emptyPath, emptyBells)) {
      var emptySnapshot = Files.readAllBytes(emptyPath);
      assertTrue(reader.tryRead().isEmpty());
      assertArrayEquals(emptySnapshot, Files.readAllBytes(emptyPath));
    }

    var fullPath = directory.resolve("full.queue");
    IpcQueue.create(fullPath, IpcQueueFormat.DataCapacity.of(32), 8);
    var fullBells = QueueFixtures.bellsFor(fullPath, 2);
    try (var writer = QueueFixtures.writer(fullPath, fullBells)) {
      commit(writer, new byte[] {1});
      commit(writer, new byte[] {2});
      var fullSnapshot = Files.readAllBytes(fullPath);

      assertEquals(IpcQueue.Full.INSTANCE, writer.tryWrite(new byte[] {3}));
      assertArrayEquals(fullSnapshot, Files.readAllBytes(fullPath));
    }
  }

  @Test
  void blockedWriterIsWokenAfterReaderReleasesSpace() throws Exception {
    var path = directory.resolve("space-wait.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      commit(writer, new byte[] {1});
      commit(writer, new byte[] {2});
      assertRecord(new byte[] {1}, reader.tryRead().orElseThrow());

      var waiting = executor.submit(() -> writer.waitWritable(1));
      awaitWriterArmed(path);
      reader.release(1);

      assertEquals(IpcQueue.WaitResult.READY, waiting.get(1, TimeUnit.SECONDS));
      commit(writer, new byte[] {3});
    }
  }

  @Test
  void receiptWaitCompletesOnlyAfterReaderReleasesItsRecord() throws Exception {
    var path = directory.resolve("receipt-wait.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      var receipt = commit(writer, new byte[] {1});
      assertFalse(writer.isReleased(receipt));
      assertRecord(new byte[] {1}, reader.tryRead().orElseThrow());

      var waiting = executor.submit(() -> writer.waitReleased(receipt));
      awaitWriterArmed(path);
      reader.release(1);

      assertEquals(IpcQueue.WaitResult.READY, waiting.get(1, TimeUnit.SECONDS));
      assertTrue(writer.isReleased(receipt));
    }
  }

  @Test
  void wrapPreservesRecordOrderAndReceiptBoundary() throws Exception {
    var path = directory.resolve("wrap.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(40), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      commit(writer, new byte[] {1});
      commit(writer, new byte[] {2});
      var first = reader.tryRead().orElseThrow();
      assertRecord(new byte[] {1}, first);
      reader.release(1);

      var wrappedReceipt = commit(writer, new byte[] {3});
      assertRecord(new byte[] {1}, first);
      assertRecord(new byte[] {2}, reader.tryRead().orElseThrow());
      assertRecord(new byte[] {3}, reader.tryRead().orElseThrow());
      reader.release(1);
      assertFalse(writer.isReleased(wrappedReceipt));
      reader.release(1);

      assertTrue(writer.isReleased(wrappedReceipt));
    }
  }

  @Test
  void readAheadReleasesOneContinuousPrefixAfterLocalBoundaryStorageWraps() throws Exception {
    var path = directory.resolve("read-ahead.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(512), Integer.BYTES);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      for (var sequence = 0; sequence < 24; sequence++) {
        commit(writer, integerBytes(sequence));
      }
      for (var sequence = 0; sequence < 20; sequence++) {
        assertEquals(sequence, reader.tryRead().orElseThrow().asReadOnlyByteBuffer().getInt());
      }
      reader.release(12);

      IpcQueue.WriteReceipt lastReceipt = null;
      for (var sequence = 24; sequence < 40; sequence++) {
        lastReceipt = commit(writer, integerBytes(sequence));
      }
      for (var sequence = 20; sequence < 40; sequence++) {
        assertEquals(sequence, reader.tryRead().orElseThrow().asReadOnlyByteBuffer().getInt());
      }
      reader.release(28);

      assertTrue(writer.isReleased(lastReceipt));
    }
  }

  @Test
  void unreadReleaseBoundaryReplaysAfterReaderReopens() throws Exception {
    var path = directory.resolve("replay.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells)) {
      commit(writer, new byte[] {7});
    }
    try (var reader = QueueFixtures.reader(path, bells)) {
      assertRecord(new byte[] {7}, reader.tryRead().orElseThrow());
    }
    try (var reader = QueueFixtures.reader(path, bells)) {
      assertRecord(new byte[] {7}, reader.tryRead().orElseThrow());
      reader.release(1);
    }
  }

  @Test
  void invalidReleaseCountDoesNotChangeQueueBytes() throws Exception {
    var path = directory.resolve("invalid-release.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      commit(writer, new byte[] {1});
      reader.tryRead().orElseThrow();
      var snapshot = Files.readAllBytes(path);

      assertThrows(IllegalArgumentException.class, () -> reader.release(2));
      assertArrayEquals(snapshot, Files.readAllBytes(path));
    }
  }

  @Test
  void queueCreationDoesNotReplaceAnExistingFile() throws Exception {
    var path = directory.resolve("existing.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var snapshot = Files.readAllBytes(path);

    assertThrows(
        FileAlreadyExistsException.class,
        () -> IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 8));

    assertArrayEquals(snapshot, Files.readAllBytes(path));
  }

  @Test
  void receiptCannotBeUsedAfterWriterReopens() throws Exception {
    var path = directory.resolve("receipt-owner.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    IpcQueue.WriteReceipt receipt;
    try (var writer = QueueFixtures.writer(path, bells)) {
      receipt = commit(writer, new byte[] {1});
    }
    try (var reopenedWriter = QueueFixtures.writer(path, bells)) {
      assertThrows(IllegalArgumentException.class, () -> reopenedWriter.isReleased(receipt));
      assertThrows(IllegalArgumentException.class, () -> reopenedWriter.waitReleased(receipt));
    }
  }

  @Test
  void blockedReaderIsWokenByMappedWriter() throws Exception {
    var path = directory.resolve("wait.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      var waiting = executor.submit(reader::waitReadable);
      commit(writer, new byte[] {9});
      assertEquals(IpcQueue.WaitResult.READY, waiting.get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void localInterrupterWakesBlockedReaderWithoutCreatingData() throws Exception {
    var path = directory.resolve("interrupt.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var reader = QueueFixtures.reader(path, bells)) {
      var waiting = executor.submit(reader::waitReadable);
      reader.interrupter().interrupt();

      assertEquals(IpcQueue.WaitResult.INTERRUPTED, waiting.get(1, TimeUnit.SECONDS));
      assertTrue(reader.tryRead().isEmpty());
    }
  }

  @Test
  void localInterrupterWakesBlockedWriterWithoutWritingData() throws Exception {
    var path = directory.resolve("writer-interrupt.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var writer = QueueFixtures.writer(path, bells)) {
      commit(writer, new byte[] {1});
      commit(writer, new byte[] {2});
      var snapshot = Files.readAllBytes(path);
      var waiting = executor.submit(() -> writer.waitWritable(1));
      awaitWriterArmed(path);

      writer.interrupter().interrupt();

      assertEquals(IpcQueue.WaitResult.INTERRUPTED, waiting.get(1, TimeUnit.SECONDS));
      assertArrayEquals(snapshot, Files.readAllBytes(path));
      assertEquals(IpcQueue.Full.INSTANCE, writer.tryWrite(new byte[] {3}));
    }
  }

  @Test
  void interruptionBeforeWaitIsConsumedOnceWithoutChangingQueueBytes() throws Exception {
    var path = directory.resolve("early-interrupt.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      var snapshot = Files.readAllBytes(path);
      reader.interrupter().interrupt();
      reader.interrupter().interrupt();

      assertEquals(IpcQueue.WaitResult.INTERRUPTED, reader.waitReadable());
      assertArrayEquals(snapshot, Files.readAllBytes(path));

      var nextWait = executor.submit(reader::waitReadable);
      awaitReaderArmed(path);
      commit(writer, new byte[] {1});
      assertEquals(IpcQueue.WaitResult.READY, nextWait.get(1, TimeUnit.SECONDS));
    }
  }

  @Test
  void receiptInspectionRejectsCorruptLivePositions() throws Exception {
    var path = directory.resolve("corrupt-receipt.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells)) {
      var receipt = commit(writer, new byte[] {1});
      writeLong(path, IpcQueueFormat.RELEASE_OFFSET, 24);

      var inspectionError =
          assertThrows(IpcQueueFormatException.class, () -> writer.isReleased(receipt));
      var waitError =
          assertThrows(IpcQueueFormatException.class, () -> writer.waitReleased(receipt));

      assertEquals("ipc.queue.release_ahead_of_commit", inspectionError.code());
      assertEquals("ipc.queue.release_ahead_of_commit", waitError.code());
      assertEquals(0, readInt(path, IpcQueueFormat.WRITER_BELL_SLOT_OFFSET));
    }
  }

  @Test
  void readerWaitRejectsCommitBehindItsLocalReadPosition() throws Exception {
    var path = directory.resolve("regressed-reader.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(32), 8);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      commit(writer, new byte[] {1});
      reader.tryRead().orElseThrow();
      writeLong(path, IpcQueueFormat.COMMIT_OFFSET, 0);

      var error = assertThrows(IpcQueueFormatException.class, reader::waitReadable);

      assertEquals("ipc.queue.commit_regressed", error.code());
      assertEquals(1, readInt(path, IpcQueueFormat.READER_BELL_SLOT_OFFSET));
    }
  }

  @Test
  void mappedWaitStressPreservesEveryRecordInOrder() throws Exception {
    var path = directory.resolve("stress.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), Integer.BYTES);
    var recordCount = 2_000;
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var executor = Executors.newVirtualThreadPerTaskExecutor();
        var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      var writes =
          executor.submit(
              () -> {
                for (var sequence = 0; sequence < recordCount; sequence++) {
                  var record = ByteBuffer.allocate(Integer.BYTES).putInt(sequence).array();
                  while (writer.tryWrite(record) == IpcQueue.Full.INSTANCE) {
                    assertEquals(IpcQueue.WaitResult.READY, writer.waitWritable(record.length));
                  }
                }
                return null;
              });
      var reads =
          executor.submit(
              () -> {
                for (var sequence = 0; sequence < recordCount; sequence++) {
                  var record = reader.tryRead();
                  while (record.isEmpty()) {
                    assertEquals(IpcQueue.WaitResult.READY, reader.waitReadable());
                    record = reader.tryRead();
                  }
                  assertEquals(sequence, record.orElseThrow().asReadOnlyByteBuffer().getInt());
                  reader.release(1);
                }
                return null;
              });

      writes.get(10, TimeUnit.SECONDS);
      reads.get(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void writerRejectsAPeerOrdinalItsRegionCannotHoldWithoutRepairingIt() throws Exception {
    var path = directory.resolve("corrupt-write.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells)) {
      writeInt(path, IpcQueueFormat.READER_BELL_SLOT_OFFSET, 2);

      var error = assertThrows(BellRegionException.class, () -> writer.tryWrite(new byte[] {1}));

      assertEquals("ipc.bell.slot_out_of_range", error.code());
      assertEquals(2, readInt(path, IpcQueueFormat.READER_BELL_SLOT_OFFSET));
    }
  }

  @Test
  void readerRejectsAPeerOrdinalItsRegionCannotHoldWithoutRepairingIt() throws Exception {
    var path = directory.resolve("corrupt-release.queue");
    IpcQueue.create(path, IpcQueueFormat.DataCapacity.of(64), 24);
    var bells = QueueFixtures.bellsFor(path, 2);

    try (var writer = QueueFixtures.writer(path, bells);
        var reader = QueueFixtures.reader(path, bells)) {
      commit(writer, new byte[] {1});
      reader.tryRead().orElseThrow();
      writeInt(path, IpcQueueFormat.WRITER_BELL_SLOT_OFFSET, 2);

      var error = assertThrows(BellRegionException.class, () -> reader.release(1));

      assertEquals("ipc.bell.slot_out_of_range", error.code());
      assertEquals(2, readInt(path, IpcQueueFormat.WRITER_BELL_SLOT_OFFSET));
    }
  }

  private static void writeInt(Path path, long offset, int value) throws Exception {
    var bytes = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
    bytes.flip();
    writeFully(path, offset, bytes);
  }

  private static void writeLong(Path path, long offset, long value) throws Exception {
    var bytes = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value);
    bytes.flip();
    writeFully(path, offset, bytes);
  }

  private static void writeFully(Path path, long offset, ByteBuffer bytes) throws Exception {
    try (var channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      var position = offset;
      while (bytes.hasRemaining()) {
        var written = channel.write(bytes, position);
        if (written <= 0) {
          throw new AssertionError("Mapped test write made no progress");
        }
        position += written;
      }
    }
  }

  private static IpcQueue.WriteReceipt commit(IpcQueue.Writer writer, byte[] record)
      throws IOException, IpcQueueFormatException {
    return assertInstanceOf(IpcQueue.Committed.class, writer.tryWrite(record)).receipt();
  }

  private static byte[] integerBytes(int value) {
    return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
  }

  private static void assertRecord(byte[] expected, ByteString actual) {
    assertArrayEquals(expected, actual.toByteArray());
  }

  private static int readInt(Path path, long offset) throws Exception {
    try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
      var bytes = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      var position = offset;
      while (bytes.hasRemaining()) {
        var read = channel.read(bytes, position);
        if (read <= 0) {
          throw new AssertionError("Mapped test read made no progress");
        }
        position += read;
      }
      bytes.flip();
      return bytes.getInt();
    }
  }

  private static void awaitReaderArmed(Path queue) throws Exception {
    waitUntil(() -> QueueFixtures.readerIsArmed(queue, QueueFixtures.bellsPathFor(queue)));
  }

  private static void awaitWriterArmed(Path queue) throws Exception {
    waitUntil(() -> QueueFixtures.writerIsArmed(queue, QueueFixtures.bellsPathFor(queue)));
  }

  private static void waitUntil(CheckedCondition condition) throws Exception {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.ready()) {
      if (System.nanoTime() >= deadline) {
        throw new AssertionError("Timed out waiting for a mapped doorbell fact");
      }
      Thread.onSpinWait();
    }
  }

  @FunctionalInterface
  private interface CheckedCondition {
    boolean ready() throws Exception;
  }
}
