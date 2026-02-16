/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft.storage.segstore;

import static org.apache.ignite.internal.raft.storage.segstore.FileSystemUtils.writeFully;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.HEADER_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayload.CRC_SIZE_BYTES;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayloadParser.endOfSegmentReached;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentPayloadParser.validateSegmentFileHeader;
import static org.apache.ignite.internal.util.IgniteUtils.atomicMoveFile;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.util.VarlenEncoder;
import org.jetbrains.annotations.VisibleForTesting;

class RaftLogGarbageCollector {
    private static final IgniteLogger LOG = Loggers.forClass(RaftLogGarbageCollector.class);

    private static final String TMP_FILE_SUFFIX = ".tmp";

    private final IndexFileManager indexFileManager;

    private final AtomicLong logSize = new AtomicLong();

    RaftLogGarbageCollector(IndexFileManager indexFileManager) {
        this.indexFileManager = indexFileManager;
    }

    @VisibleForTesting
    void compactSegmentFile(SegmentFile segmentFile) throws IOException {
        // Cache for avoiding excessive min/max log index computations.
        var logStorageInfos = new Long2ObjectOpenHashMap<GroupIndexInfo>();

        ByteBuffer buffer = segmentFile.buffer();

        validateSegmentFileHeader(buffer, segmentFile.path());

        TmpSegmentFile tmpSegmentFile = null;

        WriteModeIndexMemTable tmpMemTable = null;

        try {
            while (!endOfSegmentReached(buffer)) {
                int startOfRecordOffset = buffer.position();

                long groupId = buffer.getLong();

                int payloadLength = buffer.getInt();

                if (payloadLength <= 0) {
                    // Skip special entries (such as truncation records). They can always be omitted.
                    int endOfRecordOffset = buffer.position() + Long.BYTES + CRC_SIZE_BYTES;

                    buffer.position(endOfRecordOffset);

                    continue;
                }

                int endOfRecordOffset = buffer.position() + payloadLength + CRC_SIZE_BYTES;

                long index = VarlenEncoder.readLong(buffer);

                GroupIndexInfo info = logStorageInfos.computeIfAbsent(groupId, GroupIndexInfo::new);

                if (index < info.firstLogIndexInclusive() || index >= info.lastLogIndexExclusive()) {
                    // We found a truncated entry, it should be skipped.
                    buffer.position(endOfRecordOffset);

                    continue;
                }

                if (tmpSegmentFile == null) {
                    tmpSegmentFile = new TmpSegmentFile(segmentFile);

                    tmpSegmentFile.writeHeader();

                    tmpMemTable = new SingleThreadMemTable();
                }

                int oldLimit = buffer.limit();

                // Set the buffer boundaries to only write the current record to the new file.
                buffer.position(startOfRecordOffset).limit(endOfRecordOffset);

                writeFully(tmpSegmentFile.fileChannel(), buffer);

                buffer.limit(oldLimit);

                tmpMemTable.appendSegmentFileOffset(groupId, index, startOfRecordOffset);
            }

            long logSizeDelta;

            if (tmpSegmentFile != null) {
                tmpSegmentFile.syncAndRename();

                indexFileManager.createIndexFile(tmpMemTable.transitionToReadMode(), tmpSegmentFile.fileProperties());

                logSizeDelta = Files.size(segmentFile.path()) - tmpSegmentFile.size();
            } else {
                // We got lucky and the whole file can be removed.
                logSizeDelta = Files.size(segmentFile.path());
            }

            // Remove the previous generation of the segment file and its index. This is safe to do, because index state is fully in-memory
            // (we never read from index file apart from recovery) and we rely on the file system guarantees that other threads reading
            // from the segment file will still be able to do that even if the file is deleted.
            Files.delete(segmentFile.path());
            Files.delete(indexFileManager.indexFilePath(segmentFile.fileProperties()));

            logSize.addAndGet(-logSizeDelta);
        } finally {
            if (tmpSegmentFile != null) {
                tmpSegmentFile.close();
            }
        }
    }

    private class GroupIndexInfo {
        private final long firstLogIndexInclusive;

        private final long lastLogIndexExclusive;

        GroupIndexInfo(long groupId) {
            this.firstLogIndexInclusive = indexFileManager.firstLogIndexInclusive(groupId);
            this.lastLogIndexExclusive = indexFileManager.lastLogIndexExclusive(groupId);
        }

        long firstLogIndexInclusive() {
            return firstLogIndexInclusive;
        }

        long lastLogIndexExclusive() {
            return lastLogIndexExclusive;
        }
    }

    private static class TmpSegmentFile implements ManuallyCloseable {
        private final String fileName;

        private final Path tmpFilePath;

        private final FileChannel fileChannel;

        private final FileProperties fileProperties;

        TmpSegmentFile(SegmentFile originalFile) throws IOException {
            FileProperties originalFileProperties = originalFile.fileProperties();

            this.fileProperties = new FileProperties(originalFileProperties.ordinal(), originalFileProperties.generation() + 1);
            this.fileName = SegmentFile.fileName(fileProperties);
            this.tmpFilePath = originalFile.path().resolveSibling(fileName + TMP_FILE_SUFFIX);
            this.fileChannel = FileChannel.open(tmpFilePath, StandardOpenOption.WRITE);
        }

        void writeHeader() throws IOException {
            fileChannel.write(ByteBuffer.wrap(HEADER_RECORD));
        }

        FileChannel fileChannel() {
            return fileChannel;
        }

        void syncAndRename() throws IOException {
            fileChannel.force(true);

            atomicMoveFile(tmpFilePath, tmpFilePath.resolveSibling(fileName), LOG);
        }

        long size() throws IOException {
            return fileChannel.size();
        }

        FileProperties fileProperties() {
            return fileProperties;
        }

        @Override
        public void close() throws IOException {
            fileChannel.close();
        }
    }
}
