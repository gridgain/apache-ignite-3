/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: local_storage.proto

package com.alipay.sofa.jraft.entity;

import com.alipay.sofa.jraft.rpc.Message;
import com.alipay.sofa.jraft.storage.RaftMetaStorage;
import com.alipay.sofa.jraft.util.DisruptorBuilder;
import java.nio.ByteBuffer;

public final class LocalStorageOutter {
    public interface ConfigurationPB {
        java.util.List<java.lang.String> getPeersList();

        int getPeersCount();

        java.lang.String getPeers(int index);

        java.util.List<java.lang.String> getOldPeersList();

        int getOldPeersCount();

        java.lang.String getOldPeers(int index);
    }

    public interface LogPBMeta {
        long getFirstLogIndex();
    }

    public interface StablePBMeta extends Message {
        static Builder newBuilder() {
            return null;
        }

        long getTerm();

        java.lang.String getVotedfor();

        interface Builder {

            Builder setTerm(long term);

            Builder setVotedfor(String votedFor);

            StablePBMeta build();
        }
    }

    public interface LocalSnapshotPbMeta extends Message {
        static Builder newBuilder() {
            return null;
        }

        static LocalSnapshotPbMeta parseFrom(ByteBuffer buf) {
            throw new UnsupportedOperationException();
        }

        com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta getMeta();

        java.util.List<com.alipay.sofa.jraft.entity.LocalStorageOutter.LocalSnapshotPbMeta.File> getFilesList();

        int getFilesCount();

        com.alipay.sofa.jraft.entity.LocalStorageOutter.LocalSnapshotPbMeta.File getFiles(int index);

        byte[] toByteArray();

        boolean hasMeta();

        interface File {
            static Builder newBuilder() {
                return null;
            }

            java.lang.String getName();

            LocalFileMetaOutter.LocalFileMeta getMeta();

            public interface Builder {
                Builder setName(String key);

                Builder setMeta(LocalFileMetaOutter.LocalFileMeta meta);

                File build();
            }
        }

        public interface Builder {
            Builder setMeta(RaftOutter.SnapshotMeta meta);

            Builder addFiles(File file);

            LocalSnapshotPbMeta build();
        }
    }
}
