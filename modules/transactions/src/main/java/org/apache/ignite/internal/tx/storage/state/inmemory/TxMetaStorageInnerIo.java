/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.tx.storage.state.inmemory;

import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.lang.IgniteInternalCheckedException;

public class TxMetaStorageInnerIo extends BplusInnerIo<TxMetaRowWrapper> {
    /** Page IO type. */
    public static final short T_TX_STORAGE_INNER_IO = 10002;

    /** I/O versions. */
    public static final IoVersions<TxMetaStorageInnerIo> VERSIONS = new IoVersions<>(new TxMetaStorageInnerIo(1));

    /**
     * Constructor.
     *
     * @param ver       Page format version.
     */
    protected TxMetaStorageInnerIo(int ver) {
        super(T_TX_STORAGE_INNER_IO, ver, true, Long.BYTES * 2);
    }

    @Override public void store(long dstPageAddr, int dstIdx, BplusIo<TxMetaRowWrapper> srcIo, long srcPageAddr,
        int srcIdx) throws IgniteInternalCheckedException {

    }

    @Override
    public void storeByOffset(long pageAddr, int off, TxMetaRowWrapper row) throws IgniteInternalCheckedException {

    }

    @Override public TxMetaRowWrapper getLookupRow(BplusTree<TxMetaRowWrapper, ?> tree, long pageAddr,
        int idx) throws IgniteInternalCheckedException {
        return null;
    }
}
