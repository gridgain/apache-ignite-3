package org.apache.ignite.internal.tx.message;

import java.util.UUID;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.annotations.Marshallable;
import org.apache.ignite.network.annotations.Transferable;

@Transferable(TxMessageGroup.TX_STATE_REQUEST)
public interface TxStateReplicaRequest extends ReplicaRequest {
    @Marshallable
    UUID txId();

    HybridTimestamp commitTimestamp();

    @Marshallable
    NetworkAddress address();
}
