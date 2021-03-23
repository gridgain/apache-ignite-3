package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

public interface RemovePeersRequest {
    String getGroupId();

    List<Peer> getPeersList();

    interface Builder {
        Builder setGroupId(String groupId);

        Builder addPeer(Peer peer);

        RemovePeersRequest build();
    }
}
