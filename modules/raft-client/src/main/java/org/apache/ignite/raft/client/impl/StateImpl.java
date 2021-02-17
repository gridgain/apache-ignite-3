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
package org.apache.ignite.raft.client.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.State;

/**
 * Raft group state.
 */
public class StateImpl implements State {
    /** Mark a leaner peer */
    private static final String LEARNER_POSTFIX = "/learner";

    private final PeerId leader;

    private final List<PeerId> peers;

    private final List<PeerId> learners;

    /**
     * Construct a configuration instance with peers.
     *
     * @param conf configuration
     */
    public StateImpl(PeerId leader, final Iterable<PeerIdImpl> conf) {
        this(leader, conf, Collections.emptyList());
    }

    /**
     * Construct a Configuration instance with peers and learners.
     *
     * @param conf     peers configuration
     * @param learnConf learners
     * @since 1.3.0
     */
    public StateImpl(PeerId leader, final Iterable<PeerIdImpl> conf, final Iterable<PeerIdImpl> learnConf) {
        ArrayList<PeerIdImpl> p = new ArrayList<>();
        for (PeerIdImpl peerId : conf)
            p.add(peerId);

        ArrayList<PeerIdImpl> l = new ArrayList<>();
        for (PeerIdImpl learnerId : learnConf)
            l.add(learnerId);

        this.leader = leader;
        peers = Collections.unmodifiableList(p);
        learners = Collections.unmodifiableList(l);
    }

    @Override public PeerId getLeader() {
        return leader;
    }

    @Override public List<PeerId> getPeers() {
        return this.peers;
    }

    /**
     * Retrieve the learners set.
     *
     * @return learners
     */
    @Override public List<PeerId> getLearners() {
        return this.learners;
    }

    /**
     * Returns true when the configuration is valid.
     *
     * @return true if the configuration is valid.
     */
    public boolean isValid() {
        final Set<PeerId> intersection = new HashSet<>(this.peers);
        intersection.retainAll(this.learners);
        return !this.peers.isEmpty() && intersection.isEmpty();
    }

    public boolean isEmpty() {
        return this.peers.isEmpty();
    }

    /**
     * Returns the peers total number.
     *
     * @return total num of peers
     */
    public int size() {
        return this.peers.size();
    }

    @Override
    public Iterator<PeerId> iterator() {
        return this.peers.iterator();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.learners == null) ? 0 : this.learners.hashCode());
        result = prime * result + ((this.peers == null) ? 0 : this.peers.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        StateImpl other = (StateImpl) obj;
        if (this.learners == null) {
            if (other.learners != null) {
                return false;
            }
        } else if (!this.learners.equals(other.learners)) {
            return false;
        }
        if (this.peers == null) {
            return other.peers == null;
        } else {
            return this.peers.equals(other.peers);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        int i = 0;
        int size = peers.size();
        for (final PeerId peer : peers) {
            sb.append(peer);
            if (i < size - 1 || !this.learners.isEmpty()) {
                sb.append(",");
            }
            i++;
        }

        size = this.learners.size();
        i = 0;
        for (final PeerId peer : this.learners) {
            sb.append(peer).append(LEARNER_POSTFIX);
            if (i < size - 1) {
                sb.append(",");
            }
            i++;
        }

        return sb.toString();
    }
}
