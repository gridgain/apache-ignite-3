/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.replication.raft.confchange;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.internal.replication.raft.storage.ConfChange;
import org.apache.ignite.internal.replication.raft.storage.ConfChangeSingle;
import org.apache.ignite.internal.replication.raft.Inflights;
import org.apache.ignite.internal.replication.raft.InvalidConfigTransitionException;
import org.apache.ignite.internal.replication.raft.Progress;
import org.apache.ignite.internal.replication.raft.ProgressMap;
import org.apache.ignite.internal.replication.raft.TrackerConfig;
import org.apache.ignite.internal.replication.raft.quorum.JointConfig;

import static org.apache.ignite.internal.replication.raft.storage.ConfChange.ConfChangeTransition.ConfChangeTransitionAuto;

/**
 * Changer facilitates configuration changes. It exposes methods to handle
 * simple and joint consensus while performing the proper validation that allows
 * refusing invalid configuration changes before they affect the active
 * configuration.
 */
public class Changer {
    private final TrackerConfig trackerCfg;
    private final long lastIdx;
    private final int maxInflight;

    private ProgressMap progressMap;

    private Set<UUID> incoming;

    private Set<UUID> outgoing;

    private Set<UUID> learners;

    private Set<UUID> learnersNext;

    private boolean autoLeave;

    public Changer(TrackerConfig trackerCfg, ProgressMap originalProgress, long lastIdx, int maxInflight) {
        this.trackerCfg = trackerCfg;
        this.lastIdx = lastIdx;
        this.maxInflight = maxInflight;

        disassemble(trackerCfg, originalProgress);
    }

    // LeaveJoint is true if the configuration change leaves a joint configuration.
    // This is the case if the ConfChangeV2 is zero, with the possible exception of
    // the Context field.
    public static boolean leaveJoint(ConfChange cc) {
        // TODO agoncharuk cc.context(null) - why this is needed?
        return cc.changes().isEmpty();
    }

    // EnterJoint returns true if and only if this config change will use Joint Consensus,
    // which is the case if it contains more than one change or if the use of Joint Consensus
    // was requested explicitly.
    public static boolean enterJoint(ConfChange cc) {
        // NB: in theory, more config changes could qualify for the "simple"
        // protocol but it depends on the config on top of which the changes apply.
        // For example, adding two learners is not OK if both nodes are part of the
        // base config (i.e. two voters are turned into learners in the process of
        // applying the conf change). In practice, these distinctions should not
        // matter, so we keep it simple and use Joint Consensus liberally.
        if (cc.transition() != ConfChangeTransitionAuto || cc.changes().size() > 1) {
            // Use Joint Consensus.
            return true;
        }

        return false;
    }

    public static boolean autoLeave(ConfChange cc) {
        assert cc.transition() != ConfChangeTransitionAuto || cc.changes().size() > 1;

        switch (cc.transition()) {
            case ConfChangeTransitionAuto:
            case ConfChangeTransitionJointImplicit:
                return true;

            case ConfChangeTransitionJointExplicit:
                return false;

            default:
                throw new AssertionError("unknown transition: " + cc);
        }
    }

    // EnterJoint verifies that the outgoing (=right) majority config of the joint
    // config is empty and initializes it with a copy of the incoming (=left)
    // majority config. That is, it transitions from
    //
    //     (1 2 3) && ()
    // to
    //     (1 2 3) && (1 2 3).
    //
    // The supplied changes are then applied to the incoming majority config,
    // resulting in a joint configuration that in terms of the Raft thesis[1]
    // (Section 4.3) corresponds to `C_{new,old}`.
    //
    // [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    public RestoreResult enterJoint(boolean autoLeave, List<ConfChangeSingle> ccs) {
        if (isJoint())
            throw new InvalidConfigTransitionException("config is already joint");

        if (incoming.isEmpty()) {
            // We allow adding nodes to an empty config for convenience (testing and
            // bootstrap), but you can't enter a joint state.
            throw new InvalidConfigTransitionException("can't make a zero-voter config joint");
        }

        // Copy incoming to outgoing.
        outgoing = new HashSet<>(incoming);

        apply(ccs);

        this.autoLeave = autoLeave;

        return checkAndReturn();
    }

    // LeaveJoint transitions out of a joint configuration. It is an error to call
    // this method if the configuration is not joint, i.e. if the outgoing majority
    // config Voters[1] is empty.
    //
    // The outgoing majority config of the joint configuration will be removed,
    // that is, the incoming config is promoted as the sole decision maker. In the
    // notation of the Raft thesis[1] (Section 4.3), this method transitions from
    // `C_{new,old}` into `C_new`.
    //
    // At the same time, any staged learners (LearnersNext) the addition of which
    // was held back by an overlapping voter in the former outgoing config will be
    // inserted into Learners.
    //
    // [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    public RestoreResult leaveJoint() {
        if (!isJoint())
            throw new InvalidConfigTransitionException("can't leave a non-joint config");

        for (UUID id : learnersNext) {
            learners.add(id);

            progressMap.get(id).setLearner(true);
        }

        learnersNext = null;

        for (UUID id : outgoing) {
            boolean isVoter = incoming.contains(id);
            boolean isLearner = learners.contains(id);

            if (!isVoter && !isLearner)
                progressMap.remove(id);
        }

        outgoing = null;

        autoLeave = false;

        return checkAndReturn();
    }

    // Simple carries out a series of configuration changes that (in aggregate)
    // mutates the incoming majority config Voters[0] by at most one. This method
    // will return an error if that is not the case, if the resulting quorum is
    // zero, or if the configuration is in a joint state (i.e. if there is an
    // outgoing configuration).
    public RestoreResult simple(List<ConfChangeSingle> css) {
        if (isJoint())
            throw new InvalidConfigTransitionException("can't apply simple config change in joint config");

        apply(css);

        int n = symdiff(trackerCfg.voters().incoming(), incoming);

        if (n > 1)
            throw new InvalidConfigTransitionException("more than one voter changed without entering joint config");

        return checkAndReturn();
    }

    public long lastIndex() {
        return lastIdx;
    }

    public int maxInflight() {
        return maxInflight;
    }

    // apply a change to the configuration. By convention, changes to voters are
    // always made to the incoming majority config Voters[0]. Voters[1] is either
    // empty or preserves the outgoing majority configuration while in a joint state.
    private void apply(List<ConfChangeSingle> ccs) {
        for (ConfChangeSingle cc : ccs) {
            if (cc.nodeId() == null) {
                // etcd replaces the NodeID with zero if it decides (downstream of
                // raft) to not apply a change, so we have to have explicit code
                // here to ignore these.
                continue;
            }

            switch (cc.type()) {
                case ConfChangeAddNode: {
                    makeVoter(cc.nodeId());

                    break;
                }

                case ConfChangeAddLearnerNode: {
                    makeLearner(cc.nodeId());

                    break;
                }

                case ConfChangeRemoveNode: {
                    remove(cc.nodeId());

                    break;
                }

                case ConfChangeUpdateNode: {
                    break;
                }

                default:
                    throw new InvalidConfigTransitionException("unexpected conf type " + cc.type());
            }
        }

        if (incoming.isEmpty())
            throw new InvalidConfigTransitionException("removed all voters");
    }

    // makeVoter adds or promotes the given ID to be a voter in the incoming
    // majority config.
    private void makeVoter(UUID id) {
        Progress pr = progressMap.get(id);

        if (pr == null) {
            initProgress(id, false /* isLearner */);

            return;
        }

        pr.setLearner(false);

        learners.remove(id);
        learnersNext.remove(id);
        incoming.add(id);

        return;
    }

    // makeLearner makes the given ID a learner or stages it to be a learner once
    // an active joint configuration is exited.
    //
    // The former happens when the peer is not a part of the outgoing config, in
    // which case we either add a new learner or demote a voter in the incoming
    // config.
    //
    // The latter case occurs when the configuration is joint and the peer is a
    // voter in the outgoing config. In that case, we do not want to add the peer
    // as a learner because then we'd have to track a peer as a voter and learner
    // simultaneously. Instead, we add the learner to LearnersNext, so that it will
    // be added to Learners the moment the outgoing config is removed by
    // LeaveJoint().
    private void makeLearner(UUID id) {
        Progress pr = progressMap.get(id);

        if (pr == null) {
            initProgress(id, true /* isLearner */);

            return;
        }

        if (pr.isLearner())
            return;

        // Remove any existing voter in the incoming config...
        remove(id);

        // ... but save the Progress.
        progressMap.put(id, pr);

        // Use LearnersNext if we can't add the learner to Learners directly, i.e.
        // if the peer is still tracked as a voter in the outgoing config. It will
        // be turned into a learner in LeaveJoint().
        //
        // Otherwise, add a regular learner right away.
        boolean onRight = outgoing.contains(id);

        if (onRight)
            learnersNext.add(id);
        else {
            pr.setLearner(true);

            learners.add(id);
        }
    }

    // remove this peer as a voter or learner from the incoming config.
    private void remove(UUID id) {
        if (!progressMap.containsKey(id))
            return;

        incoming.remove(id);

        learners.remove(id);
        learnersNext.remove(id);

        // If the peer is still a voter in the outgoing config, keep the Progress.
        if (!outgoing.contains(id))
            progressMap.remove(id);
    }

    // initProgress initializes a new progress for the given node or learner.
    private void initProgress(UUID id, boolean isLearner) {
        if (!isLearner)
            incoming.add(id);
        else
            learners.add(id);

        progressMap.put(id,
            // Initializing the Progress with the last index means that the follower
            // can be probed (with the last index).
            //
            // TODO(tbg): seems awfully optimistic. Using the first index would be
            // better. The general expectation here is that the follower has no log
            // at all (and will thus likely need a snapshot), though the app may
            // have applied a snapshot out of band before adding the replica (thus
            // making the first index the better choice).
            new Progress(
                0,
                lastIdx,
                new Inflights(maxInflight),
                isLearner,
                // When a node is first added, we should mark it as recently active.
                // Otherwise, CheckQuorum may cause us to step down if it is invoked
                // before the added node has had a chance to communicate with us.
                true
            ));
    }

    // checkInvariants makes sure that the config and progress are compatible with
    // each other. This is used to check both what the Changer is initialized with,
    // as well as what it returns.
    private void checkInvariants() {
        // NB: intentionally allow the empty config. In production we'll never see a
        // non-empty config (we prevent it from being created) but we will need to
        // be able to *create* an initial config, for example during bootstrap (or
        // during tests). Instead of having to hand-code this, we allow
        // transitioning from an empty config into any other legal and non-empty
        // config.
        Stream<UUID> allIds = Stream.concat(
            Stream.concat(incoming.stream(), outgoing.stream()),
            Stream.concat(learners.stream(), learnersNext.stream())
        );

        Optional<UUID> missed = allIds.filter(id -> !progressMap.containsKey(id)).findAny();

        if (!missed.isEmpty())
            throw new InvalidConfigTransitionException("no progress for " + missed.get());

        // Any staged learner was staged because it could not be directly added due
        // to a conflicting voter in the outgoing config.
        for (UUID id : learnersNext) {
            if (!outgoing.contains(id))
                throw new InvalidConfigTransitionException(id + " is in learnersNext, but not outgoing");

            if (progressMap.get(id).isLearner())
                throw new InvalidConfigTransitionException(id + " is in learnersNext, but is already marked as learner");
        }

        // Conversely Learners and Voters doesn't intersect at all.
        for (UUID id : learners) {
            if (outgoing.contains(id))
                throw new InvalidConfigTransitionException(id + " is in learners and outgoing");

            if (incoming.contains(id))
                throw new InvalidConfigTransitionException(id + " is in learners and incoming");

            if (!progressMap.get(id).isLearner())
                throw new InvalidConfigTransitionException(id + " is in learners, but is not marked as learner");
        }

        if (!isJoint()) {
            if (autoLeave)
                throw new InvalidConfigTransitionException("autoLeave must be false when not joint");
        }
    }

    // checkAndCopy copies the tracker's config and progress map (deeply enough for
    // the purposes of the Changer) and returns those copies. It returns an error
    // if checkInvariants does.
    private void disassemble(TrackerConfig trackerCfg, ProgressMap originalProgress)  {
        incoming = nullCopy(trackerCfg.voters().incoming());
        outgoing = nullCopy(trackerCfg.voters().outgoing());
        learners = nullCopy(trackerCfg.learners());
        learnersNext = nullCopy(trackerCfg.learnersNext());

        progressMap = new ProgressMap();

        for (Map.Entry<UUID, Progress> entry : originalProgress.entrySet()) {
            Progress p = entry.getValue();

            // A shallow copy is enough because we only mutate the Learner field.
            progressMap.put(entry.getKey(), new Progress(
                p.match(),
                p.next(),
                p.inflights(),
                p.isLearner(),
                p.recentActive()
            ));
        }

        checkInvariants();
    }

    private RestoreResult checkAndReturn() {
        return new RestoreResult(
            new TrackerConfig(
                JointConfig.of(incoming, outgoing),
                learners,
                learnersNext,
                autoLeave
            ),
            progressMap
        );
    }

    private boolean isJoint() {
        return !outgoing.isEmpty();
    }

    private Set<UUID> nullCopy(Set<UUID> src) {
        return src == null ? new HashSet<>() : new HashSet<>(src);
    }
    // symdiff returns the cardinality of the symmetric difference between the sets of
    // UUIDs, i.e. len((l - r) \union (r - l)).

    private int symdiff(Set<UUID> l, Set<UUID> r) {
        int n = 0;

        Set<UUID>[][] pairs = new Set[][] {
            {l, r},
            {r, l}
        };

        for (Set<UUID>[] p : pairs) {
            for (UUID id : p[0]) {
                if (!p[1].contains(id))
                    n++;
            }
        }

        return n;
    }
}
