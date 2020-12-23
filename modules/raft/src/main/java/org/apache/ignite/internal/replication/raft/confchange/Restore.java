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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.replication.raft.ConfChangeSingle;
import org.apache.ignite.internal.replication.raft.ConfigState;

/**
 *
 */
public class Restore {
    // toConfChangeSingle translates a conf state into a) a list of operations creating
    // first the config that will become the outgoing one, and then the incoming one, and
    // b) another list that, when applied to the config resulted from a), represents the
    // ConfigState.
    private static List<ConfChangeSingle>[] toConfChangeSingle(ConfigState cs) {
        // Example to follow along this code:
        // voters=(1 2 3) learners=(5) outgoing=(1 2 4 6) learners_next=(4)
        //
        // This means that before entering the joint config, the configuration
        // had voters (1 2 4 6) and perhaps some learners that are already gone.
        // The new set of voters is (1 2 3), i.e. (1 2) were kept around, and (4 6)
        // are no longer voters; however 4 is poised to become a learner upon leaving
        // the joint state.
        // We can't tell whether 5 was a learner before entering the joint config,
        // but it doesn't matter (we'll pretend that it wasn't).
        //
        // The code below will construct
        // outgoing = add 1; add 2; add 4; add 6
        // incoming = remove 1; remove 2; remove 4; remove 6
        //            add 1;    add 2;    add 3;
        //            add-learner 5;
        //            add-learner 4;
        //
        // So, when starting with an empty config, after applying 'outgoing' we have
        //
        //   quorum=(1 2 4 6)
        //
        // From which we enter a joint state via 'incoming'
        //
        //   quorum=(1 2 3)&&(1 2 4 6) learners=(5) learners_next=(4)
        //
        // as desired.

        List<ConfChangeSingle> out = new ArrayList<>(cs.outgoing().size());

        for (UUID id : cs.outgoing()) {
            // If there are outgoing voters, first add them one by one so that the
            // (non-joint) config has them all.
            out.add(new ConfChangeSingle(id, ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

        }

        // We're done constructing the outgoing slice, now on to the incoming one
        // (which will apply on top of the config created by the outgoing slice).
        List<ConfChangeSingle> in = new ArrayList<>(cs.outgoing().size() + cs.voters().size() + cs.learners().size() +
            cs.learnersNext().size());

        // First, we'll remove all of the outgoing voters.
        for (UUID id : cs.outgoing())
            in.add(new ConfChangeSingle(id, ConfChangeSingle.ConfChangeType.ConfChangeRemoveNode));

        // Then we'll add the incoming voters and learners.
        for (UUID id : cs.voters())
            in.add(new ConfChangeSingle(id, ConfChangeSingle.ConfChangeType.ConfChangeAddNode));

        for (UUID id : cs.learners())
            in.add(new ConfChangeSingle(id, ConfChangeSingle.ConfChangeType.ConfChangeAddLearnerNode));

        // Same for LearnersNext; these are nodes we want to be learners but which
        // are currently voters in the outgoing config.
        for (UUID id : cs.learnersNext()) {
            in.add(new ConfChangeSingle(id, ConfChangeSingle.ConfChangeType.ConfChangeAddLearnerNode));
        }

        return new List[] {out, in};
    }

    private static RestoreResult chain(Changer chg, List<ChangeOp> ops) {

        RestoreResult res = null;

        for (ChangeOp op : ops) {
            res = op.apply(chg);

            chg = new Changer(res.trackerConfig(), res.progressMap(), chg.lastIndex(), chg.maxInflight());
        }

        return res;
    }

    // Restore takes a Changer (which must represent an empty configuration), and
    // runs a sequence of changes enacting the configuration described in the
    // ConfState.
    //
    // TODO(tbg) it's silly that this takes a Changer. Unravel this by making sure
    // the Changer only needs a ProgressMap (not a whole Tracker) at which point
    // this can just take LastIndex and MaxInflight directly instead and cook up
    // the results from that alone.
    public static RestoreResult restore(Changer chg, ConfigState cs) {
        List<ConfChangeSingle>[] outIn = toConfChangeSingle(cs);

        List<ConfChangeSingle> outgoing = outIn[0];
        List<ConfChangeSingle> incoming = outIn[1];

        List<ChangeOp> ops = new ArrayList<>();

        if (outgoing.isEmpty()) {
            // No outgoing config, so just apply the incoming changes one by one.
            for (ConfChangeSingle cc :  incoming) {
                ops.add(new ChangeOp() {
                    @Override public RestoreResult apply(Changer chg) {
                        return chg.simple(Collections.singletonList(cc));
                    }
                });
            }
        }
        else {
            // The ConfState describes a joint configuration.
            //
            // First, apply all of the changes of the outgoing config one by one, so
            // that it temporarily becomes the incoming active config. For example,
            // if the config is (1 2 3) & (2 3 4), this will establish (2 3 4)&().
            for (ConfChangeSingle cc : outgoing) {
                ops.add(new ChangeOp() {
                    @Override public RestoreResult apply(Changer chg) {
                        return chg.simple(Collections.singletonList(cc));
                    }
                });
            }

            // Now enter the joint state, which rotates the above additions into the
            // outgoing config, and adds the incoming config in. Continuing the
            // example above, we'd get (1 2 3)&(2 3 4), i.e. the incoming operations
            // would be removing 2,3,4 and then adding in 1,2,3 while transitioning
            // into a joint state.
            ops.add(new ChangeOp() {
                @Override public RestoreResult apply(Changer chg) {
                    return chg.enterJoint(cs.autoLeave(), incoming);
                }
            });
        }

        return chain(chg, ops);
    }

    private static interface ChangeOp {
        RestoreResult apply(Changer chg);
    }
}
