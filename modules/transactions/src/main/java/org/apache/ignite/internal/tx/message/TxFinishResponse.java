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

package org.apache.ignite.internal.tx.message;

import java.io.Serializable;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * The result of an action.
 */
@Transferable(value = TxMessageGroup.TX_FINISH_RESPONSE)
// TODO: sanpwc remove.
public interface TxFinishResponse extends NetworkMessage, Serializable {
    /**
     * Returns the error message.
     *
     * @return Finish error message or null if finished normally.
     */
    @Nullable String errorMessage();
}
