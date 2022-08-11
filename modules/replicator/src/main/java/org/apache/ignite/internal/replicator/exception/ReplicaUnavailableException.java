package org.apache.ignite.internal.replicator.exception;

import java.util.UUID;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * The exception is thrown when the replica is not ready to handle a request.
 */
public class ReplicaUnavailableException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param groupId Replication group id.
     * @param nodeId Node's id.
     */
    public ReplicaUnavailableException(String groupId, String nodeId) {
        super(Replicator.REPLICA_UNAVAILABLE_ERR,
                IgniteStringFormatter.format("Replica is not ready [replicaGrpId={}, nodeId={}]", groupId, nodeId));
    }

    /**
     * The constructor is used for create an exception instance that has thrown in remote server.
     *
     * @param traceId Trace id.
     * @param code Error code.
     * @param message Error message.
     * @param cause Cause exception.
     */
    public ReplicaUnavailableException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
