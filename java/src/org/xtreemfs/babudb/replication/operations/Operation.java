/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.operations;

import java.util.concurrent.atomic.AtomicInteger;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponseAvailableListener;
import org.xtreemfs.include.foundation.oncrpc.server.ONCRPCRequest;
import org.xtreemfs.babudb.interfaces.utils.Serializable;
import org.xtreemfs.babudb.replication.Request;

/**
 * Super class for operations triggered by external {@link ONCRPCRequest}s.
 * 
 * @since 05/02/2009
 * @author flangner
 */

public abstract class Operation {

    /**
     * @return the unique operationId.
     */
    public abstract int getProcedureId();
    
    /**
     * @return true, if this operation can be disabled, false otherwise. 
     */
    public abstract boolean canBeDisabled();

    /**
     * Parses the request.
     * @param rq the request
     * @return null if successful, error message otherwise
     */
    public abstract Serializable parseRPCMessage(Request rq);
    
    /**
     * called after request was parsed and operation assigned.
     * @param rq the new request
     */
    public abstract void startRequest(Request rq);

    public abstract void startInternalEvent(Object[] args);

    /**
     * Wait for responses of a broadcast-request.
     * 
     * @param responses
     * @param listener
     */
    @SuppressWarnings("unchecked")
    public void waitForResponses(final RPCResponse[] responses, final ResponsesListener listener) {

        assert(responses.length > 0);

        final AtomicInteger count = new AtomicInteger(0);
        final RPCResponseAvailableListener l = new RPCResponseAvailableListener() {

            @Override
            public void responseAvailable(RPCResponse r) {
                if (count.incrementAndGet() == responses.length) {
                    listener.responsesAvailable();
                }
            }
        };

        for (RPCResponse r : responses) {
            r.registerListener(l);
        }
    }

    public static interface ResponsesListener {
        public void responsesAvailable();
    }
}