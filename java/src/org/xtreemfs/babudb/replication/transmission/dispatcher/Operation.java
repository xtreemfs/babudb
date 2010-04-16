/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.transmission.dispatcher;

import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.foundation.oncrpc.client.RPCResponse;
import org.xtreemfs.foundation.oncrpc.client.RPCResponseAvailableListener;
import org.xtreemfs.foundation.oncrpc.server.ONCRPCRequest;

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
     * Parses the request.
     * @param rq the request
     * @return null if successful, error message otherwise
     */
    public abstract yidl.runtime.Object parseRPCMessage(Request rq);
    
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