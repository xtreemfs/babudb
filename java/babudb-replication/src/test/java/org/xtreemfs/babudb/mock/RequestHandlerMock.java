/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import java.util.Map;

import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;

/**
 * Mock for request handler logic.
 * 
 * @author flangner
 * @since 02/25/2011
 */
public class RequestHandlerMock extends RequestHandler {

    private final int interfaceID;
    
    public RequestHandlerMock(int maxQ, int interfaceID, Map<Integer, Operation> ops) {
        super(maxQ);
        this.interfaceID = interfaceID;
        this.operations.putAll(ops);
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler#
     *              getInterfaceID()
     */
    @Override
    public int getInterfaceID() {
        return interfaceID;
    }
}
