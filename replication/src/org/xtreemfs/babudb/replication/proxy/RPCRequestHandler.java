/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.proxy.operations.MakePersistantOperation;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsVerification;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;

/**
 * Object to dispatch requests matching the RemoteAccessInterface logically.
 * 
 * @author flangner
 * @since 01/19/2011
 */
public class RPCRequestHandler extends RequestHandler {
    
    /**
     * @param verificator
     */
    public RPCRequestHandler(ParticipantsVerification verificator,
            PersistenceManager localPersMan) {
        super(verificator);
        
        // setup the operations
        Operation op = new MakePersistantOperation(localPersMan);
        operations.put(op.getProcedureId(), op);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler#getInterfaceID()
     */
    @Override
    public int getInterfaceID() {
        return RemoteAccessServiceConstants.INTERFACE_ID;
    }

}
