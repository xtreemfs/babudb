/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.proxy.operations.GetDatabaseOperation;
import org.xtreemfs.babudb.replication.proxy.operations.GetDatabasesOperation;
import org.xtreemfs.babudb.replication.proxy.operations.LookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.MakePersistantOperation;
import org.xtreemfs.babudb.replication.proxy.operations.PrefixLookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.PrefixLookupReverseOperation;
import org.xtreemfs.babudb.replication.proxy.operations.RangeLookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.RangeLookupReverseOperation;
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
     * 
     * @param verificator
     * @param dbs - interface for local BabuDB operations.
     */
    public RPCRequestHandler(BabuDBInterface dbs) {
                
        Operation op = new MakePersistantOperation(dbs); 
        operations.put(op.getProcedureId(), op);
        
        op = new GetDatabaseOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new GetDatabasesOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new LookupOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new PrefixLookupOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new PrefixLookupReverseOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new RangeLookupOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new RangeLookupReverseOperation(dbs);
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
