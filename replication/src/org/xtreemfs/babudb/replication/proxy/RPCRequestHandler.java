/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.xtreemfs.babudb.pbrpc.RemoteAccessServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.proxy.operations.GetDatabaseByIdOperation;
import org.xtreemfs.babudb.replication.proxy.operations.GetDatabaseByNameOperation;
import org.xtreemfs.babudb.replication.proxy.operations.GetDatabasesOperation;
import org.xtreemfs.babudb.replication.proxy.operations.LookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.MakePersistantOperation;
import org.xtreemfs.babudb.replication.proxy.operations.PrefixLookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.PrefixLookupReverseOperation;
import org.xtreemfs.babudb.replication.proxy.operations.RangeLookupOperation;
import org.xtreemfs.babudb.replication.proxy.operations.RangeLookupReverseOperation;
import org.xtreemfs.babudb.replication.transmission.RequestControl;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Request;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;
import org.xtreemfs.foundation.pbrpc.generatedinterfaces.RPC.ErrorType;

/**
 * Organizes the logic to dispatch requests matching the RemoteAccessInterface logically. Also 
 * capable to queue requests for a short time period (eg. failover).
 * 
 * @author flangner
 * @since 01/19/2011
 */
public class RPCRequestHandler extends RequestHandler implements RequestControl {
    
    private final Queue<AssignedRequest> queue = new PriorityQueue<AssignedRequest>();
    
    private final int MAX_Q;
    
    /**
     * @param maxQ
     * @param dbs - interface for local BabuDB operations.
     */
    public RPCRequestHandler(BabuDBInterface dbs, int maxQ) {
        
        MAX_Q = maxQ;
        
        Operation op = new MakePersistantOperation(dbs); 
        operations.put(op.getProcedureId(), op);
        
        op = new GetDatabaseByNameOperation(dbs);
        operations.put(op.getProcedureId(), op);
        
        op = new GetDatabaseByIdOperation(dbs);
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
        
        // enable message queuing
        enableQueuing();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.RequestControl#enableQueuing()
     */
    @Override
    public void enableQueuing() {
        synchronized (queuingEnabled) {
            queuingEnabled.set(true);  
        }  
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler#queue(int, org.xtreemfs.babudb.replication.transmission.dispatcher.Request)
     */
    @Override
    public void queue(int operationId, Request rq) {
        
        // ensure that the queue does not growth beyond the MAX_Q limit by rejecting the oldest 
        // request queued
        if (queue.size() == MAX_Q) {
            queue.poll().rq.sendError(ErrorType.INTERNAL_SERVER_ERROR, 
                    "Replication setup could not have been stabilized and " +
                    "servers run out of buffer.");
        }
        
        queue.add(new AssignedRequest(operationId, rq));
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.RequestControl#processQueue()
     */
    @Override
    public void processQueue() {
        
        List<AssignedRequest> todo;
        synchronized (queuingEnabled) {
            queuingEnabled.set(false);
            todo = new ArrayList<AssignedRequest>(queue);
            queue.clear();
        }
        
        for (AssignedRequest rq : todo) {
            operations.get(rq.operationID).startRequest(rq.rq);
        }
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler#getInterfaceID()
     */
    @Override
    public int getInterfaceID() {
        return RemoteAccessServiceConstants.INTERFACE_ID;
    }

    /**
     * Class for requests that have been already assigned to an operation.
     * 
     * @author flangner
     * @since 04/05/2011
     */
    private final static class AssignedRequest {
        
        private final int       operationID;
        private final Request   rq;
        
        private AssignedRequest(int opId, Request rq) {
            this.operationID = opId;
            this.rq = rq;
        }
    }
}
