/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.pbrpc.ReplicationServiceConstants;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.TopLayer;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates;
import org.xtreemfs.babudb.replication.service.operations.ChunkOperation;
import org.xtreemfs.babudb.replication.service.operations.FleaseOperation;
import org.xtreemfs.babudb.replication.service.operations.HeartbeatOperation;
import org.xtreemfs.babudb.replication.service.operations.LoadOperation;
import org.xtreemfs.babudb.replication.service.operations.LocalTimeOperation;
import org.xtreemfs.babudb.replication.service.operations.ReplicaOperation;
import org.xtreemfs.babudb.replication.service.operations.ReplicateOperation;
import org.xtreemfs.babudb.replication.service.operations.StateOperation;
import org.xtreemfs.babudb.replication.service.operations.SynchronizeOperation;
import org.xtreemfs.babudb.replication.service.operations.VolatileStateOperation;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.babudb.replication.transmission.dispatcher.Operation;
import org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler;

/**
 * Object to dispatch requests matching the ReplicationInterface logically.
 * 
 * @author flangner
 * @since 01/19/2011
 */
public class ReplicationRequestHandler extends RequestHandler {
    
    public <T extends TopLayer & FleaseMessageReceiver> ReplicationRequestHandler(
            ParticipantsStates pStates, T fleaseReceiver, BabuDBInterface babuDBI,
            ReplicationStage replStage, AtomicReference<LSN> lastOnView, int maxChunkSize, 
            FileIOInterface fileIO, int maxQ) {
        
        super(maxQ);
        
        Operation op = new LocalTimeOperation();
        operations.put(op.getProcedureId(), op);
        
        op = new FleaseOperation(fleaseReceiver);
        operations.put(op.getProcedureId(), op);
        
        op = new StateOperation(babuDBI, fleaseReceiver);
        operations.put(op.getProcedureId(), op);
        
        op = new VolatileStateOperation(babuDBI);
        operations.put(op.getProcedureId(), op);
        
        op = new HeartbeatOperation(pStates);
        operations.put(op.getProcedureId(), op);
        
        op = new SynchronizeOperation(replStage);
        operations.put(op.getProcedureId(),op);
        
        op = new ReplicateOperation(replStage);
        operations.put(op.getProcedureId(),op);
        
        op = new ReplicaOperation(lastOnView, babuDBI, fileIO);
        operations.put(op.getProcedureId(),op);
        
        op = new LoadOperation(lastOnView, maxChunkSize, babuDBI, fileIO);
        operations.put(op.getProcedureId(),op);
        
        op = new ChunkOperation();
        operations.put(op.getProcedureId(),op);
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.transmission.dispatcher.RequestHandler#getInterfaceID()
     */
    @Override
    public int getInterfaceID() {
        return ReplicationServiceConstants.INTERFACE_ID;
    }
}
