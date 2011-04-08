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
import org.xtreemfs.babudb.replication.control.ControlLayerInterface;
import org.xtreemfs.babudb.replication.service.accounting.StatesManipulation;
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
    
    public ReplicationRequestHandler(StatesManipulation pStates, 
            ControlLayerInterface ctrlLayer, BabuDBInterface babuDBI, RequestManagement reqMan, 
            AtomicReference<LSN> lastOnView, int maxChunkSize, FileIOInterface fileIO, int maxQ) {
        
        super(maxQ);
        
        Operation op = new LocalTimeOperation();
        operations.put(op.getProcedureId(), op);
        
        op = new FleaseOperation(ctrlLayer);
        operations.put(op.getProcedureId(), op);
        
        op = new StateOperation(babuDBI, ctrlLayer);
        operations.put(op.getProcedureId(), op);
        
        op = new VolatileStateOperation(babuDBI);
        operations.put(op.getProcedureId(), op);
        
        op = new HeartbeatOperation(pStates);
        operations.put(op.getProcedureId(), op);
        
        op = new SynchronizeOperation(reqMan);
        operations.put(op.getProcedureId(),op);
        
        op = new ReplicateOperation(reqMan);
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
