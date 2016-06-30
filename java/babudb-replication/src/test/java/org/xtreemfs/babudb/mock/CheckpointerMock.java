/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import java.util.Map;

import org.xtreemfs.babudb.api.dev.CheckpointerInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.logging.Logging;

/**
 * 
 * @author flangner
 * @since 03/18/2011
 */

public class CheckpointerMock extends CheckpointerInternal {

    private final TransactionManagerMock txnMan;
    
    public CheckpointerMock(TransactionManagerMock persMan) {
        this.txnMan = persMan;
    }
    
    @Override
    public void checkpoint() throws BabuDBException, InterruptedException {
        checkpoint(false);
    }
    
    @Override
    public LSN checkpoint(boolean incViewId) throws BabuDBException {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
            "Mock tried to create CP. With inc. viewID set %s.", incViewId);
        
        LSN onDisk = txnMan.getLatestOnDiskLSN();
        if (incViewId) {
            txnMan.onDisk.set(new LSN(onDisk.getViewId() + 1, 0L));
        }
        return onDisk;
    }

    @Override
    public void waitForCheckpoint() throws InterruptedException {
        Logging.logMessage(Logging.LEVEL_INFO, this,
            "Mock tried to wait for CP.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#init(org.xtreemfs.babudb.log.DiskLogger, int, long)
     */
    @Override
    public void init(DiskLogger logger, int checkInterval, long maxLogLength) {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
            "Mock tried to initialize checkpointer.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#suspendCheckpointing()
     */
    @Override
    public void suspendCheckpointing() throws InterruptedException {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
            "Mock tried to suspend checkpointer.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#shutdown()
     */
    @Override
    public void shutdown() {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
            "Mock tried to shutdown checkpointer.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#waitForShutdown()
     */
    @Override
    public void waitForShutdown() throws InterruptedException {
        Logging.logMessage(Logging.LEVEL_ERROR, this,
            "Mock tried to wait for checkpointer shutdown.");
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#addSnapshotMaterializationRequest(java.lang.String, int[], org.xtreemfs.babudb.snapshots.SnapshotConfig)
     */
    @Override
    public void addSnapshotMaterializationRequest(String dbName, int[] snapIds, SnapshotConfig snap) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#removeSnapshotMaterializationRequest(java.lang.String, java.lang.String)
     */
    @Override
    public void removeSnapshotMaterializationRequest(String dbName, String snapshotName) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.CheckpointerInternal#getRuntimeState(java.lang.String)
     */
    @Override
    public Object getRuntimeState(String property) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, Object> getRuntimeState() {
        // TODO Auto-generated method stub
        return null;
    }
}
