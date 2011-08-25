/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.logic;

import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.BabuDBInterface;
import org.xtreemfs.babudb.replication.service.SlaveView;
import org.xtreemfs.babudb.replication.service.StageRequest;
import org.xtreemfs.babudb.replication.service.ReplicationStage.StageCondition;
import org.xtreemfs.babudb.replication.transmission.FileIOInterface;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Interface for replication-behavior classes.
 * 
 * @author flangner
 * @since 06/08/2009
 */

public abstract class Logic {
    
    public enum LogicID {
        BASIC,
        REQUEST,
        LOAD,
    }
    
    protected final BabuDBInterface      babuDB;
        
    protected final FileIOInterface      fileIO;
    
    protected final SlaveView            slaveView;
    
    protected final AtomicReference<LSN> lastOnView;
    
    /** LSN of the last asynchronously inserted entry */
    private final AtomicReference<LSN>   lastAsyncInserted = new AtomicReference<LSN>(new LSN(0, 0L));
    
    private LSN                          acknowledged = new LSN(0,0L);
    
    /**
     * @param babuDB
     * @param slaveView
     * @param fileIO
     * @param lastOnView
     */
    Logic(BabuDBInterface babuDB, SlaveView slaveView, FileIOInterface fileIO, AtomicReference<LSN> lastOnView) {
        
        this.slaveView = slaveView;
        this.fileIO = fileIO;
        this.babuDB = babuDB;
        this.lastOnView = lastOnView;
    }
    
    /**
     * Method to set a new latest entry that was inserted asynchronously.
     * 
     * @param lsn
     */
    void updateLastAsyncInsertedEntry(LSN lsn) {
        synchronized (lastAsyncInserted) {
            if (lastAsyncInserted.get() != null) {
                lastAsyncInserted.set(lsn);
                if (lsn == null) lastAsyncInserted.notify();
            }
        }
    }
    
    /**
     * Update the last LSN acknowledged by the on-disk BabuDB.
     * @param lsn
     */
    void updateLastAcknowledgedEntry(LSN lsn) {
        synchronized (lastAsyncInserted) {
            if (acknowledged.compareTo(lsn) < 0) {
                acknowledged = lsn;
                lastAsyncInserted.notify();
            }
        }
    }
    
    /**
     * @return the LSN of the latest entry written to disk.
     */
    public LSN getState() {
        LSN written = babuDB.getState();
        LSN async = lastAsyncInserted.get();
        return (async == null || async.compareTo(written) <= 0) ? written : async;
    }
    
    /**
     * Method to wait for a steady state.
     * 
     * @throws InterruptedException
     */
    public void waitForSteadyState() throws InterruptedException {
        synchronized (lastAsyncInserted) {
            LSN async = lastAsyncInserted.get();
            while (async != null && async.compareTo(acknowledged) > 0) {
                lastAsyncInserted.wait();
                async = lastAsyncInserted.get();
            }
        }
    }
    
    /**
     * @return unique id, identifying the logic.
     */
    public abstract LogicID getId();
    
    /**
     * Function to execute, if logic is needed.
     * 
     * @param rq
     * 
     * @return new {@link StageCondition}, or null, if stage has to be reset.
     */
    public abstract StageCondition run(StageRequest rq);
    
    /**
     * Function to execute, if logic is needed.
     * 
     * @param condition
     * 
     * @throws InterruptedException if the execution was interrupted.
     * 
     * @return new {@link StageCondition}, or null, if stage has to be reset.
     */
    public abstract StageCondition run(StageCondition condition) throws InterruptedException;
    
    /**
     * Method to decide which logic shall be used next.
     * 
     * @param end
     * 
     * @return the {@link StageCondition} for the next step.
     */
    protected StageCondition finish(LSN end) {
        return finish(end, false);
    }
    
    /**
     * Method to decide which logic shall be used next.
     * 
     * @param end - (inclusive)
     * @param load
     * 
     * @return the {@link StageCondition} for the next step.
     */
    protected StageCondition finish(LSN end, boolean load) {
        LSN actual = getState();
           
        // we are still missing some entries
        // update the missing entries
        if (end != null && actual.compareTo(end) < 0) {
            if (load) {
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "LOAD to LSN(%s) @ LSN(%s).", 
                        end.toString(), actual.toString());
                return new StageCondition(LogicID.LOAD, end);
            } else {
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "REQUEST to LSN(%s) @ LSN(%s).", 
                        end.toString(), actual.toString());
                return new StageCondition(end);
            }
           
        // all went fine -> back to basic
        } else {
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "Back to basic @ LSN(%s).", actual.toString()); 
            lastAsyncInserted.set(new LSN (0, 0L));
            return StageCondition.DEFAULT;
        }
    }
}