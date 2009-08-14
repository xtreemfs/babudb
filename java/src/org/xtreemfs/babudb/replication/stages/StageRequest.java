/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.stages;

import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;

/**
 * Wrapper for stage requests.
 * 
 * @author flangner
 * @since 06/08/2009
 */

public class StageRequest implements Comparable<StageRequest>{

    private int                 stageMethod;

    private Object[]            args;
    
    private final LSN           lsn;

    /**
     * @param stageMethod
     * @param args - first argument has to be the {@link LSN} for ordering the requests.
     */
    public StageRequest(int stageMethod, Object[] args) {
        this.args = args;
        this.stageMethod = stageMethod;
        if (args[0] instanceof LSN)
            this.lsn = (LSN) args[0];
        else 
            this.lsn = null;
    }

    public int getStageMethod() {
        return stageMethod;
    }

    public Object[] getArgs() {
        return args;
    }
    
    public LSN getLSN() {
        return lsn;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(StageRequest o) {
        assert(lsn != null && o.lsn != null); 
        return lsn.compareTo(o.lsn);
    }
    
    /**
     * Frees all reusable buffers on the arguments list.
     */
    public void free() {
        for (Object arg : args) {
            if (arg instanceof ReusableBuffer) {
                BufferPool.free((ReusableBuffer) arg);
            } else if (arg instanceof LogEntry) {
                ((LogEntry) arg).free();
            }
        }
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return ((stageMethod == 4) ? "LE ":"Meta ")+lsn.toString();
    }
}