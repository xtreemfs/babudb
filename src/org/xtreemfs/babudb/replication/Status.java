/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.util.concurrent.atomic.AtomicInteger;

import org.xtreemfs.babudb.replication.ReplicationThread.ReplicationException;

import static org.xtreemfs.babudb.replication.Status.STATUS.*;

/**
 * <p>Wrapper that adds a state to an <T>, that has to be requested.</p>
 * <p>Method compareTo first orders by status and if equal by <T>.</p>
 * 
 * @author flangner
 * @param <T>
 */
class Status<T> {
    /**
     * <p>Status options.</p>
     * <p>Tokens are ordered by the priority, with which they will be processed in the ReplicationThread.</p>
     * 
     * @author flangner
     *
     */
    static enum STATUS {
        
        /** not requested */
        OPEN,
        
        /** requested and waiting for answer */
        PENDING,
        
        };
    
    private             STATUS          stat    = STATUS.OPEN;
    
    private             T               c       = null;
    
    /** counter for failed attempts to process this {@link Request} */
    private final       AtomicInteger   failedAttempts          = new AtomicInteger(0);
    
    private final   ReplicationThread   statusListener;
    
    /**
     * <p>Dummy constructor!</p>
     * <p>Saves a the given <code>c</code>.</p>
     * <p>Status will be OPEN.</p>
     * 
     * @param c
     */
    Status(T c) {
        this(c,(ReplicationThread) null);
    }
    
    /**
     * <p>Dummy constructor!</p>
     * <p>Saves a the given <code>c</code> and the {@link STATUS} <code>stat</code>.</p>
     * 
     * @param c
     * @param stat
     */
    Status(T c, STATUS stat) {
        this(c,stat,null);
    }
    
    /**
     * <p>Saves a the given <code>c</code>.</p>
     * <p>Status will be OPEN.</p>
     * 
     * @param c
     * @param sL - the obsolete-status-listener
     */
    Status(T c, ReplicationThread sL) {
        this.c = c;
        this.statusListener = sL;
    }
    
    /**
     * <p>Saves a the given <code>c</code> and the {@link STATUS} <code>stat</code>.</p>
     * 
     * @param c
     * @param stat
     * @param sL - the obsolete-status-listener
     */
    Status(T c, STATUS stat, ReplicationThread sL) {
        this(c,sL);
        this.stat = stat;
    }

/*
 * getter/setter    
 */   
    STATUS getStatus(){
        return stat;
    }
    
    T getValue(){
        return c;
    }
   
    void setStatus(STATUS s){
        this.stat = s;
    }
    
    void setValue(T v){
        this.c = v;
    }
    
    /**
     * Increases a inner failure-attempt-counter.
     * 
     * @return true, if there is an attempt left.
     */
    boolean attemptFailedAttemptLeft(int maxTries){
        int failed = this.failedAttempts.incrementAndGet();
        return (maxTries == 0) || (failed < maxTries);
    }
    
    /**
     * <p>If the request becomes obsolete it will be removed from the listeners queues.</p>
     * @throws ReplicationException if request could not be removed.
     */
    void cancel() throws ReplicationException {       
        assert (statusListener != null) : "A dummy cannot be obsolete!";

        statusListener.remove(this);
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.LSN#equals(java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        Status<T> o = (Status<T>) obj;
        if (obj == null) return false;
        return c.equals(o.c);
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {  
        String string = stat.toString()+": "+((c!=null) ? c.toString() : "n.a.");
        if (stat.equals(OPEN))
            string+="This request failed for '"+failedAttempts.get()+"' times,";
        
        return string;       
    }
}
