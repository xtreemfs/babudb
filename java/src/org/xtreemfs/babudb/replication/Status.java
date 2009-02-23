/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

/**
 * <p>Wrapper that adds a state to an {@link Comparable} <T>, that has to be requested.</p>
 * <p>Method compareTo first orders by status and if equal by <T>.</p>
 * 
 * @author flangner
 * @param <T>
 */
class Status<T extends Comparable<T>> implements Comparable<Status<T>>{
    /**
     * <p>Status options.</p>
     * 
     * @author flangner
     *
     */
    static enum STATUS {
        
        /**
         * not requested
         */
        OPEN,
        
        /**
         * request failed
         */
        FAILED,
        
        /**
         * requested and waiting for answer
         */
        PENDING
        };
    
    private STATUS stat = STATUS.OPEN;
    
    private T c = null;
    
    /**
     * <p>Saves a the given {@link Comparable} <code>c</code>.</p>
     * <p>Status will be OPEN.</p>
     * 
     * @param c
     */
    Status(T c) {
        this.c = c;
    }
    
    /**
     * <p>Saves a the given {@link Comparable} <code>c</code> and the {@link STATUS} <code>stat</code>.</p>
     * 
     * @param c
     * @param stat
     */
    Status(T c, STATUS stat) {
        this(c);
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
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.LSN#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        Status<T> o = (Status<T>) obj;
        if (obj == null) return false;
        return c.equals(o.c);
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Status<T> o) {        
        if (stat.compareTo(o.stat)==0) {
            return c.compareTo(o.c);
        }else 
            return stat.compareTo(o.stat);
    }
}
