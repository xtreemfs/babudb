/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

/**
 * <p>Wrapper for an {@link Comparable}, that is missing and has to be requested.</p>
 * <p>The is ordered by status.</p>
 * 
 * @author flangner
 *
 */
class Missing<T> implements Comparable<Missing<Comparable<T>>>{
    /**
     * <p>Status options for a missing {@link LSN}.</p>
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
    
    STATUS stat = null;
    
    Comparable<T> c = null;
    
    /**
     * <p>Saves a the given {@link Comparable} <code>c</code> and the {@link STATUS} <code>stat</code>.</p>
     * 
     * @param c
     * @param stat
     */
    Missing(Comparable<T> c, STATUS stat) {
        this.c = c;
        this.stat = stat;
    }
    
    /*
     * (non-Javadoc)
     * @see org.xtreemfs.babudb.lsmdb.LSN#equals(java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object obj) {
        Missing<T> o = (Missing<T>) obj;
        return c.equals(o.c);
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Missing<Comparable<T>> o) {        
        if (stat.compareTo(o.stat)==0) {
            return c.compareTo((T) o.c);
        }else 
            return stat.compareTo(o.stat);
    }
}
