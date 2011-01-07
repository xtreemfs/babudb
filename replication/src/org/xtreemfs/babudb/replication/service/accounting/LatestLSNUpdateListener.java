/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.accounting;

import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * Instance that has to be informed about latest {@link LSN} changes.
 * 
 * @author flangner
 * @since 06/05/2009
 */

abstract class LatestLSNUpdateListener implements Comparable<LatestLSNUpdateListener> {
    final LSN lsn;
    
    LatestLSNUpdateListener(LSN lsn) {
        this.lsn = lsn;
    }
    
    /**
     * Function to call, if the latest {@link LSN} has been changed.
     * 
     */
    abstract void upToDate();
    
    /**
     * Function to call, if the listener is outdated before its {@link LSN} was reached.
     */
    abstract void failed();
    
    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(LatestLSNUpdateListener o) {
        return lsn.compareTo(o.lsn);
    }
}
