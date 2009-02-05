/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/
package org.xtreemfs.babudb.replication;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.List;

import org.xtreemfs.babudb.lsmdb.LSN;

/**
 * <p>Table of slaves with their latest acknowledged {@link LSN}s.</p>
 * <p>Thread safe.</p>
 * 
 * <p>Slaves are identified by their {@link InetAddress} given from a pinky response.</p>
 * 
 * @author flangner
 */
class SlavesStatus extends Hashtable<InetAddress,LSN> {
    /***/
    private static final long serialVersionUID = -9069872080659282882L;

    private volatile LSN latestCommon = null;
    
    /**
     * <p>Initialises the {@link Hashtable} with <code>slaves</code> with LSN "0:0" as initial acknowledged LSN.</p>
     * 
     * @param slaves
     */
    SlavesStatus(List<InetSocketAddress> slaves) {
        super();
        if (slaves!=null)
            for (InetSocketAddress slave : slaves) 
                put(slave.getAddress(), new LSN(0,0));
            
        latestCommon = new LSN(0,0);
    }
    
    /**
     * <p>Updates the latest acknowledged {@link LSN} of a <code>slave</code>.</p>
     * <p>Compares all {@link LSN}s if necessary, and updates the latest common
     * acknowledged {@link LSN}.</p>
     * 
     * @param slave
     * @param acknowledgedLSN
     * @return <code>true</code>, if the latestCommon {@link LSN} has changed, <code>false</code> otherwise.
     */
    synchronized boolean update(InetAddress slave, LSN acknowledgedLSN) {
        // the latest common LSN is >= the acknowledged one, just update the slave
        if (get(slave)!=null) {            
            // compare all LSNs and take the smallest one as latest common one
            if (get(slave).compareTo(acknowledgedLSN)<0) {
                put(slave, acknowledgedLSN);
                
                LSN latestCommon = acknowledgedLSN;
                for (LSN lsn : values()) {
                    latestCommon = (lsn.compareTo(latestCommon)<0) ? lsn : latestCommon;
                }
                if (!this.latestCommon.equals(latestCommon)) {
                    this.latestCommon = latestCommon;         
                    return true;
                }
            }
        // put a new slave with initial acknowledged LSN    
        // decrease the latestCommon LSN if necessary
        } else {
            put(slave, acknowledgedLSN);
            if (this.latestCommon.compareTo(acknowledgedLSN)>0) {
                this.latestCommon = acknowledgedLSN;
                return true;
            }
        }
        
        // latestCommon LSN did'nt change
        return false;
    }
    
    /**
     * 
     * @return the latest common LSN for all registered slaves.
     */
    LSN getLatestCommonLSN() {
        return latestCommon;
    }
}
