/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationThread.ReplicationException;

/**
 * <p>Table of slaves with their latest acknowledged {@link LSN}s.</p>
 * <p>Thread safe.</p>
 * 
 * <p>Slaves are identified by their {@link InetSocketAddress} given from a pinky response.</p>
 * 
 * @author flangner
 */
class SlavesStatus extends Hashtable<InetSocketAddress,LSN> {
    /***/
    private static final long serialVersionUID = -9069872080659282882L;

    private volatile LSN latestCommon = null;
    
    private final Hashtable<InetSocketAddress, List<Status<Request>>> statusListener;
    
    /**
     * <p>Initializes the {@link Hashtable} with <code>slaves</code> with LSN "0:0" as initial acknowledged LSN.</p>
     * 
     * @param slaves
     */
    SlavesStatus(List<InetSocketAddress> slaves) {
    	super();
        if (slaves!=null){
            for (InetSocketAddress slave : slaves) 
                put(slave, new LSN(0,0));
            
            statusListener = new Hashtable<InetSocketAddress, List<Status<Request>>>(slaves.size());
        }else
        	statusListener = new Hashtable<InetSocketAddress, List<Status<Request>>>();
        
        latestCommon = new LSN(0,0);
    }
    
    /**
     * <p>Updates the latest acknowledged {@link LSN} of a <code>slave</code>.</p>
     * <p>Compares all {@link LSN}s if necessary, and updates the latest common
     * acknowledged {@link LSN}.</p>
     * <p>Notify's the listener if set.</p>
     * 
     * @param slave
     * @param acknowledgedLSN
     * @return <code>true</code>, if the latestCommon {@link LSN} has changed, <code>false</code> otherwise.
     * @throws ReplicationException 
     */
    synchronized boolean update(InetSocketAddress slave, LSN acknowledgedLSN) throws ReplicationException {
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
        
        // notify the listener\s, if available
        List<Status<Request>> listeners = statusListener.get(slave);
        if (listeners!=null && !listeners.isEmpty()) {
        	Collection<Status<Request>> ready = new HashSet<Status<Request>>();
        	for (Status<Request> listener : listeners){
        		if (listener.getValue().getLSN().compareTo(acknowledgedLSN) <= 0) {
        			listener.finished();
        			ready.add(listener);
        		}
        	}
        	listeners.removeAll(ready);
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
    
    /**
     * <p>The added listener will be notified, if the latest acknowledged {@link LSN} of the key-address changed.</p>
     * 
     * @param key
     * @param listener
     */
    synchronized void addStatusListener(InetSocketAddress key, Status<Request> listener){
    	List<Status<Request>> listeners = statusListener.get(key);
    	if (listeners==null)
    		listeners = new LinkedList<Status<Request>>();

    	listeners.add(listener);
    	statusListener.put(key, listeners);
    }
    
    /**
     * @return the state of all registered slaves.
     */
    @Override
    public synchronized String toString() {
	String result = "Slaves-Status:\n";
	result +="Slave's Address || Least acknowledged LSN\n";
        for (InetSocketAddress slave : keySet())
            result += slave.toString()+" || "+get(slave).toString()+"\n";
            
        return result;
    }
}
