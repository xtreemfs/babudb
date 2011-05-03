/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.control;

import java.net.InetSocketAddress;

import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseStatusListener;
import org.xtreemfs.foundation.flease.proposer.FleaseException;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Holder of the currently valid {@link Flease}. Is also a listener for lease
 * changes and notifies other replication components.
 * </br>
 * </br>
 * states: WAIT (null), MASTER (thisAddress), SLAVE (otherAddress), FAILED (Flease.EMPTY_LEASE)
 *
 * @author flangner 
 * @since 04/15/2010
 */
public class FleaseHolder implements FleaseStatusListener {
    
    /** listener to inform about certain lease changes */
    private final FleaseEventListener   listener;
    
    /** the currently valid lease */
    private Flease                      lease = null;

    /**
     * @param cellId
     * @param listener
     */
    FleaseHolder(ASCIIString cellId, FleaseEventListener listener) {
        this.listener = listener;
    }
    
    /**
     * May block for at most <code>DELAY_TO_WAIT_FOR_LEASE_MS</code> ms for a valid lease to become 
     * available.
     * 
     * @return the address of the currently valid leaseHolder.
     */
    synchronized InetSocketAddress getLeaseHolderAddress() {
        
        try {
            if (lease == null) {
                wait();
            }
        } catch (InterruptedException e) {
            /* I don't care */
        }
        
        return (lease != null && lease.isValid()) ? getAddress(lease.getLeaseHolder()) : null;
    }
    
    /**
     * Resets the currently valid lease to ensure that the notifier will be executed on the receive
     * of the next valid {@link Flease} message.
     */
    synchronized void reset() {
        lease = Flease.EMPTY_LEASE;
    }
       
/*
 * overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseStatusListener#leaseFailed(
     *          org.xtreemfs.foundation.buffer.ASCIIString, 
     *          org.xtreemfs.foundation.flease.proposer.FleaseException)
     */
    @Override
    public void leaseFailed(ASCIIString cellId, FleaseException error) {
        
        Logging.logMessage(Logging.LEVEL_WARN, this, "Flease was not" +
                " able to become the current lease holder in %s because:" +
                " %s ", cellId.toString(), error.getMessage());
        
        synchronized (this) {
            lease = Flease.EMPTY_LEASE;
            notifyAll();
        }
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseStatusListener#statusChanged(
     *          org.xtreemfs.foundation.buffer.ASCIIString, org.xtreemfs.foundation.flease.Flease)
     */
    @Override
    public void statusChanged(ASCIIString cellId, Flease newLease) {
        
        Logging.logMessage(Logging.LEVEL_DEBUG, this, "Received new Lease (%s).", 
                newLease.toString());
        
        ASCIIString newLeaseHolder = newLease.getLeaseHolder();
        
        // if the lease is outdated or broken, it will be ignored
        if (newLease.isValid()) {
            
            // update the old lease. assume FLease will always announce lease changes in order.
            Flease oldLease = null;
            synchronized (this) {
                oldLease = lease;
                lease = newLease;
                notifyAll();
            }
            
            // notify listener if the leaseholder has changed
            if (oldLease == null || !newLeaseHolder.equals(oldLease.getLeaseHolder())) {
                
                listener.updateLeaseHolder(getAddress(newLeaseHolder));
            }
        }
    }
    
/*
 * static methods
 */
    
    /**
     * @param address
     * @return the string representation of the given address.
     */
    public final static String getIdentity (InetSocketAddress address) {
        String host = address.getAddress().getHostAddress();
        assert (host != null) : "Address was not resolved before!";
        return host + ":" + address.getPort();
    }
    
    /**
     * @param identity
     * @return the address used to create the given identity.
     */
    public final static InetSocketAddress getAddress (ASCIIString identity) {
        String[] adr = identity.toString().split(":");
        assert(adr.length == 2);
        return new InetSocketAddress(adr[0], Integer.parseInt(adr[1]));
    }
}