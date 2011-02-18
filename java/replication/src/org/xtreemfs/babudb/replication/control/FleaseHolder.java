/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.control;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.xtreemfs.foundation.TimeSync;
import org.xtreemfs.foundation.buffer.ASCIIString;
import org.xtreemfs.foundation.flease.Flease;
import org.xtreemfs.foundation.flease.FleaseStatusListener;
import org.xtreemfs.foundation.flease.proposer.FleaseException;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Holder of the currently valid {@link Flease}. Is also a listener for lease
 * changes and notifies other replication components.
 *
 * @author flangner 
 * @since 04/15/2010
 */
public class FleaseHolder implements FleaseStatusListener {
  
    /** the currently valid lease */
    private final AtomicReference<Flease> flease = new AtomicReference<Flease>(Flease.EMPTY_LEASE);
    
    /** listener to inform about certain lease changes */
    private final FleaseEventListener     listener;

    /**
     * @param cellId
     * @param listener
     */
    FleaseHolder(ASCIIString cellId, FleaseEventListener listener) {
        this.listener = listener;
    }
    
    /**
     * @return the address of the currently valid leaseHolder.
     */
    InetSocketAddress getLeaseHolderAddress() {
        Flease lease = flease.get();
        return (lease.isValid()) ? getAddress(lease.getLeaseHolder()) : null;
    }
    
    /**
     * Resets the currently valid lease to ensure that the notifier will be executed on the receive
     * of the next valid {@link Flease} message.
     */
    void reset() {
        flease.set(Flease.EMPTY_LEASE);
    }
       
/*
 * overridden methods
 */
    
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseStatusListener#leaseFailed(org.xtreemfs.foundation.buffer.ASCIIString, org.xtreemfs.foundation.flease.proposer.FleaseException)
     */
    @Override
    public void leaseFailed(ASCIIString cellId, FleaseException error) {
        Logging.logMessage(Logging.LEVEL_WARN, this, "Flease was not" +
                " able to become the current lease holder in %s because:" +
                " %s ", cellId.toString(), error.getMessage());
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseStatusListener#statusChanged(
     *          org.xtreemfs.foundation.buffer.ASCIIString, org.xtreemfs.foundation.flease.Flease)
     */
    @Override
    public void statusChanged(ASCIIString cellId, Flease lease) {
        Logging.logMessage(Logging.LEVEL_INFO, this, "received '%s' at" +
                " time %d", lease.toString(), TimeSync.getGlobalTime());
        
        ASCIIString newLeaseHolder = lease.getLeaseHolder();
        
        // if the lease is outdated ,broken or the leaseholder has not changed, it will be ignored
        if (lease.isValid() && !newLeaseHolder.equals(flease.getAndSet(lease).getLeaseHolder())) {
            
            // notify listener about the change
            try {
                listener.updateLeaseHolder(getAddress(newLeaseHolder).getAddress());
            } catch (Exception e) {
                Logging.logError(Logging.LEVEL_WARN, this, e);
                reset();
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
    public static String getIdentity (InetSocketAddress address) {
        String host = address.getAddress().getHostAddress();
        assert (host != null) : "Address was not resolved before!";
        return host + ":" + address.getPort();
    }
    
    /**
     * @param identity
     * @return the address used to create the given identity.
     */
    public static InetSocketAddress getAddress (ASCIIString identity) {
        String[] adr = identity.toString().split(":");
        assert(adr.length == 2);
        return new InetSocketAddress(adr[0], Integer.parseInt(adr[1]));
    }
}