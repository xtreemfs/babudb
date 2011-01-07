/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
/*
 * AUTHORS: Felix Langner (ZIB)
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
    private final AtomicReference<Flease> flease;
    
    /** the ID of this server to compare with the lease ID */
    private final ASCIIString             id;
    
    /** listener to inform about certain lease changes */
    private volatile ControlListener      listener;

    /**
     * @param cellId
     * @param ownId
     */
    FleaseHolder(ASCIIString cellId, ASCIIString ownId) {
        
        this.flease = new AtomicReference<Flease>(Flease.EMPTY_LEASE);
        this.id = ownId;
    }
    
    synchronized void registerListener(ControlListener listener) {
        if (this.listener == null)
            this.listener = listener;
    }
    
    /**
     * Determines if this server is owner of the given lease or not.
     * 
     * @return true if this server is owner of the lease, false otherwise.
     */
    boolean amIOwner() {
        Flease lease = this.flease.get();
        return lease.isValid() && this.id.equals(lease.getLeaseHolder());
    }
    
    /**
     * @return the timeout for the currently valid lease.
     */
    long getLeaseTimeout() {
        return this.flease.get().getLeaseTimeout_ms();
    }
    
    /**
     * @return the address of the currently valid leaseHolder.
     */
    InetSocketAddress getLeaseHolderAddress() {
        Flease lease = this.flease.get();
        return (lease.isValid()) ? getAddress(lease.getLeaseHolder()) : null;
    }
    
/*
 * Overridden methods
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
     * @see org.xtreemfs.foundation.flease.FleaseStatusListener#statusChanged(org.xtreemfs.foundation.buffer.ASCIIString, org.xtreemfs.foundation.flease.Flease)
     */
    @Override
    public void statusChanged(ASCIIString cellId, Flease lease) {
        Logging.logMessage(Logging.LEVEL_INFO, this, "received '%s' at" +
                " time %d", lease.toString(), TimeSync.getGlobalTime());
        
        // outdated leases or broken leases will be ignored
        if (lease.isValid()) {
            synchronized (this) {
                ASCIIString newLeaseHolder = lease.getLeaseHolder();
                // check if the lease holder changed
                if (!newLeaseHolder.equals(
                        this.flease.get().getLeaseHolder())) {
                    
                    // notify the handover if this server loses ownership of the 
                    // lease
                    if (this.listener != null)
                        this.listener.notifyForHandover();
                    
                    // notify failover about the change
                    if (this.listener != null)
                        this.listener.notifyForFailover(
                                getAddress(newLeaseHolder).getAddress());
                }
                this.flease.set(lease);
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