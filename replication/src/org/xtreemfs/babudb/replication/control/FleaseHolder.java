/*
 * Copyright (c) 2010, Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this 
 * list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice, 
 * this list of conditions and the following disclaimer in the documentation 
 * and/or other materials provided with the distribution.
 * Neither the name of the Konrad-Zuse-Zentrum fuer Informationstechnik Berlin 
 * nor the names of its contributors may be used to endorse or promote products 
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
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