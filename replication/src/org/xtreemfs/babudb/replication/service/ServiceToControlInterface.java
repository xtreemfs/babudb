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
package org.xtreemfs.babudb.replication.service;

import java.io.IOException;
import java.net.InetAddress;

import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.replication.Coinable;
import org.xtreemfs.babudb.replication.FleaseMessageReceiver;
import org.xtreemfs.babudb.replication.control.ControlLayer;
import org.xtreemfs.babudb.replication.control.RoleChangeListener;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.accounting.ReplicateResponse;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * The interface for the {@link ControlLayer} to access methods of the 
 * {@link ServiceLayer}.
 *
 * @author flangner
 * @since 04/14/2010
 */
public interface ServiceToControlInterface extends 
    Coinable<RoleChangeListener, FleaseMessageReceiver> {

    /**
     * Replicate the given LogEntry.
     * 
     * @param payload - the serialized {@link LogEntry} to replicate.
     * @param le - the original {@link LogEntry}.
     * 
     * @return the {@link ReplicateResponse}.
     */
    public ReplicateResponse replicate(LogEntry le, ReusableBuffer payload);

    /**
     * Tries to synchronize with one of the most up-to-date replication
     * participants.
     * 
     * @throws BabuDBException if synchronization failed. 
     * @throws InterruptedException if execution was interrupted.
     * @throws IOException if the log-file could not be switched.
     */
    public void synchronize() throws BabuDBException, 
        InterruptedException, IOException;

    /**
     * Registers a new master given by its address, valid for all replication
     * components.
     * 
     * @param address - may be null, if no master is available at the moment, or
     *                  the local instance is owner of the master privilege.
     */
    public void changeMaster(InetAddress address);
    
    /**
     * @return an overview for available participants.
     */
    public ParticipantsOverview getParticipantOverview();

    /**
     * Registers the listener for a replicate call.
     * 
     * @param response
     */
    public void subscribeListener(ReplicateResponse rp);
    
    /**
     * Aborts any listener registered at the participants states.
     */
    public void reset();
}