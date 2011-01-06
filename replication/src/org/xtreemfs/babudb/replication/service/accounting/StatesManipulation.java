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
package org.xtreemfs.babudb.replication.service.accounting;

import java.net.SocketAddress;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;

/**
 * <p>
 * Interface for the {@link ParticipantsStates} to manipulate them using 
 * informations received via other components of the system.
 * </p>
 * 
 * @author flangner
 * @since 13.04.2010
 */
public interface StatesManipulation {

    /**
     * <p>
     * Updates the state for the given participant using the receiveTime and
     * the acknowledgedLSN.
     * </p>
     * 
     * @param participant
     * @param acknowledgedLSN
     * @param receiveTime
     * 
     * @throws UnknownParticipantException if the participant is not registered.
     */
    public void update(SocketAddress participant, LSN acknowledgedLSN, 
            long receiveTime) throws UnknownParticipantException;

    /**
     * <p>
     * Marks a slave manually as dead and decrements the open requests for the 
     * slave.
     * </p>
     * 
     * @param slave
     */
    public void markAsDead(SlaveClient slave);

    /**
     * <p>Decrements the open requests for the slave.</p>
     * 
     * @param slave
     */
    public void requestFinished(SlaveClient slave);
}