/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.accounting;

import java.net.InetSocketAddress;

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
 * @since 04/13/2010
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
    public void update(InetSocketAddress participant, LSN acknowledgedLSN, 
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