/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.accounting.StatesManipulation;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.service.clients.ClientInterface;
import org.xtreemfs.babudb.replication.service.clients.SlaveClient;

import static junit.framework.Assert.*;

/**
 * @author flangner
 * @since 04/08/2011
 */
public class StatesManipulationMock implements StatesManipulation {

    private final InetSocketAddress participant;
    private LSN lsn = new LSN(0,0L);
    
    public StatesManipulationMock(InetSocketAddress participant) {
        this.participant = participant;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.StatesManipulation#
     *          update(java.net.InetSocketAddress, org.xtreemfs.babudb.lsmdb.LSN, long)
     */
    @Override
    public void update(InetSocketAddress participant, LSN acknowledgedLSN, long receiveTime)
            throws UnknownParticipantException {
        
        assertEquals(this.participant, participant);
        assertTrue(lsn.compareTo(acknowledgedLSN) < 0);
        lsn = acknowledgedLSN;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.StatesManipulation#
     *          markAsDead(org.xtreemfs.babudb.replication.service.clients.ClientInterface)
     */
    @Override
    public void markAsDead(ClientInterface slave) {
        assertEquals(participant, slave.getDefaultServerAddress());
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.service.accounting.StatesManipulation#requestFinished(
     *          org.xtreemfs.babudb.replication.service.clients.SlaveClient)
     */
    @Override
    public void requestFinished(SlaveClient slave) {
        assertEquals(participant, slave.getDefaultServerAddress());
    }
}
