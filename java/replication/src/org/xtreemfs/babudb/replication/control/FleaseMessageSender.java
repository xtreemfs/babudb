/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.control;

import java.net.InetSocketAddress;

import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.accounting.ParticipantsStates.UnknownParticipantException;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture.ClientResponseAvailableListener;
import org.xtreemfs.foundation.flease.FleaseMessageSenderInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Interface for sending {@link FleaseMessage}s to a distinct receiver.
 * 
 * @author flangner
 * @since 03/08/2010
 */
class FleaseMessageSender implements FleaseMessageSenderInterface {

    /** the accounting object for all replication participants */
    private final ParticipantsOverview  states;
    
    /** the address of this sender */
    private final InetSocketAddress     sender;
    
    /**
     * @param states
     */
    FleaseMessageSender(ParticipantsOverview states, InetSocketAddress senderAddress) {
        this.states = states;
        this.sender = senderAddress;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseMessageSenderInterface#sendMessage(
     *          org.xtreemfs.foundation.flease.comm.FleaseMessage, java.net.InetSocketAddress)
     */
    @Override
    public void sendMessage(final FleaseMessage message, final InetSocketAddress recipient) {
        
        assert (recipient != null && message != null);
                
        try {
            ConditionClient c = states.getByAddress(recipient);
            assert (c != null) : "could not retrieve client for " + recipient.toString();
            message.setSender(sender);

            Logging.logMessage(Logging.LEVEL_DEBUG, this, "sending '%s' from '%s' to '%s' ... ", 
                    message.toString(), sender.toString(), recipient.toString());
            
            c.flease(message).registerListener(new ClientResponseAvailableListener<Object>() {

                @Override
                public void requestFailed(Exception e) {
                    // Flease does not care about failures on sending messages!
                    Logging.logMessage(Logging.LEVEL_INFO, this, 
                            "%s could not be send to '%s', because %s.", 
                            message.toString(), recipient.toString(), e.getMessage());
                    if (e.getMessage() == null) {
                        Logging.logError(Logging.LEVEL_INFO, this, e);
                    }
                }

                @Override
                public void responseAvailable(Object r) { /* I don't care */ }
            });
        } catch (UnknownParticipantException e) {
            
            Logging.logMessage(Logging.LEVEL_ALERT, this, "FLease could not send a message (%s) " +
            		"to %s, because %s.", 
            		message.toString(), recipient.toString(), e.getMessage());
        }
    }
}
