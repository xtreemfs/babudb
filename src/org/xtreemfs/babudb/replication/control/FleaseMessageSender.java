/*
 * Copyright (c) 2009-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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

import org.xtreemfs.babudb.replication.service.accounting.ParticipantsOverview;
import org.xtreemfs.babudb.replication.service.clients.ConditionClient;
import org.xtreemfs.foundation.flease.FleaseMessageSenderInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.oncrpc.client.RPCResponse;

/**
 * Interface for sending {@link FleaseMessage}s to a distinct receiver.
 * 
 * @author flangner
 * @since 03/08/2010
 */
class FleaseMessageSender implements FleaseMessageSenderInterface {

    /** the accounting object for all replication participants */
    private final ParticipantsOverview states;
    
    /**
     * @param states
     */
    FleaseMessageSender(ParticipantsOverview states) {
        this.states = states;
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseMessageSenderInterface#sendMessage(org.xtreemfs.foundation.flease.comm.FleaseMessage, java.net.InetSocketAddress)
     */
    @Override
    public void sendMessage(FleaseMessage message, InetSocketAddress recipient) {
        ConditionClient c = this.states.getByAddress(recipient.getAddress());
        assert (c != null) : "could not retrieve client for " + 
                             recipient.toString();
        
        RPCResponse<Object> rp = c.flease(message);
        try {
            rp.get();
        } catch (Exception e) {
            // Flease does not care about failures on sending messages!
            Logging.logMessage(Logging.LEVEL_DEBUG, this, 
                    "%s could not be send to '%s', because %s.", 
                    message.toString(), recipient.toString(), e.getMessage());
            if (e.getMessage() == null) 
                Logging.logError(Logging.LEVEL_DEBUG, this, e);
        } finally {
            if (rp != null) rp.freeBuffers();
        }
    }
}
