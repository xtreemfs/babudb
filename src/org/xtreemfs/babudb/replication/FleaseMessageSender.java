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
package org.xtreemfs.babudb.replication;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.xtreemfs.babudb.clients.StateClient;
import org.xtreemfs.foundation.flease.FleaseMessageSenderInterface;
import org.xtreemfs.foundation.flease.comm.FleaseMessage;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.oncrpc.client.RPCNIOSocketClient;
import org.xtreemfs.include.foundation.oncrpc.client.RPCResponse;

/**
 * Interface for sending {@link FleaseMessage}s to a distinct receiver.
 * 
 * @author flangner
 * @since 03/08/2010
 */
public class FleaseMessageSender implements FleaseMessageSenderInterface {

    /**
     * The {@link RPCNIOSocketClient} for sending messages to other servers.
     */
    private final RPCNIOSocketClient client;
    
    /**
     * The {@link InetSocketAddress} of the local server.
     */
    private final InetSocketAddress local;
    
    /**
     * Caching recipients to avoid generation of unnecessary StateClient
     * objects.
     */
    private final Map<InetSocketAddress, StateClient> recipientsCache;
    
    public FleaseMessageSender(RPCNIOSocketClient c, 
            InetSocketAddress localAddress) {
        assert(c != null);
        assert(localAddress != null);
        
        this.local = localAddress;
        this.client = c;
        this.recipientsCache = new HashMap<InetSocketAddress, StateClient>();
    }
    
    /* (non-Javadoc)
     * @see org.xtreemfs.foundation.flease.FleaseMessageSenderInterface#sendMessage(org.xtreemfs.foundation.flease.comm.FleaseMessage, java.net.InetSocketAddress)
     */
    @Override
    public void sendMessage(FleaseMessage message, InetSocketAddress recipient) {
        StateClient c = null;
        if ((c = recipientsCache.get(recipient)) == null) {
            c = new StateClient(client, recipient, local);
            recipientsCache.put(recipient, c);
        }
        
        RPCResponse<Object> rp = c.flease(message);
        try {
            rp.get();
        } catch (Exception e) {
            // Flease does not care about failures on sending messages!
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "%s could not be send" +
            		" to '%s', because %s.", message.toString(), 
            		recipient.toString(), e.getMessage());
            if (e.getMessage() == null) 
                Logging.logError(Logging.LEVEL_DEBUG, this, e);
        } finally {
            if (rp != null) rp.freeBuffers();
        }
    }
}
