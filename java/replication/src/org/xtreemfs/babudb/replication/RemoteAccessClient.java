/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
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

import org.xtreemfs.babudb.api.PersistenceManager;
import org.xtreemfs.babudb.replication.service.clients.ClientResponseFuture;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

/**
 * RPCClient for delegating BabuDB requests to the instance with master 
 * privilege.
 * 
 * @author flangner
 * @since 01/19/2011
 */
public interface RemoteAccessClient {
    
    /**
     * RPC for delegating the duties of {@link PersistenceManager} to a remote
     * BabuDB instance with master privilege.
     * 
     * @param master
     * @param type
     * @param data
     * @return the request future.
     */
    public ClientResponseFuture<?> makePersistent(InetSocketAddress master, 
            int type, ReusableBuffer data);
}
