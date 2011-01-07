/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.clients;

import java.net.InetSocketAddress;

/**
 * Default functionality of a client for a specified server.
 * 
 * @author flangner
 * @since 04/13/2010
 */
public interface ClientInterface {
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString();

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj);
    
    /**
     * @return the address of the server this client addresses.
     */
    public InetSocketAddress getDefaultServerAddress();
}