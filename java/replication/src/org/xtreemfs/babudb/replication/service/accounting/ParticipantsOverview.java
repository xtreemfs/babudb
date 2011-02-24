/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service.accounting;

import java.net.InetSocketAddress;
import java.util.List;

import org.xtreemfs.babudb.replication.service.clients.ConditionClient;

/**
 * Interface to the {@link ConditionClient}s of servers participating at the
 * replication.
 * 
 * @author flangner
 * @since 04/13/2010
 */
public interface ParticipantsOverview {

    /**
     * @return a list of all available {@link ConditionClient}s 
     *         descending sorted by the last acknowledged LSN of their servers.
     */
    public List<ConditionClient> getConditionClients();

    /**
     * @param address
     * @return a {@link ConditionClient} retrieved by the address of the server
     *         it connects to.
     */
    public ConditionClient getByAddress(InetSocketAddress address);
}