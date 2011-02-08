/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.service;

import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.service.clients.MasterClient;

/**
 * Interface to provide a slave view on the service layer.
 * 
 * @author flangner
 * @since 04/14/2010
 */
public interface SlaveView {

    /**
     * Chooses a server from the given list which is at least that up-to-date to
     * synchronize with this server.
     * 
     * @param progressAtLeast
     * @return a master client retrieved from the list, or null if none of the 
     *         given babuDBs has the required information. 
     */
    public MasterClient getSynchronizationPartner(LSN progressAtLeast);
}