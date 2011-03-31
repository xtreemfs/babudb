/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.control;

import java.net.InetSocketAddress;

import org.xtreemfs.foundation.flease.Flease;

/**
 * Interface for receiving filtered {@link Flease} events.
 * 
 * @author flangner
 * @since 02/17/2011
 */
public interface FleaseEventListener {

    /**
     * Method to execute if a new leaseholder has to be announced.
     * 
     * @param leaseholder
     * 
     * @throws Exception if leaseholder update was not successful.
     */
    public void updateLeaseHolder(InetSocketAddress leaseholder) throws Exception;
}