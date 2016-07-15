/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import org.xtreemfs.foundation.flease.comm.FleaseMessage;

/**
 * Receives {@link FleaseMessage}s.
 * 
 * @author flangner
 * @since 04/15/2010
 */
public interface FleaseMessageReceiver {

    /**
     * Method to receive a {@link FleaseMessage}.
     * @param message
     */
    public void receive(FleaseMessage message);
}
