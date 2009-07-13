/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.events;

import org.xtreemfs.babudb.replication.SlavesStates.NotEnoughAvailableSlavesException;
import org.xtreemfs.babudb.replication.trigger.Trigger;

/**
 * Super type for internally triggered operations. 
 * 
 * @since 06/05/2009
 * @author flangner
 */

public abstract class Event {

    /**
     * @return the unique {@link Event} id.
     */
    public abstract int getProcedureId();

    /**
     * Called after the {@link Event} was assigned.
     * 
     * @param trigger
     * @throws NotEnoughAvailableSlavesException
     * @return a proxy for the broadcast response.
     */
    public abstract EventResponse startEvent(Trigger trigger) throws NotEnoughAvailableSlavesException;
}