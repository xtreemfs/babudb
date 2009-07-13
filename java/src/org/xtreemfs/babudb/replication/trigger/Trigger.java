/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication.trigger;

import org.xtreemfs.babudb.replication.events.Event;

/**
 * Interface for classes triggering internal {@link Event}s.
 * 
 * @author flangner
 * @since 06/05/2009
 */

public interface Trigger {
    
    /**
     * @return the unique internal event number.
     */
    public int getEventNumber();
}