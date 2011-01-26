/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.api;


/**
 * <p>
 * Interface to provide a static database setup for all participants of a 
 * replication setup that will not be replicated on startup.
 * </p>
 * @author flangner
 * @since 03/03/2010
 */
@Deprecated
public interface StaticInitialization {
    
    /**
     * Method that provides an initial setup for {@link BabuDB}.
     * 
     * @param dbMan
     * @param sMan
     */
    public void initialize(DatabaseManager dbMan, SnapshotManager sMan);
}
