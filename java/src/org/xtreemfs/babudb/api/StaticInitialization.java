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
 * Interface to provide static initialization code. The code will be executed
 * prior to initializing any plug-ins.
 * </p>
 * <p>
 * Static initialization code can e.g. be used to ensure that all replicas of a
 * replicated BabuDB installation have the same set of initial databases with
 * the same content.
 * </p>
 * 
 * @author flangner
 * @since 03/03/2010
 */
public interface StaticInitialization {
    
    /**
     * Method that provides an initial setup for {@link BabuDB}.
     * 
     * @param dbMan the database manager. It can e.g. be used to create and retrieve databases.
     * @param sMan the snapshot manager. It can e.g. be used to create initial snapshots.
     */
    public void initialize(DatabaseManager dbMan, SnapshotManager sMan);
}
