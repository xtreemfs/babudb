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
package org.xtreemfs.babudb.replication.policy;

/**
 * Interface to implement replication policies. Decide for each kind of database
 * operation if it is necessary to perform in on the master only.
 * 
 * @author flangner
 * @since 01/19/2011
 */
public interface Policy {
    
    /**
     * @return true, if user inserts are only allowed to be performed at the 
     *         replication master. false, if also slaves are allowed to perform
     *         inserts directly (these are not replicated automatically).
     */
    public boolean insertIsMasterRestricted();
    
    /**
     * @return true, if snapshot manipulations are only allowed to be performed 
     *         at the replication master. false, if also slaves are allowed to 
     *         perform snapshot operations directly (these are not replicated 
     *         automatically).
     */
    public boolean snapshotManipultationIsMasterRestricted();
    
    /**
     * @return true, if database manipulations are only allowed to be performed 
     *         at the replication master. false, if also slaves are allowed to 
     *         perform database manipulating operations directly (these are not 
     *         replicated automatically).
     */
    public boolean dbModificationIsMasterRestricted();
    
    /**
     * @return true, if lookup operations are only allowed to be performed at 
     *         the replication master. false, if also slaves are allowed to 
     *         perform lookups directly.
     */
    public boolean lookUpIsMasterRestricted();
}
