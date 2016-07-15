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
 * This policy allows lookups at the slaves. 
 * Be careful consistency may suffer!
 * 
 * @author flangner
 * @since 05/17/2011
 */
public final class WriteRestriction implements Policy {

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.policy.Policy#insertIsMasterRestricted()
     */
    @Override
    public boolean insertIsMasterRestricted() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.policy.Policy#snapshotManipultationIsMasterRestricted()
     */
    @Override
    public boolean snapshotManipultationIsMasterRestricted() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.policy.Policy#dbModificationIsMasterRestricted()
     */
    @Override
    public boolean dbModificationIsMasterRestricted() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.replication.policy.Policy#lookUpIsMasterRestricted()
     */
    @Override
    public boolean lookUpIsMasterRestricted() {
        return false;
    }

}
