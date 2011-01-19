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
 * Default replication policy. Any kind of operation has to be performed at the
 * master server in first case. This policy has the worst performance, but best
 * consistency for the data stored at the database. If you are not sure how the
 * queries for your database look like exactly, this is the only policy you 
 * should use.
 * 
 * @author flangner
 * @since 01/19/2011
 */
public final class MasterOnly implements Policy {

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
        return true;
    }

}
