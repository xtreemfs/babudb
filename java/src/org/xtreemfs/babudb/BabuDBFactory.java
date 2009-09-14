/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import org.xtreemfs.include.common.config.BabuDBConfig;
import org.xtreemfs.include.common.config.ReplicationConfig;

/**
 * A factory for the creation of BabuDB instances.
 * 
 * @author stenjan
 *
 */
public class BabuDBFactory {

    /**
     * Initializes a new BabuDB instance.
     * 
     * @param configuration the configuration
     * @throws BabuDBException
     */
    public static BabuDB createBabuDB(BabuDBConfig configuration) 
        throws BabuDBException {
        
        return new BabuDB(configuration);
    }
    
    /**
     * Initializes a BabuDB instance with replication.
     * Replication will be suspended until one BabuDB instance
     * will be declared to master.
     * 
     * @param configuration the {@link ReplicationConfig}
     * @throws BabuDBException 
     */
    public static BabuDB createReplicatedBabuDB(ReplicationConfig configuration) 
        throws BabuDBException {
        
        return new BabuDB(configuration);
    }
}
