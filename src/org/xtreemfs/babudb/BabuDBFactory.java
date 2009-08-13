/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import org.xtreemfs.include.common.config.BabuDBConfig;
import org.xtreemfs.include.common.config.MasterConfig;
import org.xtreemfs.include.common.config.SlaveConfig;


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
    public static BabuDB createBabuDB(BabuDBConfig configuration) throws BabuDBException {
        return new BabuDB(configuration);
    }
    
    /**
     * Initializes a BabuDB instance with replication in master mode.
     * 
     * @param configuration the master configuration
     * @throws BabuDBException 
     */
    public static BabuDB createMasterBabuDB(MasterConfig configuration) throws BabuDBException {
        return new BabuDB(configuration);
    }
    
    /**
     * Initializes a BabuDB instance with replication in slave mode.
     * 
     * @param configuration the slave configuration
     * @throws BabuDBException 
     */
    public static BabuDB createSlaveBabuDB(SlaveConfig configuration) throws BabuDBException {
        return new BabuDB(configuration);
    }
    
}
