/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exceptions.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.lsmdb.DBConfig;

/**
 * Interface of {@link BabuDB} for internal usage. This should not be accessed
 * by any user application, but may be accessed by plugins.
 * 
 * @author flangner
 * @date 11/02/2010
 */
public interface BabuDBInternal extends BabuDB {

    /**
     * @return the {@link DBConfig}.
     */
    public DBConfig getDBConfigFile();

    /**
     * Returns the configuration associated with this BabuDB instance.
     * 
     * @return the configuration.
     */
    public BabuDBConfig getConfig();
    
    /**
     * Initializes all services provided by BabuDB.
     * 
     * @param staticInit
     * @throws BabuDBException if initialization failed.
     */
    public void init(final StaticInitialization staticInit) throws BabuDBException;
}
