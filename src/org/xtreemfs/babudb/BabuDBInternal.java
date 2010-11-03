/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.lsmdb.DBConfig;
import org.xtreemfs.babudb.lsmdb.LSN;

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
     * Returns a reference to the disk logger. The disk logger should not be
     * accessed by applications.
     * 
     * @return a reference to the disk logger
     */
    public DiskLogger getLogger();
    
    /**
     * Initializes all services provided by BabuDB.
     * 
     * @param staticInit
     * @throws BabuDBException if initialization failed.
     */
    public void init(final StaticInitialization staticInit) throws BabuDBException;

    /**
     * Stops all BabuDB services to be able to manipulate files without being 
     * disturbed. 
     * 
     * @see restart()
     */
    public void stop();
    
    /**
     * All services of BabuDB are restarted. Call only after stop()!
     * 
     * @see stop()
     * @throws BabuDBException
     * @return the next LSN.
     */
    public LSN restart() throws BabuDBException;
}
