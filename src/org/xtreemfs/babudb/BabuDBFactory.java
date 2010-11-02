/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import java.io.IOException;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.exceptions.BabuDBException;
import org.xtreemfs.babudb.api.exceptions.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.conversion.AutoConverter;
import org.xtreemfs.babudb.plugin.PluginLoader;
import org.xtreemfs.foundation.logging.Logging;
import org.xtreemfs.foundation.logging.Logging.Category;

/**
 * A factory for the creation of BabuDB instances.
 * 
 * @author stenjan
 * @author flangner
 */
public final class BabuDBFactory {
    
    /**
     * Version (name)
     */
    public static final String           BABUDB_VERSION           = "0.5";
    
    /**
     * Version of the DB on-disk format (to detect incompatibilities).
     */
    public static final int              BABUDB_DB_FORMAT_VERSION = 4;
    
    /**
     * Initializes a new BabuDB instance.
     * 
     * @param configuration
     *            the configuration
     * @throws BabuDBException
     */
    public final static BabuDB createBabuDB(BabuDBConfig configuration) 
            throws BabuDBException {       
        return createBabuDB(configuration, null);
    }
    
    /**
     * Initializes a new BabuDB instance.
     * 
     * @param configuration
     *            the configuration
     * @param staticInit
     * @throws BabuDBException
     */
    public final static BabuDB createBabuDB(BabuDBConfig configuration, 
            StaticInitialization staticInit) throws BabuDBException {
        
        BabuDBInternal babuDB = new BabuDBImpl(configuration);
        
        /*
         * initialize the logger
         */
        Logging.start(configuration.getDebugLevel());
        Logging.logMessage(Logging.LEVEL_INFO, babuDB, 
                "BabuDB %s", BABUDB_VERSION);
        Logging.logMessage(Logging.LEVEL_INFO, babuDB, 
                "\n%s", configuration.toString());
        
        /*
         * run automatic database conversion if necessary
         */
        if (babuDB.getDBConfigFile().isConversionRequired()) {
            Logging.logMessage(Logging.LEVEL_WARN, Category.storage, babuDB, 
                      "The database version is outdated. The database will be "
                    + "automatically converted to the latest version if "
                    + "possible. This may take some time, depending on the " 
                    + "size.");
                
            AutoConverter.initiateConversion(
                    babuDB.getDBConfigFile().getDBFormatVersion(), 
                    configuration);
        }
        
        /*
         * load the optionally plugins
         */
        try {
            babuDB = PluginLoader.init(babuDB);
        } catch (IOException e) {
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, e.getMessage(), 
                    e.getCause());
        }
        
        /*
         * initialize all services provided
         */
        babuDB.init(staticInit);
        
        return babuDB;
    }
}
