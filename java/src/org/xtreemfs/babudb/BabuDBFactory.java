/*
 * Copyright (c) 2010 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */

package org.xtreemfs.babudb;

import java.io.IOException;

import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.StaticInitialization;
import org.xtreemfs.babudb.api.dev.BabuDBInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
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
    public static final String BABUDB_VERSION           = "0.5.6";
    
    /**
     * Version of the DB on-disk format (to detect incompatibilities).
     */
    public static final int    BABUDB_DB_FORMAT_VERSION = 4;
    
    /**
     * Initializes a new BabuDB instance.
     * 
     * @param configuration
     *            the configuration
     * @throws BabuDBException
     */
    public final static BabuDB createBabuDB(BabuDBConfig configuration) throws BabuDBException {
        return createBabuDB(configuration, null);
    }
    
    /**
     * Initializes a new BabuDB instance.
     * 
     * @param configuration
     *            the configuration
     * @param staticInit
     *            an implementation of the {@link StaticInitialization}
     *            interface. Its <code>initialize</code> method will be executed
     *            prior to setting up any plug-ins. If <code>staticInit</code>
     *            is <code>null</code>, no such code will be executed.
     * @throws BabuDBException
     */
    public final static BabuDB createBabuDB(BabuDBConfig configuration, StaticInitialization staticInit)
        throws BabuDBException {
        
        /*
         * initialize the logger
         */
        Logging.start(configuration.getDebugLevel());
        
        /*
         * allocate and preload BabuDB
         */
        BabuDBInternal babuDB = new BabuDBImpl(configuration);
        Logging.logMessage(Logging.LEVEL_INFO, babuDB, "BabuDB %s", BABUDB_VERSION);
        Logging.logMessage(Logging.LEVEL_INFO, babuDB, "\n%s", configuration.toString());
        
        /*
         * run automatic database conversion if necessary
         */
        if (babuDB.getDBConfigFile().isConversionRequired()) {
            Logging.logMessage(Logging.LEVEL_WARN, Category.storage, babuDB,
                "The database version is outdated. The database will be "
                    + "automatically converted to the latest version if "
                    + "possible. This may take some time, depending on the " + "size.");
            
            AutoConverter.initiateConversion(babuDB.getDBConfigFile().getDBFormatVersion(), configuration);
        }
        
        /*
         * load the optional plugins
         */
        try {
            babuDB = PluginLoader.init(babuDB);
        } catch (IOException e) {
            if (e.getMessage() == null)
                Logging.logError(Logging.LEVEL_ERROR, babuDB, e);
            throw new BabuDBException(ErrorCode.BROKEN_PLUGIN, e.getMessage(), e.getCause());
        }
        
        /*
         * initialize all services provided
         */
        babuDB.init(staticInit);
        
        return babuDB;
    }
}
