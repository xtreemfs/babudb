/*
 * Copyright (c) 2010-2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import java.io.IOException;

import org.xtreemfs.babudb.BabuDBInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.exception.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.api.plugin.PluginMain;
import org.xtreemfs.babudb.config.ReplicationConfig;
import org.xtreemfs.babudb.plugin.PluginLoader;
import org.xtreemfs.babudb.replication.proxy.BabuDBProxy;
import org.xtreemfs.babudb.replication.transmission.FileIO;
import org.xtreemfs.foundation.logging.Logging;

/**
 * Entry for the {@link PluginLoader} to initialize the replication mechanism.
 * 
 * @author flangner
 * @date 11/03/2010
 */
public class Main implements PluginMain {
    
    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.plugin.PluginMain#start(
     *          org.xtreemfs.babudb.BabuDBInternal, java.lang.String)
     */
    @Override
    public BabuDBInternal start(BabuDBInternal babuDB, String configPath) 
            throws BabuDBException {
        
        // load the plugins configuration
        ReplicationConfig configuration;
        try {
            configuration = new ReplicationConfig(configPath, 
                    babuDB.getConfig());
        } catch (IOException ioe) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                    "Replication configuration is broken.", ioe.getCause());
        }
        
        // replay the backup, if available
        try {
            new FileIO(configuration).replayBackupFiles();
        } catch (IOException io) {
            Logging.logMessage(Logging.LEVEL_ERROR, this, "Could not retrieve" +
            		" the slave backup files, because: ", io.getMessage());
        }
        
        // initialize the replication services
        ReplicationManagerImpl replMan;
        try {
            replMan = new ReplicationManagerImpl(babuDB, configuration);           
        } catch (Exception e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, 
                    e.getMessage(), e.getCause());
        } 
        
        // initialize the BabuDB proxy interface
        return new BabuDBProxy(babuDB, replMan, 
                configuration.getReplicationPolicy(), 
                replMan.getRemoteAccessClient());
    }
}