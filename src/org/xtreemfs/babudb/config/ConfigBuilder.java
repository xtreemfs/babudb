/*
 * Copyright (c) 2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.babudb.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.foundation.logging.Logging;

/**
 * A configuration tool for BabuDB. This tool simplifies the configuration of
 * BabuDB by allowing users to selectively adjust configuration properties. If
 * no adjustments are made, default properties will be used. <br>
 * For a more fine-grained configuration, use
 * {@link org.xtreemfs.babudb.config.BabuDBConfig}.
 * 
 * @author stenjan
 * 
 */
public class ConfigBuilder {
    
    Map<String, String> changes = new HashMap<String, String>();
    
    /**
     * Sets the path in which all persistently stored data of BabuDB resides.
     * Both checkpoint and log files will be in the same directory.
     * 
     * @param dir
     *            the data directory
     * @return a reference to this object
     */
    public ConfigBuilder setDataPath(String dir) {
        
        changes.put("babudb.baseDir", dir);
        changes.put("babudb.logDir", dir + "/log");
        
        return this;
    }
    
    /**
     * Sets the paths in which all persistently stored data of BabuDB resides.
     * 
     * @param dbDir
     *            the directory for checkpoint files of indices
     * @param logDir
     *            the directory for database log files
     * @return a reference to this object
     */
    public ConfigBuilder setDataPath(String dbDir, String logDir) {
        
        changes.put("babudb.baseDir", dbDir);
        changes.put("babudb.logDir", logDir);
        
        return this;
    }
    
    /**
     * Enables multi-threaeded request processing and adjusts the size of the
     * thread pool.
     * 
     * @param numThreads
     *            the number of threads in the thread pool
     * @return a reference to this object
     */
    public ConfigBuilder setMultiThreaded(int numThreads) {
        
        changes.put("babudb.worker.numThreads", numThreads + "");
        return this;
    }
    
    /**
     * Enables or disables compression of database contents.
     * 
     * @param compression
     *            if <code>true</code>, compression will be enabled; otherwise,
     *            it will be disabled
     * @return a reference to this object
     */
    public ConfigBuilder setCompressed(boolean compression) {
        
        changes.put("babudb.compression", compression + "");
        return this;
    }
    
    /**
     * Specifies the synchronization mode for log appends.
     * 
     * @param syncMode
     *            the synchronization mode
     * @return a reference to this object
     */
    public ConfigBuilder setLogAppendSyncMode(SyncMode syncMode) {
        
        changes.put("babudb.sync", syncMode.toString());
        return this;
    }
    
    // /**
    // * Sets the list of replicas.
    // *
    // * @param replicas
    // * a list of socket addresses for the replicas, including the one
    // * of the local replica
    // * @return a reference to this object
    // */
    // public ConfigBuilder setReplicas(InetSocketAddress[] replicas) {
    //        
    // int i = 0;
    // for (InetSocketAddress repl : replicas) {
    // changes.put("babudb.repl.participant." + i, repl.getHostName());
    // changes.put("babudb.repl.participant." + i + ".port", repl.getPort() +
    // "");
    // i++;
    // }
    //        
    // return this;
    // }
    
    /**
     * Builds a BabuDB configuration instance.
     * 
     * @return a <code>BabuDBConfig</code> instance
     */
    public BabuDBConfig build() {
        
        Properties props = new Properties();
        BabuDBConfig cfg = null;
        try {
            props.load(ConfigBuilder.class.getResourceAsStream("default-config.properties"));
            
            props.putAll(changes);
            
            cfg = new BabuDBConfig(props);
            
        } catch (IOException exc) {
            Logging.logError(Logging.LEVEL_ERROR, null, exc);
        }
        
        if (cfg == null)
            throw new NullPointerException();
        
        return cfg;
    }
    
    // /**
    // * Builds a configuration instance for a replicated BabuDB installation.
    // *
    // * @return @return a <code>ReplicationConfig</code> instance
    // */
    // public ReplicationConfig buildRepl() {
    //        
    // Properties props = new Properties();
    // ReplicationConfig cfg = null;
    // try {
    // props.load(ConfigBuilder.class.getResourceAsStream("default-config.properties"));
    //            
    // props.putAll(changes);
    //            
    // cfg = new ReplicationConfig(props);
    //            
    // } catch (IOException exc) {
    // Logging.logError(Logging.LEVEL_ERROR, null, exc);
    // }
    //        
    // if (cfg == null)
    // throw new NullPointerException();
    //        
    // return cfg;
    //        
    // }
    
}