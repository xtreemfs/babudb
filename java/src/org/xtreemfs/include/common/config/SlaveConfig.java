/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.include.common.config;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.include.foundation.pinky.SSLOptions;

/**
 * Reading configurations from the replication-config-file.
 * 
 * @since 06/11/2009
 * @author flangner
 *
 */

public class SlaveConfig extends ReplicationConfig {
    
    protected String backupDir;
    
    /** Error message. */
    public static final String slaveProtection = "You are not allowed to proceed this operation, " +
            "because this DB is running as a slave!";
    
    public SlaveConfig() {
        super();
    }
       
    public SlaveConfig(Properties prop) {
        super(prop);
    }
    
    public SlaveConfig(String filename) throws IOException {
        super(filename);
    }
    
    public SlaveConfig(MasterConfig c, InetSocketAddress master, String backupDir) {
        super(c.baseDir,c.dbLogDir,c.numThreads,c.maxLogfileSize,c.checkInterval,
                c.syncMode,c.pseudoSyncWait,c.maxQueueLength,c.port,c.address,
                master,null,c.localTimeRenew,c.sslOptions,0);
        this.backupDir = backupDir;
    }
    
    public SlaveConfig(String baseDir, String logDir, int numThreads, long maxLogFileSize, 
            int checkInterval, SyncMode mode, int pseudoSyncWait, int maxQ,
            int port, InetAddress address, InetSocketAddress master, List<InetSocketAddress> slaves, 
            int localTimeRenew, SSLOptions sslOptions, int repMaxQ, String backupDir) {
        
        super(baseDir, logDir, numThreads, maxLogFileSize, checkInterval, mode, pseudoSyncWait, maxQ, 
                port, address, master, slaves, localTimeRenew, sslOptions, repMaxQ);
        this.backupDir = backupDir;
    }

    public void read() throws IOException {
        super.read();
        
        String backupDir = this.readRequiredString("db.backupDir");
        if (backupDir.equals(baseDir) || backupDir.equals(dbLogDir)) throw new IOException("backup directory has to be different to the dbLog directory and the base directory");   
        this.backupDir = (backupDir.endsWith(File.separator)) ? backupDir : backupDir+File.separator;
        
        
        // read the master
        this.master = this.readRequiredInetAddr("master.address", "master.port");
        
        // read the slaves
        this.slaves = new LinkedList<InetSocketAddress>();
        int number = 0;
        
        InetSocketAddress addr;
        while ((addr = this.readOptionalInetSocketAddr("slave."+number, "slave."+number+".port",null))!=null){
            this.slaves.add(addr);
            number++;
        }
        
        this.slaves.add(new InetSocketAddress(this.address, this.port));
    }
    
    public String getBackupDir() {
        return backupDir;
    }
}