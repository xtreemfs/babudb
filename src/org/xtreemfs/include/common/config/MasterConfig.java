/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.include.common.config;

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
 * @since 05/02/2009
 * @author flangner
 *
 */

public class MasterConfig extends ReplicationConfig {
          
    public final static int DEFAULT_MAX_CHUNK_SIZE = 5*1024*1024;
    
    protected int         syncN;
    
    /** Chunk size, for initial load of file chunks. */
    protected int         chunkSize;
    
    public MasterConfig() {
        super();
    }
    
    public MasterConfig(Properties prop) {
        super(prop);
    }
    
    public MasterConfig(String filename) throws IOException {
        super(filename);
    }
    
    public MasterConfig(String baseDir, String logDir, int numThreads, long maxLogFileSize, 
            int checkInterval, SyncMode mode, int pseudoSyncWait, int maxQ,
            int port, InetAddress address, InetSocketAddress master, List<InetSocketAddress> slaves, 
            int localTimeRenew, SSLOptions sslOptions, int repMaxQ, int syncN) {
        
        super(baseDir, logDir, numThreads, maxLogFileSize, checkInterval, mode, pseudoSyncWait, maxQ, 
                port, address, master, slaves, localTimeRenew, sslOptions, repMaxQ);
        this.syncN = syncN;
        this.chunkSize = DEFAULT_MAX_CHUNK_SIZE;
    }
    
    public void read() throws IOException {
        super.read();

        this.chunkSize = this.readOptionalInt("chunkSize", DEFAULT_MAX_CHUNK_SIZE);
        
        this.syncN = this.readOptionalInt("sync.n", 0);
        
        // read the master
        this.master = new InetSocketAddress(this.address,this.port);
        
        // read the slaves
        this.slaves = new LinkedList<InetSocketAddress>();
        int number = 0;
        this.slaves.add(this.readRequiredInetAddr("slave."+number, "slave."+number+".port"));
        number++;
        
        InetSocketAddress addr;
        while ((addr = this.readOptionalInetSocketAddr("slave."+number, "slave."+number+".port",null))!=null){
            this.slaves.add(addr);
            number++;
        }
    }
    
    public int getSyncN(){
        return this.syncN;
    }
    
    public int getChunkSize() {
        return this.chunkSize;
    }
}