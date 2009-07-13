/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.include.common.config;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Properties;

/**
 * Reading configurations from the replication-config-file.
 * 
 * @since 05/02/2009
 * @author flangner
 *
 */

public class MasterConfig extends ReplicationConfig {
            
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
    
    public void read() throws IOException {
        super.read();

        this.chunkSize = this.readOptionalInt("chunkSize", 5*1024*1024);
        
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