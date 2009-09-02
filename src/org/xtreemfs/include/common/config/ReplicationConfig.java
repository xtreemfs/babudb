/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.include.common.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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

public abstract class ReplicationConfig extends BabuDBConfig {
    
    protected int         port;
    
    protected InetAddress address;
        
    protected SSLOptions  sslOptions;
    
    protected List<InetSocketAddress>   slaves;
    
    protected InetSocketAddress         master;
    
    protected int         maxQ;
    
    protected int         localTimeRenew;
    
    public ReplicationConfig() {
        super();
    }
    
    public ReplicationConfig(Properties prop) {
        super(prop);
    }
    
    public ReplicationConfig(String filename) throws IOException {
        super(filename);
    }
    
    public ReplicationConfig(String baseDir, String logDir, int numThreads, long maxLogFileSize, 
            int checkInterval, SyncMode mode, int pseudoSyncWait, int maxQ,
            int port, InetAddress address, InetSocketAddress master, List<InetSocketAddress> slaves, 
            int localTimeRenew, SSLOptions sslOptions, int repMaxQ) {
        
        super(baseDir, logDir, numThreads, maxLogFileSize, checkInterval, mode, pseudoSyncWait, maxQ);
        this.master = master;
        this.slaves = slaves;
        this.maxQ = repMaxQ;
        this.localTimeRenew = localTimeRenew;
        this.address = address;
        this.port = port;
        this.sslOptions = sslOptions;
    }
    
    public void read() throws IOException {
        super.read();
        
        this.port = this.readRequiredInt("listen.port");
        
        this.address = this.readOptionalInetAddr("listen.address", null);
        
        this.maxQ = this.readOptionalInt("maxQ", 0);
        
        this.localTimeRenew = this.readOptionalInt("localTimeRenew", 3000);
        
        if (this.readRequiredBoolean("ssl.enabled")) {
            this.sslOptions = new SSLOptions(
                    new FileInputStream(this.readRequiredString("ssl.service_creds")),
                    this.readRequiredString("ssl.service_creds.pw"), 
                    this.readRequiredString("ssl.service_creds.container"),
                    new FileInputStream(this.readRequiredString("ssl.trusted_certs")),
                    this.readRequiredString("ssl.trusted_certs.pw"),
                    this.readRequiredString("ssl.trusted_certs.container"),
                    this.readRequiredBoolean("ssl.authenticationWithoutEncryption")
                    );
        }
    }
    
    public int getPort() {
        return this.port;
    }

    public InetAddress getAddress() {
        return this.address;
    }
    
    public SSLOptions getSSLOptions() {
        return this.sslOptions;
    }
    
    public InetSocketAddress getMaster(){
        return this.master;
    }
    
    public List<InetSocketAddress> getSlaves(){
        return this.slaves;
    }

    public int getMaxQ() {
        return this.maxQ;
    }

    public int getLocalTimeRenew() {
        return localTimeRenew;
    }
}