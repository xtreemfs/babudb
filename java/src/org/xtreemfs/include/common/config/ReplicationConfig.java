/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.include.common.config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;

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
    
    protected boolean     useSSL;
    
    protected String      serviceCredsFile;
    
    protected String      serviceCredsPassphrase;
    
    protected String      serviceCredsContainer;
    
    protected String      trustedCertsFile;
    
    protected String      trustedCertsPassphrase;
    
    protected String      trustedCertsContainer;
    
    protected List<InetSocketAddress>   slaves;
    
    protected InetSocketAddress         master;
    
    protected int         maxQ;
    
    private int         localTimeRenew;
    
    public ReplicationConfig() {
        super();
    }
    
    public ReplicationConfig(Properties prop) {
        super(prop);
    }
    
    public ReplicationConfig(String filename) throws IOException {
        super(filename);
    }
    
    public void read() throws IOException {
        super.read();
        
        this.port = this.readRequiredInt("listen.port");
        
        this.address = this.readOptionalInetAddr("listen.address", null);
        
        this.maxQ = this.readOptionalInt("maxQ", 0);
        
        this.localTimeRenew = this.readOptionalInt("localTimeRenew", 3000);
        
        if (this.useSSL = this.readRequiredBoolean("ssl.enabled")) {
            this.serviceCredsFile = this.readRequiredString("ssl.service_creds");
            
            this.serviceCredsPassphrase = this.readRequiredString("ssl.service_creds.pw");
            
            this.serviceCredsContainer = this.readRequiredString("ssl.service_creds.container");
            
            this.trustedCertsFile = this.readRequiredString("ssl.trusted_certs");
            
            this.trustedCertsPassphrase = this.readRequiredString("ssl.trusted_certs.pw");
            
            this.trustedCertsContainer = this.readRequiredString("ssl.trusted_certs.container");
        }
    }
    
    public int getPort() {
        return this.port;
    }

    public InetAddress getAddress() {
        return this.address;
    }
    
    public boolean isUsingSSL() {
        return this.useSSL;
    }
    
    public String getServiceCredsContainer() {
        return this.serviceCredsContainer;
    }
    
    public String getServiceCredsFile() {
        return this.serviceCredsFile;
    }
    
    public String getServiceCredsPassphrase() {
        return this.serviceCredsPassphrase;
    }
    
    public String getTrustedCertsContainer() {
        return this.trustedCertsContainer;
    }
    
    public String getTrustedCertsFile() {
        return this.trustedCertsFile;
    }
    
    public String getTrustedCertsPassphrase() {
        return this.trustedCertsPassphrase;
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