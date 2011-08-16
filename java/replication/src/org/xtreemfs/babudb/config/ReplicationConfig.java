/*
 * Copyright (c) 2009 - 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.replication.control.FleaseHolder;
import org.xtreemfs.babudb.replication.policy.Policy;
import org.xtreemfs.foundation.SSLOptions;
import org.xtreemfs.foundation.flease.FleaseConfig;

/**
 * Reading configurations from the replication-config-file.
 * 
 * @since 05/02/2009
 * @author flangner
 * 
 */

public class ReplicationConfig extends PluginConfig {
    
    private final static String POLICY_PACKAGE = "org.xtreemfs.babudb.replication.policy.";
    
    protected final BabuDBConfig     babuDBConfig;
    
    protected InetSocketAddress      address;
    
    protected SSLOptions             sslOptions;
    
    /**
     * List of participants without the local instance.
     */
    protected Set<InetSocketAddress> participants;
    
    protected int                    localTimeRenew;
    
    protected int                    timeSyncInterval;
    
    protected final FleaseConfig     fleaseConfig;
    
    protected String                 policyName;
    protected final Policy           replicationPolicy;
    
    /** 
     * longest duration a message can live on the wire, before it becomes void (one-way)
     */
    public static final int         MESSAGE_TIMEOUT        = 5 * 1000;
    
    /** 
     * longest duration a connection can be established without getting closed 
     */
    public static final int          LEASE_TIMEOUT          = 60 * 1000;
        
    /** 
     * longest duration before an RPC-Call is timed out for the client (RTT) 
     */
    public static final int          REQUEST_TIMEOUT        = 2 * MESSAGE_TIMEOUT;
    
    /** 
     * longest duration before an idle connection is closed 
     */
    public static final int          CONNECTION_TIMEOUT     = 5 * 60 * 1000;
    
    /** 
     * the maximal delay to wait for an valid lease to become available 
     */
    public static final int         DELAY_TO_WAIT_FOR_LEASE_MS = REQUEST_TIMEOUT;
    
    // for master usage only
    
    protected int                    syncN;
    
    /** 
     * Chunk size, for initial load of file chunks 
     */
    protected int                    chunkSize;
    
    private final static int         DEFAULT_MAX_CHUNK_SIZE = 5 * 1024 * 1024;
    
    // for slave usage only
    
    protected String                 backupDir;
    
    protected boolean                redirect = false;
        
    /** 
     * maximal retries per failure 
     */
    public static final int          MAX_RETRIES            = 3;
    
    public ReplicationConfig(Properties prop, BabuDBConfig babuConf) 
            throws IOException {
        super(prop);
        this.babuDBConfig = babuConf;
        read();
        
        this.fleaseConfig = new FleaseConfig(LEASE_TIMEOUT, this.localTimeRenew, 
                MESSAGE_TIMEOUT, this.address, 
                FleaseHolder.getIdentity(this.address), MAX_RETRIES);
        
        // get the replication policy
        try {
            this.replicationPolicy = (Policy) Policy.class.getClassLoader().
                        loadClass(POLICY_PACKAGE + policyName).newInstance();
            
        } catch (Exception e) {
            throw new IOException("ReplicationPolicy could not be " +
            		"instantiated, because: " + e.getMessage());
        }
    }
    
    public ReplicationConfig(String filename, BabuDBConfig babuConf) 
            throws IOException {
        super(filename);
        this.babuDBConfig = babuConf;
        read();
        
        this.fleaseConfig = new FleaseConfig(LEASE_TIMEOUT, this.localTimeRenew,
                MESSAGE_TIMEOUT, this.address,
                FleaseHolder.getIdentity(this.address), MAX_RETRIES);
        
        // get the replication policy
        try {
            this.replicationPolicy = (Policy) Policy.class.getClassLoader().
                        loadClass(POLICY_PACKAGE + policyName).newInstance();
            
        } catch (Exception e) {
            throw new IOException("ReplicationPolicy could not be " +
                        "instantiated, because: " + e.getMessage());
        }
    }
    
    public ReplicationConfig(Set<InetSocketAddress> participants, 
            int localTimeRenew, SSLOptions sslOptions, int syncN, 
            String tempDir, BabuDBConfig babuConf, boolean redirect) throws IOException {
        
        this.babuDBConfig = babuConf;
        this.participants = new HashSet<InetSocketAddress>();
        this.localTimeRenew = localTimeRenew;
        this.redirect = redirect;
        Socket s;
        for (InetSocketAddress participant : participants) {
            s = new Socket();
            try {
                s.bind(participant);
                if (address == null) {
                    address = participant;
                } else if (!address.equals(participant)) {
                    participants.add(participant);
                }
            } catch (Exception e) {
                participants.add(participant);
            } finally {
                try {
                    s.close();
                } catch (IOException e) { /* ignored */
                }
            }
        }
        
        this.sslOptions = sslOptions;
        this.syncN = syncN;
        this.chunkSize = DEFAULT_MAX_CHUNK_SIZE;
        this.backupDir = tempDir;
                
        this.fleaseConfig = new FleaseConfig(LEASE_TIMEOUT, this.localTimeRenew,
                MESSAGE_TIMEOUT, address, 
                FleaseHolder.getIdentity(address), MAX_RETRIES);
        
        checkArgs(babuDBConfig, address, sslOptions, participants, 
                localTimeRenew, timeSyncInterval, syncN, chunkSize, 
                this.backupDir, policyName);
        
        // get the replication policy
        try {
            replicationPolicy = (Policy) Policy.class.getClassLoader().
                        loadClass(POLICY_PACKAGE + policyName).newInstance();
            
        } catch (Exception e) {
            throw new IOException("ReplicationPolicy could not be " +
                        "instantiated, because: " + e.getMessage());
        }
    }
    
    private void read() throws IOException {
        
        String tempDir = this.readRequiredString("babudb.repl.backupDir");
        if (tempDir.endsWith("/") || tempDir.endsWith("\\")) {
            backupDir = tempDir;
        } else if (tempDir.contains("/")) {
            backupDir = tempDir + "/";
        } else if (tempDir.contains("\\")) {
            backupDir = tempDir + "\\";
        } else {
            backupDir = tempDir + File.separator;
        }
        
        this.localTimeRenew = this.readOptionalInt("babudb.localTimeRenew", 3000);
        this.redirect = readOptionalBoolean("babudb.repl.redirectIsVisible", false);
        this.timeSyncInterval = this.readOptionalInt("babudb.timeSync", 20000);
        
        if (this.readOptionalBoolean("babudb.ssl.enabled", false)) {
            this.sslOptions = new SSLOptions(new FileInputStream(this
                    .readRequiredString("babudb.ssl.service_creds")), this
                    .readRequiredString("babudb.ssl.service_creds.pw"), this
                    .readRequiredString("babudb.ssl.service_creds.container"), 
                            new FileInputStream(this
                    .readRequiredString("babudb.ssl.trusted_certs")), this
                    .readRequiredString("babudb.ssl.trusted_certs.pw"), this
                    .readRequiredString("babudb.ssl.trusted_certs.container"), 
                            this
                    .readRequiredBoolean(
                            "babudb.ssl.authenticationWithoutEncryption"));
        }
        
        this.chunkSize = this.readOptionalInt("babudb.repl.chunkSize", 
                DEFAULT_MAX_CHUNK_SIZE);
        
        this.syncN = this.readOptionalInt("babudb.repl.sync.n", 0);
        
        // read the participants
        this.participants = new HashSet<InetSocketAddress>();
        
        this.address = this.readOptionalInetSocketAddr("babudb.repl.localhost", 
                "babudb.repl.localport", null);
        
        int number = 0;
        Socket s;
        InetSocketAddress addrs;
        while ((addrs = readOptionalInetSocketAddr("babudb.repl.participant." + number,
                        "babudb.repl.participant." + number + ".port", null)) != null) {
            
            if (address == null) {
                s = new Socket();
                try {
                    s.bind(addrs);
                    address = addrs;
                } catch (Throwable t) {
                    // even if a participant's address is not reachable, or cannot
                    // be resolved, it never will be ignored completely, because
                    // it may become available later
                    participants.add(addrs);
                } finally {
                    try {
                        s.close();
                    } catch (IOException e) { /* ignored */ }
                }
            } else if (!address.equals(addrs)) {
                participants.add(addrs);
            }
            number++;
        }
        
        policyName = readOptionalString("babudb.repl.policy", "MasterOnly");
        
        checkArgs(babuDBConfig, address, sslOptions, participants, localTimeRenew, timeSyncInterval, 
                  syncN, chunkSize, backupDir, policyName);
    }
    
    public InetSocketAddress getInetSocketAddress() {
        return address;
    }
    
    public int getPort() {
        return address.getPort();
    }
    
    public InetAddress getAddress() {
        return address.getAddress();
    }
    
    public SSLOptions getSSLOptions() {
        return sslOptions;
    }
    
    /**
     * @return a set of addresses for participants of the replication setup without the address
     *         of the local instance.
     */
    public Set<InetSocketAddress> getParticipants() {
        return participants;
    }
    
    public int getLocalTimeRenew() {
        return localTimeRenew;
    }
    
    public int getTimeSyncInterval() {
        return timeSyncInterval;
    }
    
    public boolean redirectIsVisible() {
        return redirect;
    }
    
    public int getSyncN() {
        return syncN;
    }
    
    public int getChunkSize() {
        return chunkSize;
    }
    
    public String getTempDir() {
        return backupDir;
    }
    
    public FleaseConfig getFleaseConfig() {
        return fleaseConfig;
    }
    
    public BabuDBConfig getBabuDBConfig() {
        return babuDBConfig;
    }
    
    public Policy getReplicationPolicy() {
        return replicationPolicy;
    }
    
    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("############# Replication CONFIGURATION #############\n");
        buf.append("#             local address: " + address.toString() + "\n");
        
        if (this.sslOptions != null) {
            buf.append("#         ssl options: " + sslOptions.toString()+ "\n");
        }
        
        int i = 0;
        for (InetSocketAddress participant : this.participants) {
            buf.append("#                participant " + (i++) + ": " + 
                    participant.toString() + "\n");
        }
        buf.append("#        sync N: " + syncN + "\n");
        buf.append("#        policy: " + policyName + "\n");
        buf.append("#        local time renew: " + localTimeRenew + "\n");
        buf.append("#        time sync interval: " + timeSyncInterval + "\n");
        buf.append("#        temporary directory: " + backupDir + "\n");
        buf.append("#        chunk size: " + chunkSize + "\n");
        return buf.toString();
    }
    
    private static void checkArgs(BabuDBConfig conf, InetSocketAddress address, 
            SSLOptions options, Set<InetSocketAddress> participants, 
            int localTimeRenew, int timeSyncInterval, int syncN, int chunkSize, 
            String tempDir, String policyName) {
        
        File base = new File(conf.getBaseDir());
        File log = new File(conf.getDbLogDir());
        
        if (conf.getSyncMode() == SyncMode.ASYNC) {
            throw new IllegalArgumentException("Replication will not work properly if BabuDB " +
            		"instances run in ASYNC mode. Please change the SyncMode of your BabuDB " +
            		"setup.");
        }
        
        if (log.equals(base)) {
            throw new IllegalArgumentException("It is not permitted by the replication plugin to" +
            		" stored log and base files within the same directory. Please move either" +
            		"log (" + conf.getDbLogDir() + ") or base (" + conf.getBaseDir() + ").");
        }
        
        File tmp = new File(tempDir);
        while (tmp != null) {
            if (tmp.equals(base) || tmp.equals(log)) {
                throw new IllegalArgumentException("The backup-directory (" + tempDir 
                        + ") may not be a sub-directory of the dbLog-directory (" 
                        + conf.getDbLogDir() + ") and the base-directory (" + conf.getBaseDir() 
                        + ")!");
            }
            
            tmp = tmp.getParentFile();
        }
        
        if (address == null)
            throw new IllegalArgumentException(
                    "None of the given participants described the localhost!");
        
        if (syncN < 0 || syncN > (participants.size() + 1))
            throw new IllegalArgumentException(
                    "Wrong Sync-N! It has to be at least 0 and #of " +
                    "participants ("+(participants.size() + 1)+") at the maximum!");
        
        if (syncN != 0 && syncN <= participants.size() / 2)
            throw new IllegalArgumentException(
                    "The requested N-sync-mode (N=" + syncN + ") may cause " +
                    "inconsistent behavior, because there are '" + 
                    participants.size() + "' participants. The sync-N " + 
                    "has to be at least '" + (participants.size() / 2) + "'!");
        
        if (policyName != null && policyName != "") {
            try {
                Policy.class.getClassLoader().loadClass(
                        POLICY_PACKAGE + policyName);
            } catch (Exception e) {
                throw new IllegalArgumentException("Policy with name '" + 
                        policyName + "' could not be found at classpath.");
            }
        } else {
            throw new IllegalArgumentException("It is not possible to use " +
            		"replication without replication policy. The policy " +
            		"has at least to be set to default (MasterOnly)!");
        }
    }
}