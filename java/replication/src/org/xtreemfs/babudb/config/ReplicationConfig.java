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
import org.xtreemfs.foundation.logging.Logging;

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
    
    protected final FleaseConfig     fleaseConfig;
    
    protected String                 policyName;
    protected final Policy           replicationPolicy;
    
    /** 
     * longest duration a connection can be established without getting closed 
     */
    public static final int          FLEASE_LEASE_TIMEOUT_MS          = 60 * 1000;
        
    /**
     * Maximum assumed drift between two server clocks. If the drift is higher, the system may not function
     * properly.
     */
    public static final int         FLEASE_DMAX_MS        = 1000;
    
    /** 
     * longest duration a message can live on the wire, before it becomes void (one-way)
     */
    public static final int         FLEASE_MESSAGE_TO_MS        = 500;
    
    /** Maximal retries per failure. */
    public static final int          FLEASE_RETRIES            = 3;
    
    /** 
     * longest duration before an RPC-Call is timed out for the client (RTT) 
     */
    public static final int          REQUEST_TIMEOUT        = 2 * FLEASE_MESSAGE_TO_MS;
    
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
     * Proxy retry parameters.
     */
    /** 0 means infinitely */
    public static final int          PROXY_MAX_RETRIES      = 15;
    public static final int          PROXY_RETRY_DELAY      = 10 * 1000;
    
    public ReplicationConfig(Properties prop, BabuDBConfig babuConf) 
            throws IOException {
        super(prop);
        this.babuDBConfig = babuConf;
        read();
        
        this.fleaseConfig = createFleaseConfig();
        
        // get the replication policy
        try {
            this.replicationPolicy = (Policy) Policy.class.getClassLoader().
                        loadClass(POLICY_PACKAGE + policyName).newInstance();
            
        } catch (Exception e) {
            throw new IOException("ReplicationPolicy could not be " +
            		"instantiated, because: " + e.getMessage());
        }
    }

    private FleaseConfig createFleaseConfig() {
        return new FleaseConfig(FLEASE_LEASE_TIMEOUT_MS, FLEASE_DMAX_MS, FLEASE_MESSAGE_TO_MS, this.address,
                FleaseHolder.getIdentity(this.address), FLEASE_RETRIES);
    }
    
    public ReplicationConfig(String filename, BabuDBConfig babuConf) 
            throws IOException {
        super(filename);
        this.babuDBConfig = babuConf;
        read();
        
        this.fleaseConfig = createFleaseConfig();
        
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
        // All replicas except this node.
        this.participants = new HashSet<InetSocketAddress>();
        this.localTimeRenew = localTimeRenew;
        this.redirect = redirect;
        
        this.sslOptions = sslOptions;
        this.syncN = syncN;
        this.chunkSize = DEFAULT_MAX_CHUNK_SIZE;
        this.backupDir = tempDir;
                
        this.fleaseConfig = createFleaseConfig();
        
        parseParticipants();
        checkArgs();
        
        // get the replication policy
        try {
            replicationPolicy = (Policy) Policy.class.getClassLoader().
                        loadClass(POLICY_PACKAGE + policyName).newInstance();
            
        } catch (Exception e) {
            throw new IOException("ReplicationPolicy could not be " +
                        "instantiated, because: " + e.getMessage());
        }
    }

    private void parseParticipants() {
        Set<InetSocketAddress> finalParticipants = new HashSet<InetSocketAddress>();
        // Find myself from the list of participants, set as address and remove me.
        for (InetSocketAddress participant : participants) {
            if (address == null) {
                Socket s = new Socket();
                try {
                    s.setReuseAddress(true);
                    s.bind(participant);
                    
                    // Can listen to this IP/Port - found myself.
                    address = participant;
                } catch (Exception e) {
                    // Another host.
                    finalParticipants.add(participant);
                } finally {
                    try {
                        s.close();
                    } catch (IOException e) { /* ignored */
                    }
                }
            } else if (address.equals(participant)) {
                // Participants do not include the local node.
            } else {
                finalParticipants.add(participant);
            }
        }
        
        participants = finalParticipants;
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
        
        this.localTimeRenew = this.readOptionalInt("babudb.localTimeRenew", 0);
        this.redirect = readOptionalBoolean("babudb.repl.redirectIsVisible", false);
        
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
        InetSocketAddress addrs;
        while ((addrs = readOptionalInetSocketAddr("babudb.repl.participant." + number,
                                                   "babudb.repl.participant." + number + ".port", null)) != null) {
            participants.add(addrs);
            number++;
        }
        
        parseParticipants();
        
        policyName = readOptionalString("babudb.repl.policy", "MasterOnly");
        
        checkArgs();
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
        buf.append("#        temporary directory: " + backupDir + "\n");
        buf.append("#        chunk size: " + chunkSize + "\n");
        return buf.toString();
    }
    
    private void checkArgs(
            /*BabuDBConfig conf, InetSocketAddress address, 
            SSLOptions options, Set<InetSocketAddress> participants, 
            int localTimeRenew, int syncN, int chunkSize, 
            String tempDir, String policyName*/
            ) {
        
        File base = new File(babuDBConfig.getBaseDir());
        File log = new File(babuDBConfig.getDbLogDir());
        int numberOfReplicas = participants.size() + 1;
        
        if (babuDBConfig.getSyncMode() == SyncMode.ASYNC) {
            throw new IllegalArgumentException("Replication will not work properly if BabuDB " +
            		"instances run in ASYNC mode. Please change the SyncMode of your BabuDB " +
            		"setup e.g., to FSYNCDATA.");
        }
        
        if (log.equals(base)) {
            throw new IllegalArgumentException("It is not permitted by the replication plugin to" +
            		" store log and base files within the same directory. Please move either" +
            		"log (" + babuDBConfig.getDbLogDir() + ") or base (" + babuDBConfig.getBaseDir() + ").");
        }
        
        File tmp = new File(backupDir);
        while (tmp != null) {
            if (tmp.equals(base) || tmp.equals(log)) {
                throw new IllegalArgumentException("The backup-directory (" + backupDir 
                        + ") must not be a sub-directory of the dbLog-directory (" 
                        + babuDBConfig.getDbLogDir() + ") or the base-directory (" + babuDBConfig.getBaseDir() 
                        + ").");
            }
            
            tmp = tmp.getParentFile();
        }
        
        if (address == null)
            throw new IllegalArgumentException(
                    "None of the given participants described the local node or the configured port is used by another application.");
        
        
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
                        "has at least to be set to default (MasterOnly).");
        }
        
        if (syncN < 0 || syncN > numberOfReplicas) {
            throw new IllegalArgumentException(
                "Wrong Sync-N. It has to be at least 0 and #of " +
                "participants ("+ numberOfReplicas +") at the maximum.");
        }
        if (policyName == "WriteRestriction" && syncN != numberOfReplicas) {
            Logging.logMessage(Logging.LEVEL_INFO, null,
                    "If you set the policy to 'WriteRestriction', clients will be able to read stale data" +
                    " unless babudb.repl.sync.n equals the number of replicas (currently set to: %d).", numberOfReplicas);
        }
        if (policyName == "MasterOnly" && syncN < (numberOfReplicas + 1) / 2) {
            throw new IllegalArgumentException(
                    "The requested N-sync-mode (N=" + syncN + ") may cause " +
                    "inconsistent behavior, because at least a majority of replicas has to be updated." +
                    " As there are " + numberOfReplicas + " replicas, the babudb.repl.sync.n parameter has to be set at least to: " + ((numberOfReplicas + 1) / 2) +
                    " Alternatively, use another policy then 'MasterOnly' which do not provide strong consistenty.");
        }
    }
}