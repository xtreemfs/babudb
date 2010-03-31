/*
 * Copyright (c) 2008-2010, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.replication.ReplicationControlLayer;
import org.xtreemfs.foundation.flease.FleaseConfig;
import org.xtreemfs.include.foundation.SSLOptions;

/**
 * Reading configurations from the replication-config-file.
 * 
 * @since 05/02/2009
 * @author flangner
 * 
 */

public class ReplicationConfig extends BabuDBConfig {
    
    protected InetSocketAddress      address;
    
    protected SSLOptions             sslOptions;
    
    protected Set<InetSocketAddress> participants;
    
    protected int                    localTimeRenew;
    
    protected int                    timeSyncInterval;
    
    protected final FleaseConfig     fleaseConfig;
    
    /** longest duration a message can live on the wire, before it becomes void */
    private static final int         MESSAGE_TIMEOUT        = 10 * 1000;
    
    /** longest duration a connection can be established without getting closed */
    public static final int          LEASE_TIMEOUT          = 3 * 60 * 1000;
        
    /** longest duration before an RPC-Call is timed out */
    public static final int          REQUEST_TIMEOUT        = 30 * 1000;
    
    /** longest duration before an idle connection is closed */
    public static final int          CONNECTION_TIMEOUT     = 5 * 60 * 1000;
    
    // for master usage only
    
    protected int                    syncN;
    
    /** Chunk size, for initial load of file chunks. */
    protected int                    chunkSize;
    
    public final static int          DEFAULT_MAX_CHUNK_SIZE = 5 * 1024 * 1024;
    
    // for slave usage only
    
    protected String                 backupDir;
    
    /** Error message. */
    public static final String       slaveProtectionMsg = "You are not allowed"+
    		" to process this operation, because this DB is not running in"+
                " master-mode!";
    
    /** maximal retries per failure */
    public static final int          MAX_RETRIES            = 3;
    
    public ReplicationConfig(Properties prop) throws IOException {
        super(prop);
        read();
        
        this.fleaseConfig = new FleaseConfig(LEASE_TIMEOUT, this.localTimeRenew, 
                MESSAGE_TIMEOUT, this.address, 
                ReplicationControlLayer.getIdentity(this.address), MAX_RETRIES);
    }
    
    public ReplicationConfig(String filename) throws IOException {
        super(filename);
        read();
        
        this.fleaseConfig = new FleaseConfig(LEASE_TIMEOUT, this.localTimeRenew,
                MESSAGE_TIMEOUT, this.address,
                ReplicationControlLayer.getIdentity(this.address), MAX_RETRIES);
    }
    
    public ReplicationConfig(String baseDir, String logDir, int numThreads, long maxLogFileSize,
        int checkInterval, SyncMode mode, int pseudoSyncWait, int maxQ, Set<InetSocketAddress> participants,
        int localTimeRenew, SSLOptions sslOptions, int syncN, String backupDir, boolean compression,
        int maxNumRecordsPerBlock, int maxBlockFileSize) {
        
        super(baseDir, logDir, numThreads, maxLogFileSize, checkInterval, mode, pseudoSyncWait, maxQ,
            compression, maxNumRecordsPerBlock, maxBlockFileSize);
        
        this.participants = new HashSet<InetSocketAddress>();
        this.localTimeRenew = localTimeRenew;
        Socket s;
        for (InetSocketAddress participant : participants) {
            s = new Socket();
            try {
                s.bind(participant);
                if (this.address != null && !this.address.equals(participant))
                    throw new BindException();
                this.address = participant;
            } catch (Exception e) {
                this.participants.add(participant);
            } finally {
                try {
                    s.close();
                } catch (IOException e) { /* ignored */
                }
            }
        }
        assert (this.address != null) : "None of the given participants " + 
                                        "described the localhost!";
        this.sslOptions = sslOptions;
        this.syncN = syncN;
        this.chunkSize = DEFAULT_MAX_CHUNK_SIZE;
        this.backupDir = backupDir;
                
        this.fleaseConfig = new FleaseConfig(LEASE_TIMEOUT, this.localTimeRenew,
                MESSAGE_TIMEOUT, this.address, 
                ReplicationControlLayer.getIdentity(this.address), MAX_RETRIES);
    }
    
    public void read() throws IOException {
        super.read();
        
        this.localTimeRenew = this.readOptionalInt("babudb.localTimeRenew", 100);
        this.timeSyncInterval = this.readOptionalInt("babudb.timeSync", 20000);
        
        if (this.readRequiredBoolean("babudb.ssl.enabled")) {
            this.sslOptions = new SSLOptions(new FileInputStream(this
                    .readRequiredString("babudb.ssl.service_creds")), this
                    .readRequiredString("babudb.ssl.service_creds.pw"), this
                    .readRequiredString("babudb.ssl.service_creds.container"), new FileInputStream(this
                    .readRequiredString("babudb.ssl.trusted_certs")), this
                    .readRequiredString("babudb.ssl.trusted_certs.pw"), this
                    .readRequiredString("babudb.ssl.trusted_certs.container"), this
                    .readRequiredBoolean("babudb.ssl.authenticationWithoutEncryption"));
        }
        
        // read the participants
        this.participants = new HashSet<InetSocketAddress>();
        
        int number = 0;
        Socket s;
        InetSocketAddress addrs;
        while ((addrs = this.readOptionalInetSocketAddr("babudb.repl.participant." + number,
            "babudb.repl.participant." + number + ".port", null)) != null) {
            
            s = new Socket();
            try {
                s.bind(addrs);
                if (this.address != null && !this.address.equals(addrs))
                    throw new BindException();
                this.address = addrs;
            } catch (BindException e) {
                this.participants.add(addrs);
            } catch (Exception e) {
                System.err.println("'"+addrs+"' will be ignored, because: " + 
                        e.getMessage());
            } finally {
                try {
                    s.close();
                } catch (IOException e) { /* ignored */ }
            }
            number++;
        }
        if (this.address == null)
            throw new IOException("None of the given participants described" +
            		" the localhost!");
        
        this.chunkSize = this.readOptionalInt("babudb.repl.chunkSize", DEFAULT_MAX_CHUNK_SIZE);
        
        this.syncN = this.readOptionalInt("babudb.repl.sync.n", 0);
        
        if (this.syncN < 0 || this.syncN > participants.size())
            throw new IOException("Wrong Sync-N! It has to be at least 0 and" +
            		" #of participants ("+this.participants.size()+") at" +
            	        " the maximum!");
        
        if (this.syncN != 0 && this.syncN <= participants.size() / 2)
            throw new IOException("The requested N-sync-mode (N=" + syncN + ")"
                + " may cause inconsistent behavior, because there are '" + participants.size()
                + "' participants. The sync-N " + "has to be at least '" + (participants.size() / 2)
                + "'!");
        
        String backupDir = this.readRequiredString("babudb.repl.backupDir");
        if (backupDir.equals(baseDir) || backupDir.equals(dbLogDir))
            throw new IOException("The backup-directory has to be different to "
                + "the dbLog-directory and the base-directory!");
        
        this.backupDir = (backupDir.endsWith(File.separator)) ? backupDir : backupDir + File.separator;
    }
    
    public InetSocketAddress getInetSocketAddress() {
        return this.address;
    }
    
    public int getPort() {
        return this.address.getPort();
    }
    
    public InetAddress getAddress() {
        return this.address.getAddress();
    }
    
    public SSLOptions getSSLOptions() {
        return this.sslOptions;
    }
    
    public Set<InetSocketAddress> getParticipants() {
        return this.participants;
    }
    
    public int getLocalTimeRenew() {
        return localTimeRenew;
    }
    
    public int getTimeSyncInterval() {
        return timeSyncInterval;
    }
    
    public int getSyncN() {
        return this.syncN;
    }
    
    public int getChunkSize() {
        return this.chunkSize;
    }
    
    public String getBackupDir() {
        return backupDir;
    }
    
    public FleaseConfig getFleaseConfig() {
        return fleaseConfig;
    }
}