/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                   Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * All rights reserved.
 */

package org.xtreemfs.include.common.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
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

public class ReplicationConfig extends BabuDBConfig {
    
    protected InetSocketAddress address;
        
    protected SSLOptions        sslOptions;
    
    protected Set<InetSocketAddress>   participants;
    
    protected int               localTimeRenew;
    
    // for master usage only
    
    protected int               syncN;
    
    /** Chunk size, for initial load of file chunks. */
    protected int               chunkSize;
    
    public final static int     DEFAULT_MAX_CHUNK_SIZE = 5*1024*1024;
        
    // for slave usage only
    
    protected String            backupDir;
    
    /** Error message. */
    public static final String  slaveProtection = "You are not allowed to " +
    		"proceed this operation, because this DB is not running in " +
    		"master-mode!";
        
    public ReplicationConfig() {
        super();
    }
    
    public ReplicationConfig(Properties prop) throws IOException {
        super(prop);
        read();
    }
    
    public ReplicationConfig(String filename) throws IOException {
        super(filename);
        read();
    }
    
    public ReplicationConfig(String baseDir, String logDir, int numThreads, 
            long maxLogFileSize, int checkInterval, SyncMode mode, 
            int pseudoSyncWait, int maxQ, Set<InetSocketAddress> participants, 
            int localTimeRenew, SSLOptions sslOptions, int syncN, 
            String backupDir, boolean compression) {
        
        super(baseDir, logDir, numThreads, maxLogFileSize, checkInterval, mode, 
                pseudoSyncWait, maxQ, compression);
        this.participants = new HashSet<InetSocketAddress>();
        this.localTimeRenew = localTimeRenew;
        Socket s;
        for (InetSocketAddress participant : participants){
            s = new Socket();
            try {
                s.bind(participant);
                if (this.address != null && !this.address.equals(participant)) throw new BindException();
                this.address = participant;
            } catch (Exception e) {
                this.participants.add(participant);
            } finally {
                try {
                    s.close();
                } catch (IOException e) { /* ignored */ }
            }
        }
        assert (this.address != null) : "No one of the given participants " +
        		"described the localhost!";
        this.sslOptions = sslOptions;
        this.syncN = syncN;
        this.chunkSize = DEFAULT_MAX_CHUNK_SIZE;
        this.backupDir = backupDir;
    }
    
    public void read() throws IOException {
        super.read();
               
        this.localTimeRenew = this.readOptionalInt("babudb.localTimeRenew", 3000);
        
        if (this.readRequiredBoolean("babudb.ssl.enabled")) {
            this.sslOptions = new SSLOptions(
                    new FileInputStream(
                            this.readRequiredString("babudb.ssl.service_creds")),
                    this.readRequiredString("babudb.ssl.service_creds.pw"), 
                    this.readRequiredString("babudb.ssl.service_creds.container"),
                    new FileInputStream(
                            this.readRequiredString("babudb.ssl.trusted_certs")),
                    this.readRequiredString("babudb.ssl.trusted_certs.pw"),
                    this.readRequiredString("babudb.ssl.trusted_certs.container"),
                    this.readRequiredBoolean(
                            "babudb.ssl.authenticationWithoutEncryption"));
        }
        
        // read the participants
        this.participants = new HashSet<InetSocketAddress>();
        
        int number = 0;
        Socket s;
        InetSocketAddress addrs;
        while ((addrs = this.readOptionalInetSocketAddr(
                "babudb.repl.participant."+number, 
                "babudb.repl.participant."+number+".port",null))!=null){
            s = new Socket();
            try {
                s.bind(addrs);
                if (this.address != null && !this.address.equals(addrs)) 
                    throw new BindException();
                this.address = addrs;
            } catch (BindException e) {
                this.participants.add(addrs);
            } finally {
                try {
                    s.close();
                } catch (IOException e) { /* ignored */ }
            }
            number++;
        }
        if (this.address == null) throw new IOException("None of the given " +
        		"participants described the localhost!");
        
        this.chunkSize = this.readOptionalInt("babudb.repl.chunkSize", 
                DEFAULT_MAX_CHUNK_SIZE);
        
        this.syncN = this.readOptionalInt("babudb.repl.sync.n", 0);
        
        if (this.syncN < 0 || this.syncN > this.participants.size()) throw new
            IOException("Wrong Sync-N! It has to be at least 0 and #of "+
                        "participants at the maximum!");
        
        if (this.syncN != 0 && this.syncN <= participants.size()/2)
            throw new IOException("The requested N-sync-mode (N="+this.syncN+")" +
            		" may cause inconsistent behavior, because there are '" +
            		this.participants.size()+"' participants. The sync-N " +
            		"has to be at least '"+(this.participants.size()/2)+1+"'!");
        
        String backupDir = this.readRequiredString("babudb.repl.backupDir");
        if (backupDir.equals(baseDir) || backupDir.equals(dbLogDir)) 
            throw new IOException("The backup-directory has to be different to " +
            		"the dbLog-directory and the base-directory!");   
        
        this.backupDir = (backupDir.endsWith(File.separator)) ? backupDir : 
            backupDir+File.separator;
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
    
    public Set<InetSocketAddress> getParticipants(){
        return this.participants;
    }

    public int getLocalTimeRenew() {
        return localTimeRenew;
    }
    
    public int getSyncN(){
        return this.syncN;
    }
    
    public int getChunkSize() {
        return this.chunkSize;
    }
    
    public String getBackupDir() {
        return backupDir;
    }
}