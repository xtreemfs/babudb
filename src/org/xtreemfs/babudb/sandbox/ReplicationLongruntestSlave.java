/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package org.xtreemfs.babudb.sandbox;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.ReplicationManagerImpl;
import org.xtreemfs.babudb.replication.RequestDispatcher.DispatcherState;
import org.xtreemfs.babudb.sandbox.ContinuesRandomGenerator.LookupGroup;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.logging.Logging;

/**
 * Random longrun-test for the Slave-BabuDB.
 * 
 * @author flangner
 *
 */

public class ReplicationLongruntestSlave {

    // sum of P_* has to be 100, these values are probabilities for the events '*' stands for
    public final static int P_CLEAN_RESTART = 0;
    public final static int P_CONSISTENCY_CHECK = 100-(P_CLEAN_RESTART);
    
    // the interval to sleep, if any other event occurred before
    public final static int MIN_SLEEP_INTERVAL = ReplicationLongrunTestConfig.MIN_SLEEP_INTERVAL; 
    public final static int MAX_SLEEP_INTERVAL = ReplicationLongrunTestConfig.MAX_SLEEP_INTERVAL;
    
    public final static int MIN_DOWN_TIME = 15*1000;    
    public final static int MAX_DOWN_TIME = ReplicationLongrunTestConfig.MAX_DOWN_TIME;
    
    public final static String PATH = ReplicationLongrunTestConfig.PATH+"slave";
    public final static int NUM_WKS = 1;
    
    public final static int MAX_REPLICATION_Q_LENGTH = 0;
    public final static String BACKUP_DIR = ReplicationLongrunTestConfig.PATH+"Sbackup";
    
    private static ContinuesRandomGenerator generator;
    private static BabuDB DBS;
    private static ReplicationConfig CONFIGURATION;
    
    private static LSN latest = null;
    
    /**
     * 
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        System.out.println("LONGRUNTEST OF THE BABUDB in slave-mode");
        Logging.start(Logging.LEVEL_WARN);
        
        if (args.length!=2) usage();
        
        // delete existing files
        Process p = Runtime.getRuntime().exec("rm -rf "+PATH);
        p.waitFor(); 
        
        p = Runtime.getRuntime().exec("rm -rf "+BACKUP_DIR);
        p.waitFor(); 
        
        long seed = 0L;
        try{
            seed = Long.valueOf(args[0]);
        } catch (NumberFormatException e){
            error("Illegal seed: "+args[0]);
        }
        
        Set<InetSocketAddress> participants = new HashSet<InetSocketAddress>();    
        if (args[1].indexOf(",") == -1)
            participants.add(parseAddress(args[1]));
        else
            for (String adr : args[1].split(","))
                participants.add(parseAddress(adr));
        
        participants.add(new InetSocketAddress(InetAddress.getByAddress(
                new byte[]{127,0,0,1}),ReplicationInterface.DEFAULT_SLAVE_PORT));
        
        CONFIGURATION = new ReplicationConfig(PATH, PATH, NUM_WKS, 1, 0, SyncMode.ASYNC, 0, 0, 
                ReplicationInterface.DEFAULT_SLAVE_PORT, InetAddress.getByAddress(new byte[]{127,0,0,1}), participants, 50, null, 
                MAX_REPLICATION_Q_LENGTH, 0,BACKUP_DIR, false);
        
        DBS = (BabuDB) BabuDBFactory.createReplicatedBabuDB(CONFIGURATION);
        generator = new ContinuesRandomGenerator(seed, ReplicationLongrunTestConfig.MAX_SEQUENCENO);
        Random random = new Random();
        
        while (true) {
            int sleepInterval = random.nextInt(MAX_SLEEP_INTERVAL-MIN_SLEEP_INTERVAL)+MIN_SLEEP_INTERVAL;
            
            System.out.println("Thread will be suspended for "+sleepInterval/60000.0+" minutes.");
            Thread.sleep(sleepInterval);
        
            int event = random.nextInt(100);
            if (!((ReplicationManagerImpl) DBS.getReplicationManager()).isRunning()) {
                System.out.println("The slave is currently inactive.");
            } else if (event<P_CONSISTENCY_CHECK) {
                System.out.print("CONISTENCY CHECK:");
                performConsistencyCheck();  
            }else{
                System.out.print("CLEAN RESTART:");
                performCleanAndRestart(random);
            } 
        } 
    }
    
    /**
     * Restarts the BabuDB with complete data-loss.
     * Remains stopped a random down-time.
     * 
     * @param random
     * @throws IOException
     * @throws InterruptedException
     * @throws BabuDBException
     */
    private static void performCleanAndRestart(Random random) throws IOException, InterruptedException, BabuDBException{
        DispatcherState state = ((ReplicationManagerImpl) DBS.getReplicationManager()).stop();
        
        int downTime = random.nextInt(MAX_DOWN_TIME-MIN_DOWN_TIME)+MIN_DOWN_TIME;
        System.out.println("Slave is down for "+downTime/60000.0+" minutes.");
        Thread.sleep(downTime);
        
        // delete existing files
        Process p = Runtime.getRuntime().exec("rm -rf "+PATH);
        p.waitFor();
    
        ((ReplicationManagerImpl) DBS.getReplicationManager()).restart(state);
    }
    
    /**
     * Checks the last insert group of consistency.
     * @throws Exception
     */
    private static void performConsistencyCheck() throws Exception{
        DispatcherState state = ((ReplicationManagerImpl) DBS.getReplicationManager()).stop();
        if(latest != null && latest.equals(state.latest)) {
            System.out.println("Final synchronization with the master!");
            DBS.getReplicationManager().declareToMaster();
            System.exit(0);
        } else {
            latest = state.latest;
            System.out.println("Checking entry with LSN: "+state.latest);
            
            if (state.latest!=null && state.latest.getSequenceNo() > 0L){
                LookupGroup lookupGroup = generator.getLookupGroup(state.latest.getSequenceNo());
                if (lookupGroup != null) {
                    for (int i=0;i<lookupGroup.size();i++){
                        byte[] value = DBS.hiddenLookup(lookupGroup.dbName, lookupGroup.getIndex(i), lookupGroup.getKey(i));
                        // if the looked up entry is no delete ...
                        if (lookupGroup.getValue(i) != null) {
                            if (value==null) {
                                System.err.println("Could not check position: "+i);
                                System.err.println(lookupGroup.toString());
                                System.err.println((value == null) ? "The looked up value was null" : "The Random-Generator-value was null");
                            } else {                       
                                if (!new String(value).equals(new String(lookupGroup.getValue(i)))) {
                                    System.err.println("FAILED for LSN ("+state.latest.toString()+")!" +
                                            "\n"+new String(value)+" != "+new String(lookupGroup.getValue(i)));
                                    System.exit(1);
                                } 
                            }
                        }
                    }
                    System.out.println("SUCCESSFUL for LSN ("+state.latest.toString()+").");
                } else {
                    System.out.println("Unable to perform lookup.LSN "+state.latest.toString()+" describes a meta-operation.");
                }
            } else {
                System.out.println("Unable to perform lookup.LSN "+state.latest.toString()+" describes a meta-operation.");
            }
        }
        
        ((ReplicationManagerImpl) DBS.getReplicationManager()).restart(state);
    } 
    
    /**
     * Can exit with an error, if the given string was illegal.
     * 
     * @param adr
     * @return the parsed {@link InetSocketAddress}.
     */
    private static InetSocketAddress parseAddress (String adr){
        String[] comp = adr.split(":");
        if (comp.length!=2){
            error("Address '"+adr+"' is illegal!");
            return null;
        }
        
        try {
            int port = Integer.parseInt(comp[1]);
            return new InetSocketAddress(comp[0],port);
        } catch (NumberFormatException e) {
            error("Address '"+adr+"' is illegal! Because: "+comp[1]+" is not a number.");
            return null;
        }      
    }
    
    /**
     * Prints the error <code>message</code> and delegates to usage().
     * @param message
     */
    private static void error(String message) {
        System.err.println(message);
        usage();
    }
    
    /**
     *  Prints out usage informations and terminates the application.
     */
    public static void usage(){
            System.out.println("BabuDBRandomSlaveTest <seed> <participant_address:port>[,<participant_address:port>]");
            System.out.println("  "+"<seed> long value from which the scenario will be generated");
            System.out.println("  "+"<participant_address:port> replication participants separated by ','");
            System.exit(1);
    }
}
