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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseManager;
import org.xtreemfs.babudb.sandbox.ContinuesRandomGenerator.Operation;
import org.xtreemfs.babudb.sandbox.ContinuesRandomGenerator.InsertGroup;
import org.xtreemfs.include.common.config.ReplicationConfig;
import org.xtreemfs.include.common.logging.Logging;

/**
 * Random longrun-test for the Master-BabuDB.
 * 
 * @author flangner
 *
 */

public class ReplicationLongruntestMaster {

    public final static String PATH = ReplicationLongrunTestConfig.PATH+"master";
    public final static int NUM_WKS = 1;

    public final static int MAX_REPLICATION_Q_LENGTH = 0;
    
    // log-file size boundary in bytes (0,5 MB)
    public final static long MAX_LOG_FILE_SIZE = 512*1024; 
    // every 5 seconds
    public final static int CHECK_INTERVAL = 5;
    
    public final static String BACKUP_DIR = ReplicationLongrunTestConfig.PATH+"Mbackup";
    
    private static ContinuesRandomGenerator generator;
    private static BabuDB DBS;
    
    /**
     * 
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        System.out.println("LONGRUNTEST OF THE BABUDB in master-mode");
        Logging.start(Logging.LEVEL_WARN);
        
        if (args.length!=2) usage();
        
        // delete existing files
        Process p = Runtime.getRuntime().exec("rm -rf "+PATH);
        p.waitFor();        
        
        long seed = 0L;
        try{
            seed = Long.valueOf(args[0]);
        } catch (NumberFormatException e){
            error("Illegal seed: "+args[0]);
        }
        
        Set<InetSocketAddress> participants = new HashSet<InetSocketAddress>();    
        if (args[1].indexOf(",")==-1)
            participants.add(parseAddress(args[1]));
        else
            for (String adr : args[1].split(","))
                participants.add(parseAddress(adr));
        participants.add(new InetSocketAddress(InetAddress.getByAddress(
                new byte[]{127,0,0,1}),ReplicationInterface.DEFAULT_MASTER_PORT));
        
        
        generator = new ContinuesRandomGenerator(seed, ReplicationLongrunTestConfig.MAX_SEQUENCENO);
        
        Map<Integer,List<Object>> scenario = generator.getOperationsScenario();
        
        ReplicationConfig config = new ReplicationConfig(PATH, PATH, NUM_WKS, 
                MAX_LOG_FILE_SIZE, CHECK_INTERVAL, SyncMode.ASYNC, 0, 0, 
                ReplicationInterface.DEFAULT_MASTER_PORT, InetAddress.
                getByAddress(new byte[]{127,0,0,1}), participants, 50, null, 
                MAX_REPLICATION_Q_LENGTH, 0,BACKUP_DIR,false);
        
        DBS = (BabuDB) BabuDBFactory.createReplicatedBabuDB(config);
        
        DBS.getReplicationManager().declareToMaster();
        
        int nOmetaOp = 0;
        long metaOpTime = 0L;
        int nOinsertOp = 0;
        long insertOpTime = 0L;
        long time;
        
        for (int i=1;i<ReplicationLongrunTestConfig.MAX_SEQUENCENO;i++){
            
            List<Object> operation = scenario.get(i);
            if (operation!=null) {
                nOmetaOp++;
                time = System.currentTimeMillis();
                performOperation(operation);
                metaOpTime += System.currentTimeMillis() - time;
            } else {
                nOinsertOp++;
                time = System.currentTimeMillis();
                performInsert(i);
                insertOpTime += System.currentTimeMillis() - time;
            }
        }
        
        double metaTroughput = ((double)nOmetaOp)/(((double) metaOpTime)/1000.0);
        double insertThroughput = ((double)nOinsertOp)/(((double) insertOpTime)/1000.0);
        
        System.out.println("\nRESULTS --------------------------------------------");
        System.out.format("total throughput for meta-operations : %12.2f ops/s\n", metaTroughput);
        System.out.format("total throughput for inserts : %12.2f insertGroups/s\n", insertThroughput);
        
        // wait for the slave
        Thread.sleep(10*60*1000);
        
        DBS.shutdown();
    }
    
    /**
     * Performs an meta-operation on the BabuDB.
     * 
     * @param op
     * @throws BabuDBException
     * @throws IOException
     */
    private static void performOperation(List<Object> op) throws BabuDBException, IOException, InterruptedException {
        Operation opName = (Operation) op.get(0);
        
        DatabaseManager dbm = DBS.getDatabaseManager();
        
        switch (opName){
        case create:
            dbm.createDatabase((String) op.get(1), (Integer) op.get(2));
            break;
        case copy:
                dbm.copyDatabase((String) op.get(1), (String) op.get(2));
            break;
        case delete:
                dbm.deleteDatabase((String) op.get(1));
            break;
        default : throw new UnsupportedOperationException(opName.toString());
        }
    }
    
    /**
     * Performs an insert generated from the given sequence-number on the BabuDB.
     * @param seq
     * @throws Exception
     */
    private static void performInsert(long seq) throws Exception{
        InsertGroup isg = generator.getInsertGroup(seq);
        assert (isg != null) : "The requested operation seems to be a metaoperation.";
        
        Database db;
        try {
            db = DBS.getDatabaseManager().getDatabase(isg.dbName);
        } catch (BabuDBException e) {
            System.out.println("Error on: "+isg.toString()+"\n"+isg.dbName+"-"+seq+"\n"+
                    generator.toString());
            throw e;
        }
        BabuDBInsertGroup babuDBinsert = db.createInsertGroup();
        for (int i=0;i<isg.size();i++){
            if (i<isg.getNoInserts())
                babuDBinsert.addInsert(isg.getIndex(i), isg.getKey(i), isg.getValue(i));
            else
                babuDBinsert.addDelete(isg.getIndex(i), isg.getKey(i));
        }
        db.syncInsert(babuDBinsert);
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
        System.out.println("ReplicationLongrungtestMaster <seed> <participant_address:port>[,<participant_address:port>]");
        System.out.println("  "+"<seed> long value from which the scenario will be generated");
        System.out.println("  "+"<participant_address:port> participants of the replication separated by ','");
        System.exit(1);
    }
}
