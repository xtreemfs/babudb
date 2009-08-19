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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.interfaces.ReplicationInterface.ReplicationInterface;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.Database;
import org.xtreemfs.babudb.lsmdb.DatabaseManager;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.sandbox.ContinuesRandomGenerator.Operation;
import org.xtreemfs.babudb.sandbox.RandomGenerator.InsertGroup;
import org.xtreemfs.include.common.config.MasterConfig;
import org.xtreemfs.include.common.logging.Logging;

/**
 * Random longrun-test for the Master-BabuDB.
 * 
 * @author flangner
 *
 */

public class BabuDBRandomMasterTest {
    public final static String PATH = BabuDBLongrunTestConfig.PATH+"master";
    public final static int NUM_WKS = 1;

    public final static int MAX_REPLICATION_Q_LENGTH = 0;
    
    private final static RandomGenerator generator = new RandomGenerator();
    private static BabuDB DBS;

    private BabuDBRandomMasterTest() {}
	
    public static void main(String[] args) throws Exception {
        Logging.start(Logging.LEVEL_ERROR);
		
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
        
        List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();    
        if (args[1].indexOf(",")==-1)
            slaves.add(parseAddress(args[1]));
        else
            for (String adr : args[1].split(","))
                slaves.add(parseAddress(adr));
        
		MasterConfig config = new MasterConfig(PATH, PATH, NUM_WKS, 1, 0, SyncMode.ASYNC, 0, 0, 
                ReplicationInterface.DEFAULT_MASTER_PORT, InetAddress.getLocalHost(), 
                new InetSocketAddress(InetAddress.getLocalHost(), ReplicationInterface.DEFAULT_MASTER_PORT), slaves, 
                50, null, MAX_REPLICATION_Q_LENGTH, 0);
        
        DBS = (BabuDB) BabuDBFactory.createMasterBabuDB(config);
        
        Map<Integer, List<List<Object>>> scenario = generator.initialize(seed);
        Random random = new Random();
        assert (RandomGenerator.MAX_SEQUENCENO<((long) Integer.MAX_VALUE)) : "This test cannot handle such a big MAX_SEQUENCENO.";
		
        int nOmetaOp = 0;
        long metaOpTime = 0L;
        int nOinsertOp = 0;
        long insertOpTime = 0L;
        long time;
		
		
        System.out.println("BabuDBRandomMaster-Longruntest----------------------");
        for (int viewID=1;viewID<=RandomGenerator.MAX_VIEWID;viewID++){
            System.out.print("Performing meta-operations for viewID '"+viewID+"' ...");
            time = System.currentTimeMillis();
            int metaSeq = 0;
            for (List<Object> operation : scenario.get(viewID)){
                performOperation(operation);
                nOmetaOp++;
                metaSeq++;
            }
            metaOpTime += System.currentTimeMillis() - time;
			
            System.out.println("done.");
			
            int nOsequenceNO = random.nextInt((int)(RandomGenerator.MAX_SEQUENCENO-RandomGenerator.MIN_SEQUENCENO))+(int) RandomGenerator.MIN_SEQUENCENO;
            System.out.print("Performing "+nOsequenceNO+" insert/delete operations ...");
            LSN lsn = new LSN(0,0L);
            time = System.currentTimeMillis();
            for (int seqNo=(metaSeq+1);seqNo<=nOsequenceNO;seqNo++){
                lsn = new LSN(viewID,(long) seqNo);
                performInsert(lsn);
                nOinsertOp++;
            }
            insertOpTime += System.currentTimeMillis() - time;
            System.out.println("done. Last insert was LSN ("+lsn.toString()+").");
			
            DBS.replication_stop();
            DBS.replication_changeConfiguration(config);
        }
        double metaTroughput = ((double)nOmetaOp)/(((double) metaOpTime)/1000.0);
        double insertThroughput = ((double)nOinsertOp)/(((double) insertOpTime)/1000.0);
        
		System.out.println("\nRESULTS --------------------------------------------");
        System.out.format("total throughput for meta-operations : %12.2f ops/s\n", metaTroughput);
        System.out.format("total throughput for inserts : %12.2f insertGroups/s\n", insertThroughput);
	
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
	 * Performs an insert generated from the given {@link LSN} on the BabuDB.
	 * @param lsn
	 * @throws Exception
	 */
	private static void performInsert(LSN lsn) throws Exception{
		InsertGroup isg = generator.getInsertGroup(lsn);
		
		Database db = DBS.getDatabaseManager().getDatabase(isg.dbName);
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
     *	Prints out usage informations and terminates the application.
     */
	public static void usage(){
        System.out.println("BabuDBRandomMasterTest <seed> <slave_address:port>[,<slave_address:port>]");
        System.out.println("  "+"<seed> long value from which the scenario will be generated");
        System.out.println("  "+"<slave_address:port> same as for the master for all available slaves separated by ','");
		System.exit(1);
	}
}
