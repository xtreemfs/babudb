/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package org.xtreemfs.babudb.sandbox;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.BabuDBImpl;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.replication.Replication;
import org.xtreemfs.babudb.sandbox.RandomGenerator.LookupGroup;
import org.xtreemfs.include.common.logging.Logging;

/**
 * Random longrun-test for the Slave-BabuDB.
 * 
 * @author flangner
 *
 */

public class BabuDBRandomSlaveTest {
	
	// sum of P_* has to be 100, these values are probabilities for the events '*' stands for
	public final static int P_RESTART = 20;
	public final static int P_CLEAN_RESTART = 10;
	public final static int P_CCHECK = 100-(P_RESTART+P_CLEAN_RESTART);
	
	// the interval to sleep, if consistency-check has occurred before
	public final static int CCHECK_SLEEP_INTERVAL = 3*60*1000;
	
	// the interval to sleep, if any other event occurred before
	public final static int MIN_SLEEP_INTERVAL = 20*60*1000;
	public final static int MAX_SLEEP_INTERVAL = 30*60*1000;
	
	public final static int MIN_DOWN_TIME = 60*1000;
	public final static int MAX_DOWN_TIME = 10*60*1000;
	
	public final static String PATH = "/tmp/babuDB/slave"; // /scratch/babuDB/data/slave
	public final static int NUM_WKS = 1;
	
	private final static RandomGenerator generator = new RandomGenerator();
	private static BabuDBImpl DBS;

	private BabuDBRandomSlaveTest() {}
	
	public static void main(String[] args) throws Exception {
	    Logging.start(Logging.LEVEL_ERROR);
		
	    if (args.length!=3) usage();
		
            long seed = 0L;
            try{
            	seed = Long.valueOf(args[0]);
            } catch (NumberFormatException e){
            	error("Illegal seed: "+args[0]);
            }
            
            InetSocketAddress master = parseAddress(args[1]);
            
            List<InetSocketAddress> slaves = new LinkedList<InetSocketAddress>();    
            if (args[2].indexOf(",")==-1)
                slaves.add(parseAddress(args[2]));
            else
                for (String adr : args[2].split(","))
                    slaves.add(parseAddress(adr));
    		
            // delete existing files
            Process p = Runtime.getRuntime().exec("rm -rf "+PATH);
            p.waitFor();
            
            DBS = (BabuDBImpl) BabuDBFactory.getSlaveBabuDB(PATH, PATH, NUM_WKS, 1, 0, SyncMode.ASYNC, 0, 0, master, slaves, Replication.SLAVE_PORT, null, Replication.DEFAULT_MAX_Q);
            generator.initialize(seed);
            Random random = new Random();
            
            System.out.println("BabuDBRandomSlave-Longruntest----------------------");
            
            boolean ccheck = true;
            while (true) {
            	int sleepInterval = 0;
            	if (ccheck)
            		sleepInterval = CCHECK_SLEEP_INTERVAL;
            	else
            		sleepInterval = random.nextInt(MAX_SLEEP_INTERVAL-MIN_SLEEP_INTERVAL)+MIN_SLEEP_INTERVAL;
            	
            	System.out.println("Thread will be suspended for "+sleepInterval/60000+" minutes.");
            	Thread.sleep(sleepInterval);
            
            	int event = random.nextInt(100);
            	if (event<P_CCHECK) {
            		performConsistencyCheck();        		
            		ccheck = true;
            	}else if (event<(P_CCHECK+P_CLEAN_RESTART)){
            		performCleanAndRestart(random, master, slaves);
            		ccheck = false;
            	}else{
            		performRestart(random, master, slaves);
            		ccheck = false;
            	}
            }
	}
	
	/**
	 * Restarts the BabuDB. Remains stopped a random down-time.
	 * 
	 * @param master
	 * @param slaves
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws BabuDBException
	 */
	private static void performRestart(Random random, InetSocketAddress master, List<InetSocketAddress> slaves) throws IOException, InterruptedException, BabuDBException{
	    DBS.shutdown();
		
	    int downTime = random.nextInt(MAX_DOWN_TIME-MIN_DOWN_TIME)+MIN_DOWN_TIME;
	    System.out.println("Slave is down for "+downTime/60000+" minutes.");
	    Thread.sleep(downTime);
		
            DBS = (BabuDBImpl) BabuDBFactory.getSlaveBabuDB(PATH, PATH, NUM_WKS, 1, 0, SyncMode.ASYNC, 0, 0, master, slaves, Replication.SLAVE_PORT, null, Replication.DEFAULT_MAX_Q);
            System.out.println("Restart performed.");
	}
	
	/**
	 * Restarts the BabuDB with complete data-loss.
	 * Remains stopped a random down-time.
	 * 
	 * @param master
	 * @param slaves
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws BabuDBException
	 */
	private static void performCleanAndRestart(Random random, InetSocketAddress master, List<InetSocketAddress> slaves) throws IOException, InterruptedException, BabuDBException{
	    DBS.shutdown();
		
	    int downTime = random.nextInt(MAX_DOWN_TIME-MIN_DOWN_TIME)+MIN_DOWN_TIME;
	    System.out.println("Slave is down for "+downTime/60000+" minutes.");
	    Thread.sleep(downTime);
		
            // delete existing files
            Process p = Runtime.getRuntime().exec("rm -rf "+PATH);
            p.waitFor();
        
            DBS = (BabuDBImpl) BabuDBFactory.getSlaveBabuDB(PATH, PATH, NUM_WKS, 1, 0, SyncMode.ASYNC, 0, 0, master, slaves, Replication.SLAVE_PORT, null, Replication.DEFAULT_MAX_Q);
            System.out.println("Clean restart performed.");
	}
	
	/**
	 * Checks the last insert group of consistency.
	 * @throws Exception
	 */
	private static void performConsistencyCheck() throws Exception{
	    LSN last = DBS.replication_pause();
	    
	    if (last!=null){
	        LookupGroup lookupGroup = generator.getLookupGroup(last);
		for (int i=0;i<lookupGroup.size();i++){
		    byte[] value = DBS.hiddenLookup(lookupGroup.dbName, lookupGroup.getIndex(i), lookupGroup.getKey(i));
		    if (!new String(value).equals(new String(lookupGroup.getValue(i)))) {
			System.err.println("ConsistencyCheck failed for LSN ("+last.toString()+")!" +
					"\n"+new String(value)+" != "+new String(lookupGroup.getValue(i)));
			System.exit(1);
		    }
		}
		System.out.println("ConsistencyCheck for LSN ("+last.toString()+") was successful.");
	    }
	    
	    DBS.replication_resume();
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
            System.out.println("BabuDBRandomSlaveTest <seed> <master_address:port> <slave_address:port>[,<slave_address:port>]");
            System.out.println("  "+"<seed> long value from which the scenario will be generated");
            System.out.println("  "+"<master_address:port> the IP or URL to the master BabuDB and the port it is listening at");
            System.out.println("  "+"<slave_address:port> same as for the master for all available slaves separated by ','");
            System.exit(1);
	}
}
