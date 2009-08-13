/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
*/

package org.xtreemfs.babudb.sandbox;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.sandbox.CLIParser.CliOption;
import org.xtreemfs.include.common.config.BabuDBConfig;
import org.xtreemfs.include.common.config.MasterConfig;
import org.xtreemfs.include.common.config.SlaveConfig;
import org.xtreemfs.include.common.logging.Logging;

/**
 *
 * @author bjko
 */
public class BabuDBBenchmark {

    public static final String VER = "0.1";

    public final static byte[] CHARS;

    static {
        int charptr = 0;
        CHARS = new byte[10+26+26];
        for (int i = 48; i < 58; i++)
            CHARS[charptr++] = (byte)i;
        for (int i = 65; i < 91; i++)
            CHARS[charptr++] = (byte)i;
        for (int i = 97; i < 122; i++)
            CHARS[charptr++] = (byte)i;
    }

    private final int numKeys;
    private final int minKeyLength;
    private final int maxKeyLength;
    
    //private final BabuDB slaveDB;

    private final byte[] payload;

    private final BabuDB database;

    private final int numThreads;

    public BabuDBBenchmark(String dbDir, SyncMode syncMode, int pseudoModeWait, int maxQ, int numThreads, int numDBWorkers,
            int numKeys, int valueLength, int minKeyLength, int maxKeyLength) throws BabuDBException, IOException {
        this.numKeys = numKeys;
        this.minKeyLength = minKeyLength;
        this.maxKeyLength = maxKeyLength;
        this.numThreads = numThreads;

        payload = new byte[valueLength];
        for (int i = 0; i < valueLength; i++)
            payload[i] = 'v';

        if (numKeys > Math.pow(CHARS.length, maxKeyLength))
            throw new IllegalArgumentException(maxKeyLength+" is too short to create enough unique keys for "+numKeys+" keys");

        //use one worker because we use one database TODO rebuild
        database = BabuDBFactory.createBabuDB(new BabuDBConfig(dbDir, dbDir, numDBWorkers, 1, 0, syncMode, pseudoModeWait, maxQ));
        SlaveConfig sConf = new SlaveConfig("config/slave.properties");
        sConf.read();
        MasterConfig conf = new MasterConfig("config/master.properties");
        conf.read();
        //database = BabuDB.getMasterBabuDB(conf);
        //slaveDB = BabuDB.getSlaveBabuDB(sConf);
        
        for (int i = 1; i <= numThreads; i++)
        database.getDatabaseManager().createDatabase(""+i, 1);

        System.out.println("BabuDBBenchmark version "+VER+" ==============================\n");
        System.out.println("Configuration ----------------------------------");
        System.out.println("database directory:       "+dbDir);
        System.out.println("sync mode:                "+syncMode.name());
        System.out.println("pseudo-sync mode:         "+( (pseudoModeWait > 0) ? "enabled, "+pseudoModeWait+" ms" : "disabled"));
        System.out.println("max. queue length:        "+( (maxQ > 0) ? maxQ+" requests" : "unlimited"));
        System.out.println("min/max key length:       "+minKeyLength+"/"+maxKeyLength);
        System.out.println("# benchmark threads/DBs:  "+numThreads);
        System.out.println("# BabuDB workers:         "+numDBWorkers);
        System.out.println("auto checkpointing:       disabled");
        System.out.println("value size:               "+valueLength);
        System.out.println("num keys/worker:          "+numKeys);
        System.out.println("");
        System.out.println("Executing benchmark...\n");
    }   

    public double checkpoint() throws BabuDBException, InterruptedException {
        long tStart = System.currentTimeMillis();
        database.getCheckpointer().checkpoint();
        long tEnd = System.currentTimeMillis();
        return (tEnd-tStart)/1000.0;
    }

    public void shutdown() throws Exception {  
        database.shutdown();
        /*
        try {
            // wait until the queue is runs empty
            Thread.sleep(20000);
            
            slaveDB.checkpoint();
        } catch (Exception e) {
            e.printStackTrace();
        }
        slaveDB.shutdown(); */
    }

    public double benchmarkInserts() throws Exception {
        BenchmarkWorkerThread[] threads = new BenchmarkWorkerThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new BenchmarkWorkerThread(i+1, numKeys, minKeyLength, maxKeyLength, payload, database, BenchmarkWorkerThread.BenchmarkOperation.INSERT);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        double throughput = 0.0;
        for (int i = 0; i < numThreads; i++) {
            throughput += threads[i].waitForResult();
        }
        return throughput;
        
    }

    public double benchmarkIterate() throws Exception {
        BenchmarkWorkerThread[] threads = new BenchmarkWorkerThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new BenchmarkWorkerThread(i+1, numKeys, minKeyLength, maxKeyLength, payload, database, BenchmarkWorkerThread.BenchmarkOperation.ITERATE);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        double throughput = 0.0;
        for (int i = 0; i < numThreads; i++) {
            throughput += threads[i].waitForResult();
        }
        return throughput;
    }

    public double benchmarkLookup() throws Exception {
        BenchmarkWorkerThread[] threads = new BenchmarkWorkerThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new BenchmarkWorkerThread(i+1, numKeys, minKeyLength, maxKeyLength, payload, database, BenchmarkWorkerThread.BenchmarkOperation.LOOKUP);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        double throughput = 0.0;
        for (int i = 0; i < numThreads; i++) {
            throughput += threads[i].waitForResult();
        }
        return throughput;
    }

    public double benchmarkDirectLookup() throws Exception {
        BenchmarkWorkerThread[] threads = new BenchmarkWorkerThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new BenchmarkWorkerThread(i+1, numKeys, minKeyLength, maxKeyLength, payload, database, BenchmarkWorkerThread.BenchmarkOperation.DIRECT_LOOKUP);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        double throughput = 0.0;
        for (int i = 0; i < numThreads; i++) {
            throughput += threads[i].waitForResult();
        }
        return throughput;
    }

    public double benchmarkUDL() throws Exception {
        BenchmarkWorkerThread[] threads = new BenchmarkWorkerThread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new BenchmarkWorkerThread(i+1, numKeys, minKeyLength, maxKeyLength, payload, database, BenchmarkWorkerThread.BenchmarkOperation.UDL);
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }
        double throughput = 0.0;
        for (int i = 0; i < numThreads; i++) {
            throughput += threads[i].waitForResult();
        }
        return throughput;
    }

    public static void usage() {
        System.out.println("BabuDBBenchmark <options> <numKeysPerThread>");
        System.out.println("  "+"<numKeysPerThread> number of keys inserted/looked-up by");
        System.out.println("  "+"each thread");
        System.out.println("  "+"-path directory in which to store the database, default is /tmp/babudb_benchmark");
        System.out.println("  "+"-sync synchronization mode, default is FSYNC");
        System.out.print("       possible values: ");
        for (SyncMode tmp : SyncMode.values()) {
            System.out.print(tmp.name()+" ");
        }
        System.out.println("");
        System.out.println("  "+"-wait ms between to bach writes, default is 0 for synchronous mode");
        System.out.println("  "+"-maxq maxmimum worker queue length, default is 0 for unlimited");
        System.out.println("  "+"-workers number of database worker threads, default is 1");
        System.out.println("  "+"-thr number of threads to use for benchmarking, default is 1");
        System.out.println("  "+"-payload size of values, default is 50 bytes");
        System.out.println("  "+"-keymin minimum key length, default is 2");
        System.out.println("  "+"-keymax maximum key length, default is 20");
    }


    public static void main(String[] args) {
        try {
            Logging.start(Logging.LEVEL_WARN);

            Map<String,CLIParser.CliOption> options = new HashMap<String, CliOption>();
            options.put("path",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.FILE,new File("/tmp/babudb_benchmark")));
            options.put("sync",new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.STRING,SyncMode.FSYNC.name()));
            options.put("wait", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
            options.put("maxq", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,0));
            options.put("workers", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,1));
            options.put("thr", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,1));
            options.put("payload", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,50));
            options.put("keymin", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,2));
            options.put("keymax", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.NUMBER,20));
            options.put("nocp", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH,false));
            options.put("h", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH,false));
            options.put("warmcache", new CLIParser.CliOption(CLIParser.CliOption.OPTIONTYPE.SWITCH,false));

            List<String> arguments = new ArrayList<String>(1);
            CLIParser.parseCLI(args, options, arguments);

            if ((arguments.size() != 1) || (options.get("h").switchValue)) {
                usage();
                System.exit(1);
            }

            int numKeys = Integer.valueOf(arguments.get(0));


            BabuDBBenchmark benchmark = new BabuDBBenchmark(options.get("path").fileValue.getAbsolutePath(),
                    SyncMode.valueOf(options.get("sync").stringValue),
                    options.get("wait").numValue.intValue(),
                    options.get("maxq").numValue.intValue(),
                    options.get("thr").numValue.intValue(),
                    options.get("workers").numValue.intValue(),
                    numKeys,
                    options.get("payload").numValue.intValue(),
                    options.get("keymin").numValue.intValue(),
                    options.get("keymax").numValue.intValue());
            double tpIns =benchmark.benchmarkInserts();
            double tpIter = benchmark.benchmarkIterate();

            double durCP = 0;
            if (options.get("nocp").switchValue == false)
                durCP = benchmark.checkpoint();
            double tpLookup = benchmark.benchmarkLookup();

            double tpDLookup = benchmark.benchmarkDirectLookup();

            double tpUDL = benchmark.benchmarkUDL();

            double tpSecondLookup = 0;
            if (options.get("warmcache").switchValue)
                tpSecondLookup = benchmark.benchmarkLookup();

            System.out.println("RESULTS -----------------------------------------\n");
            
            System.out.format("total throughput for INSERT     : %12.4f keys/s\n", tpIns);
            System.out.format("total throughput for ITERATE    : %12.4f keys/s\n", tpIter);
            System.out.format("total throughput for LOOKUP     : %12.4f keys/s\n\n", tpLookup);
            System.out.format("total throughput for D.LOOKUP   : %12.4f keys/s\n\n", tpDLookup);
            System.out.format("total throughput for UDL        : %12.4f keys/s\n\n", tpUDL*10.0);

            if (options.get("warmcache").switchValue)
                System.out.format("total throughput for 2nd LOOKUP : %12.4f keys/s\n\n", tpSecondLookup);

            if (options.get("nocp").switchValue == false)
                System.out.format("CHECKPOINTING took               : %12.4f s\n", durCP);
            else
                System.out.println("CHECKPOINTING disabled");

            benchmark.shutdown();
        } catch (Exception ex) {
            System.out.println("FAILED!!!");
            ex.printStackTrace();
            System.exit(1);
        }
    }

}
