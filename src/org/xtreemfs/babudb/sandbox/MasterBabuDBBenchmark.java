/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */

package org.xtreemfs.babudb.sandbox;

import java.io.IOException;
import org.xtreemfs.babudb.BabuDB;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;

/**
 *
 * @author bjko
 * @author fx.langner
 */
public class MasterBabuDBBenchmark {

    public static final String VER = "0.1 Replication";

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

    private final byte[] payload;

    private final BabuDB database;

    private final int numThreads;

    public MasterBabuDBBenchmark(BabuDB db, String dbDir, SyncMode syncMode, int pseudoModeWait, int maxQ, int numThreads, int numDBWorkers,
            int numKeys, int valueLength, int minKeyLength, int maxKeyLength, int port, String prefix) throws BabuDBException, IOException, InterruptedException {
        this.numKeys = numKeys;
        this.minKeyLength = minKeyLength;
        this.maxKeyLength = maxKeyLength;
        this.numThreads = numThreads;

        payload = new byte[valueLength];
        for (int i = 0; i < valueLength; i++)
            payload[i] = 'v';

        if (numKeys > Math.pow(CHARS.length, maxKeyLength))
            throw new IllegalArgumentException(maxKeyLength+" is too short to create enough unique keys for "+numKeys+" keys");
        
        //use one worker because we use one database
        database = db;
        for (int i = 1; i <= numThreads; i++)
            database.createDatabase(prefix+i, 1);

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
        database.checkpoint();
        long tEnd = System.currentTimeMillis();
        return (tEnd-tStart)/1000.0;
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
}
