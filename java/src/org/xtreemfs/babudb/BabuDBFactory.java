/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.babudb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.index.ByteRangeComparator;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.LSMTree;
import org.xtreemfs.babudb.log.DiskLogFile;
import org.xtreemfs.babudb.log.DiskLogger;
import org.xtreemfs.babudb.log.LogEntry;
import org.xtreemfs.babudb.log.LogEntryException;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.babudb.lsmdb.Checkpointer;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup;
import org.xtreemfs.babudb.lsmdb.LSMDBRequest;
import org.xtreemfs.babudb.lsmdb.LSMDBWorker;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.lsmdb.LSN;
import org.xtreemfs.babudb.lsmdb.InsertRecordGroup.InsertRecord;
import org.xtreemfs.babudb.replication.Replication;
import org.xtreemfs.babudb.replication.Replication.SYNC_MODUS;
import org.xtreemfs.common.buffer.ReusableBuffer;
import org.xtreemfs.common.logging.Logging;
import org.xtreemfs.foundation.pinky.SSLOptions;

/**
 * 
 * <p>Returns instances of the BabuDB.</p>
 * 
 * @author flangner
 */

public class BabuDBFactory {
    /**
     * <p>A Single static object for creating instances of the enclosing type {@link RequestImpl}.</p>
     */
    private final static BabuDBFactory THIS = new BabuDBFactory();  
    
    /**
     * The BabuDB main class.
     * 
     * @author bjko
     *
     */
    private static class AsyncResult {

        public boolean done = false;

        public byte[] value;
        
        public Object udlresult;

        public Iterator<Entry<byte[], byte[]>> iterator;

        public BabuDBException error;

    }
    
    /**
     * @author bjko
     *
     */
    public class BabuDBImpl implements BabuDB {
        
        /**
         * The database configuration file.
         * Contains the names and number of indices for the databases.
         */
        private static final String DBCFG_FILE = "config.db";

        /**
         * Mapping from database name to database id
         */
        private final Map<String, LSMDatabase> dbNames;

        /**
         * Mapping from dbId to database
         */
        public final Map<Integer, LSMDatabase> databases;

        /**
         * base dir to store database index snapshots in
         */
        private final String baseDir;

        /**
         * directory in which the database logs are stored.
         */
        private final String dbLogDir;

        /**
         * the disk logger is used to write InsertRecordGroups persistently to disk
         * - visibility changed to public, because the replication needs access to the {@link DiskLogger}
         */
        public final DiskLogger logger;

        /**
         * object used for synchronizing modifications of the database lists.
         */
        private final Object dbModificationLock;

        /**
         * object used to ensure that only one checkpoint is created synchronously
         */
        private final Object checkpointLock;

        /**
         * object used for locking to ensure that switch of log file and overlay trees is atomic
         * and not interrupted by inserts
         */
        private final ReadWriteLock overlaySwitchLock;

        private final LSMDBWorker[] worker;

        public final Replication replicationApproach;
        
        /**
         * ID to assign to next database create
         */
        private int nextDbId;

        /**
         * Checkpointer thread for automatic checkpointing
         * -has to be public for replication issues
         */
        public final Checkpointer dbCheckptr;

        private final Map<String, ByteRangeComparator> compInstances;

        /**
         * if set to a value > 0, operations are acknowledges immediately before
         * they are written to the disk log. The disk logger will do batch writes
         * and call fsync... every pseudoSyncWait seconds. This can be used to
         * increase performance and emulate PostgreSQL behaviour
         */
        private final int pseudoSyncWait;
        
        /**
         * Needed for MasterSlave failOver in case of Replication.
         */
        private List<InetSocketAddress> slaves = null;
        
        /**
         * Needed for MasterSlave failOver in case of Replication.
         */
        private InetSocketAddress master = null;
        
        /**
         * Error message.
         */
        private static final String slaveProtection = "You are not allowed to proceed this operation, " +
        		"because this DB is running as a slave!";
        
        /**
         * Starts the BabuDB database (with replication).
         * Hidden main constructor.
         * 
         * @param baseDir directory in which the database snapshots are stored
         * @param dbLogDir directory in which the database append logs are stored (can be same as baseDir)
         * @param numThreads number of worker threads to use
         * @param maxLogfileSize a checkpoint is generated if  maxLogfileSize is exceeded
         * @param checkInterval interval between two checks in seconds, 0 disables auto checkpointing
         * @param syncMode the synchronization mode to use for the logfile
         * @param pseudoSyncWait if value > 0 then requests are immediateley aknowledged and synced to disk every
         *        pseudoSyncWait ms.
         * @param maxQ if > 0, the queue for each worker is limited to maxQ
         * @param master host, from which replicas are received
         * @param slaves hosts, where the replicas should be send to
         * @param isMaster <p>if true, the BabuDB will be started as Master, if false it will be started as slaves. 
         *        But if the <code>slaves</code> or <code>master</code> are set to null it will be started without replication.</p>
         * @param port where the application listens at. (use 0 for default configuration)
         * @param ssl if set SSL will be used while replication.
         * @param repMode synchronization mode of the replication
         * 
         * @throws BabuDBException
         */
        private BabuDBImpl(String baseDir, String dbLogDir, int numThreads,
                long maxLogfileSize, int checkInterval, SyncMode syncMode, int pseudoSyncWait,
                int maxQ,InetSocketAddress master,List<InetSocketAddress> slaves,int port,
                SSLOptions ssl,boolean isMaster,SYNC_MODUS repMode) 
        throws BabuDBException {  
            // start the replication service
            if (master!=null && slaves!=null){
                this.slaves = slaves;
                this.master = master;
                try {
                    if (isMaster) 
                        replicationApproach = new Replication(slaves,ssl,port,this,repMode);
                    else 
                        replicationApproach = new Replication(master,ssl,port,this,repMode);
                    
                } catch (Exception e) {
                    throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,"Replication could not be initialized! Because: "+e.getMessage(),e.getCause());
                }
            } else replicationApproach = null;
            
            if (baseDir.endsWith(File.separator)) {
                this.baseDir = baseDir;
            } else {
                this.baseDir = baseDir + File.separatorChar;
            }
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "base dir: " + this.baseDir);

            if (dbLogDir.endsWith(File.separator)) {
                this.dbLogDir = dbLogDir;
            } else {
                this.dbLogDir = dbLogDir + File.separatorChar;
            }
            Logging.logMessage(Logging.LEVEL_DEBUG, this, "db log dir: " + this.dbLogDir);

            dbNames = new HashMap<String, LSMDatabase>();
            databases = new HashMap<Integer, LSMDatabase>();

            nextDbId = 1;

            compInstances = new HashMap<String, ByteRangeComparator>();
            compInstances.put(DefaultByteRangeComparator.class.getName(), new DefaultByteRangeComparator());

            loadDBs();

            Logging.logMessage(Logging.LEVEL_INFO, this, "starting log replay");
            LSN nextLSN = replayLogs();
            Logging.logMessage(Logging.LEVEL_INFO, this, "log replay done");

            this.pseudoSyncWait = pseudoSyncWait;
            try {
                logger = new DiskLogger(dbLogDir, nextLSN.getViewId(), nextLSN.getSequenceNo(), syncMode, pseudoSyncWait,
                        maxQ*numThreads);
                logger.start();
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot start database operations logger", ex);
            }

            dbModificationLock = new Object();
            checkpointLock = new Object();
            overlaySwitchLock = new ReentrantReadWriteLock();

            worker = new LSMDBWorker[numThreads];
            for (int i = 0; i < numThreads; i++) {
                worker[i] = new LSMDBWorker(logger, i, overlaySwitchLock, (pseudoSyncWait>0),maxQ,replicationApproach);
                worker[i].start();
            }

            if (checkInterval > 0) {
                dbCheckptr = new Checkpointer(this, logger, checkInterval, maxLogfileSize);
                dbCheckptr.start();
            } else {
                dbCheckptr = null;
            }

            Logging.logMessage(Logging.LEVEL_INFO, this, "BabuDB for Java is running (version "+BABUDB_VERSION+")");
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#shutdown()
         */
        public void shutdown() {
            for (LSMDBWorker w : worker) {
                w.shutdown();
            }
            
           // stop the replication
            try{
                if (replicationApproach!=null) replicationApproach.stop();
            }catch (BabuDBException e){
                Logging.logMessage(Logging.LEVEL_ERROR, this,"Replication could not be stopped properly. Because: "+e.getMessage());
            }
            
            logger.shutdown();
            if (dbCheckptr != null) {
                dbCheckptr.shutdown();
            }
            try {
                logger.waitForShutdown();
                for (LSMDBWorker w : worker) {
                    w.waitForShutdown();
                }
                if (dbCheckptr != null) {
                    dbCheckptr.waitForShutdown();
                }
            } catch (InterruptedException ex) {
            }
            Logging.logMessage(Logging.LEVEL_INFO, this,"BabuDB shutdown complete.");
        }

        /**
         * NEVER USE THIS EXCEPT FOR UNIT TESTS!
         * Kills the database.
         */
        public void __test_killDB_dangerous() {
            try {
                logger.stop();
                for (LSMDBWorker w : worker) {
                    w.stop();
                }
            } catch (IllegalMonitorStateException ex) {
                //we will probably get that when we kill a thread because we do evil stuff here ;-)
            }
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#syncSingleInsert(java.lang.String, int, byte[], byte[])
         */
        public void syncSingleInsert(String database, int indexId, byte[] key, byte[] value) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            final LSMDatabase db = dbNames.get(database);
            BabuDBInsertGroup irec = new BabuDBInsertGroup(db);
            irec.addInsert(indexId, key, value);

            final AsyncResult result = new AsyncResult();

            asyncInsert(irec, new BabuDBRequestListener() {

                public void insertFinished(Object context) {
                    synchronized (result) {
                        result.done = true;
                        result.notify();
                    }
                }

                public void lookupFinished(Object context, byte[] value) {
                }

                public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
                }

                public void requestFailed(Object context, BabuDBException error) {
                    synchronized (result) {
                        result.done = true;
                        result.error = error;
                        result.notify();
                    }
                }

                public void userDefinedLookupFinished(Object context, Object result) {
                }
            }, null);
            synchronized (result) {
                try {
                    if (!result.done) {
                        result.wait();
                    }
                } catch (InterruptedException ex) {
                }
            }
            if (result.error != null) {
                throw result.error;
            }
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#syncInsert(org.xtreemfs.babudb.BabuDBInsertGroup)
         */
        public void syncInsert(BabuDBInsertGroup irg) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            //final LSMDatabase db = databases.get(irg.getDatabaseId());

            final AsyncResult result = new AsyncResult();

            asyncInsert(irg, new BabuDBRequestListener() {

                public void insertFinished(Object context) {
                    synchronized (result) {
                        result.done = true;
                        result.notify();
                    }
                }

                public void lookupFinished(Object context, byte[] value) {
                }

                public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
                }

                public void requestFailed(Object context, BabuDBException error) {
                    synchronized (result) {
                        result.done = true;
                        result.error = error;
                        result.notify();
                    }
                }

                public void userDefinedLookupFinished(Object context, Object result) {
                }
            }, null);
            synchronized (result) {
                try {
                    if (!result.done) {
                        result.wait();
                    }
                } catch (InterruptedException ex) {
                }
            }
            if (result.error != null) {
                throw result.error;
            }
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#syncLookup(java.lang.String, int, byte[])
         */
        public byte[] syncLookup(String database, int indexId, byte[] key) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);

            final AsyncResult result = new AsyncResult();

            asyncLookup(database, indexId, key, new BabuDBRequestListener() {

                public void insertFinished(Object context) {
                }

                public void lookupFinished(Object context, byte[] value) {
                    synchronized (result) {
                        result.done = true;
                        result.value = value;
                        result.notify();
                    }
                }

                public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
                }

                public void requestFailed(Object context, BabuDBException error) {
                    synchronized (result) {
                        result.done = true;
                        result.error = error;
                        result.notify();
                    }
                }

                public void userDefinedLookupFinished(Object context, Object result) {
                }
            }, null);
            synchronized (result) {
                try {
                    if (!result.done) {
                        result.wait();
                    }
                } catch (InterruptedException ex) {
                }
            }
            if (result.error != null) {
                throw result.error;
            }
            return result.value;
        }
        
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#syncUserDefinedLookup(java.lang.String, org.xtreemfs.babudb.UserDefinedLookup)
         */
        public Object syncUserDefinedLookup(String database, UserDefinedLookup udl) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            final AsyncResult result = new AsyncResult();

            asyncUserDefinedLookup(database, new BabuDBRequestListener() {

                public void insertFinished(Object context) {
                }

                public void lookupFinished(Object context, byte[] value) {

                }

                public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
                }

                public void requestFailed(Object context, BabuDBException error) {
                    synchronized (result) {
                        result.done = true;
                        result.error = error;
                        result.notify();
                    }
                }

                public void userDefinedLookupFinished(Object context, Object result2) {
                    synchronized (result) {
                        result.done = true;
                        result.udlresult = result2;
                        result.notify();
                    }
                }
            }, udl, null);
            synchronized (result) {
                try {
                    if (!result.done) {
                        result.wait();
                    }
                } catch (InterruptedException ex) {
                }
            }
            if (result.error != null) {
                throw result.error;
            }
            return result.udlresult;
        }
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#syncPrefixLookup(java.lang.String, int, byte[])
         */
        public Iterator<Entry<byte[], byte[]>> syncPrefixLookup(String database, int indexId, byte[] key) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            final AsyncResult result = new AsyncResult();

            asyncPrefixLookup(database, indexId, key, new BabuDBRequestListener() {

                public void insertFinished(Object context) {
                }

                public void lookupFinished(Object context, byte[] value) {
                }

                public void prefixLookupFinished(Object context, Iterator<Entry<byte[], byte[]>> iterator) {
                    synchronized (result) {
                        result.done = true;
                        result.iterator = iterator;
                        result.notify();
                    }
                }

                public void requestFailed(Object context, BabuDBException error) {
                    synchronized (result) {
                        result.done = true;
                        result.error = error;
                        result.notify();
                    }
                }

                public void userDefinedLookupFinished(Object context, Object result) {
                }
            }, null);
            synchronized (result) {
                try {
                    if (!result.done) {
                        result.wait();
                    }
                } catch (InterruptedException ex) {
                }
            }
            if (result.error != null) {
                throw result.error;
            }
            return result.iterator;
        }

        /**
         * Proceeds a Create, without isSlaveCheck. Replication Approach!
         * 
         * @param databaseName
         * @param numIndices
         * @throws BabuDBException
         */
        public void proceedCreate(String databaseName, int numIndices) throws BabuDBException {
            ByteRangeComparator[] comps = new ByteRangeComparator[numIndices];
            final ByteRangeComparator defaultComparator = compInstances.get(DefaultByteRangeComparator.class.getName());
            for (int i = 0; i < numIndices; i++) {
                comps[i] = defaultComparator;
            }
            synchronized (dbModificationLock) {
                if (dbNames.containsKey(databaseName)) {
                    throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + databaseName + "' already exists");
                }
                final int dbId = nextDbId++;
                LSMDatabase db = new LSMDatabase(databaseName, dbId, baseDir + databaseName + "/", numIndices, false, comps);
                databases.put(dbId, db);
                dbNames.put(databaseName, db);
                saveDBconfig();   
            }
            
           // if this is a master it sends the create-details to all slaves. otherwise nothing happens
            if (replicationApproach!=null) replicationApproach.create(databaseName,numIndices);
        }
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#createDatabase(java.lang.String, int)
         */
        public void createDatabase(String databaseName, int numIndices) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            proceedCreate(databaseName,numIndices);
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#createDatabase(java.lang.String, int, org.xtreemfs.babudb.index.ByteRangeComparator[])
         */
        public void createDatabase(String databaseName, int numIndices, ByteRangeComparator[] comparators) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            synchronized (dbModificationLock) {
                if (dbNames.containsKey(databaseName)) {
                    throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + databaseName + "' already exists");
                }
                final int dbId = nextDbId++;
                LSMDatabase db = new LSMDatabase(databaseName, dbId, baseDir + databaseName + "/", numIndices, false, comparators);
                databases.put(dbId, db);
                dbNames.put(databaseName, db);
                saveDBconfig();   
            }
            
           // if this is a master it sends the create-details to all slaves. otherwise nothing happens
            if (replicationApproach!=null) replicationApproach.create(databaseName,numIndices);
        }

        /**
         * Proceeds a Delete, without isSlaveCheck. Replication Approach!
         * 
         * @param databaseName
         * @param deleteFiles
         * @throws BabuDBException
         */
        public void proceedDelete(String databaseName, boolean deleteFiles) throws BabuDBException {
            synchronized (dbModificationLock) {
                if (!dbNames.containsKey(databaseName)) {
                    throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + databaseName + "' does not exists");
                }
                final LSMDatabase db = dbNames.get(databaseName);
                dbNames.remove(databaseName);
                databases.remove(db.getDatabaseId());
                saveDBconfig();
                if (deleteFiles) {
                    //FIXME:
                }
            }
            
           // if this is a master it sends the delete-details to all slaves. otherwise nothing happens 
            if (replicationApproach!=null) replicationApproach.delete(databaseName,deleteFiles);
        }
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#deleteDatabase(java.lang.String, boolean)
         */
        public void deleteDatabase(String databaseName, boolean deleteFiles) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            proceedDelete(databaseName, deleteFiles);
        }
        
        /**
         * Proceeds a Copy, without isSlaveCheck. Replication Approach!
         * 
         * @param sourceDB
         * @param destDB
         * @param rangeStart
         * @param rangeEnd
         * @throws BabuDBException
         * @throws IOException
         */
        public void proceedCopy(String sourceDB, String destDB, byte[] rangeStart, byte[] rangeEnd) throws BabuDBException, IOException{
            final LSMDatabase sDB = dbNames.get(sourceDB);
            if (sDB == null)
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '"+sourceDB+"' does not exist");
            
            final int dbId;
            synchronized (dbModificationLock) {
                if (dbNames.containsKey(destDB)) {
                    throw new BabuDBException(ErrorCode.DB_EXISTS, "database '" + destDB + "' already exists");
                }
                dbId = nextDbId++;
                //just "reserve" the name
                dbNames.put(destDB,null);
                saveDBconfig();
                
            }
            //materializing the snapshot takes some time, we should not hold the lock meanwhile!
            
            int[] snaps = sDB.createSnapshot();
            File dbDir = new File(baseDir+destDB);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }
            sDB.writeSnapshot(baseDir+destDB+"/", snaps);
            
            //create new DB and load from snapshot
            LSMDatabase dDB = new LSMDatabase(destDB, dbId, baseDir + destDB + "/", sDB.getIndexCount(), true, sDB.getComparators());
            
            //insert real database
            synchronized (dbModificationLock) {
                databases.put(dbId, dDB);
                dbNames.put(destDB, dDB);
                saveDBconfig();          
            }
            
           // if this is a master it sends the copy-details to all slaves. otherwise nothing happens  
            if (replicationApproach!=null) replicationApproach.copy(sourceDB,destDB);
        }
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#copyDatabase(java.lang.String, java.lang.String, byte[], byte[])
         */
        public void copyDatabase(String sourceDB, String destDB, byte[] rangeStart, byte[] rangeEnd) throws BabuDBException, IOException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            proceedCopy(sourceDB, destDB, rangeStart, rangeEnd);
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#checkpoint()
         */
        public void checkpoint() throws BabuDBException, InterruptedException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            List<LSMDatabase> dbListCopy;

            synchronized (dbModificationLock) {
                dbListCopy = new ArrayList<LSMDatabase>(databases.size());
                for (LSMDatabase db : databases.values()) {
                    dbListCopy.add(db);
                }
            }

            synchronized (checkpointLock) {
                try {
                    int[][] snapIds = new int[dbListCopy.size()][];
                    int i = 0;

                    LSN lastWrittenLSN = null;
                    try {
                        //critical block...
                        logger.lockLogger();
                        for (LSMDatabase db : dbListCopy) {
                            snapIds[i++] = db.createSnapshot();
                        }
                        lastWrittenLSN = logger.switchLogFile();
                    } finally {
                        logger.unlockLogger();
                    }
                    
                    
                    i = 0;
                    for (LSMDatabase db : dbListCopy) {
                        db.writeSnapshot(lastWrittenLSN.getViewId(), lastWrittenLSN.getSequenceNo(), snapIds[i++]);
                        db.cleanupSnapshot(lastWrittenLSN.getViewId(), lastWrittenLSN.getSequenceNo());
                    }

                    //FIXME: delete old logfiles!
                    //delete all logfile with LSN <= lastWrittenLSN
                    File f = new File(dbLogDir);
                    String[] logs = f.list(new FilenameFilter() {

                        public boolean accept(File dir, String name) {
                            return name.endsWith(".dbl");
                        }
                    });
                    if (logs != null) {
                        Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.dbl");
                        for (String log : logs) {
                            Matcher m = p.matcher(log);
                            m.matches();
                            String tmp = m.group(1);
                            int viewId = Integer.valueOf(tmp);
                            tmp = m.group(2);
                            int seqNo = Integer.valueOf(tmp);
                            LSN logLSN = new LSN(viewId, seqNo);
                            if (logLSN.compareTo(lastWrittenLSN) <= 0) {
                                Logging.logMessage(Logging.LEVEL_DEBUG, this, "deleting old db log file: " + log);
                                f = new File(dbLogDir + log);
                                f.delete();
                            }
                        }
                    }
                } catch (IOException ex) {
                    throw new BabuDBException(ErrorCode.IO_ERROR, "cannot create checkpoint", ex);
                }
            }

        }
        
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#createSnapshot(java.lang.String)
         */
        public int[] createSnapshot(String dbName) throws BabuDBException, InterruptedException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
                    
            final LSMDatabase db = dbNames.get(dbName);
            if (db == null)
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '"+dbName+"' does not exist");

            try {
                //critical block...
                logger.lockLogger();
                return db.createSnapshot();
            } finally {
                logger.unlockLogger();
            } 

        }
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#writeSnapshot(java.lang.String, int[], java.lang.String)
         */
        public void writeSnapshot(String dbName, int[] snapIds, String directory) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            try {
                final LSMDatabase db = dbNames.get(dbName);
                if (db == null) {
                    throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database '" + dbName + "' does not exist");
                }
                db.writeSnapshot(directory, snapIds);
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot write snapshot: "+ex,ex);
            }
        }

        /**
         * Loads the configuration and each database from disk
         * @throws BabuDBException
         */
        private void loadDBs() throws BabuDBException {
            try {
                File f = new File(baseDir + DBCFG_FILE);
                if (f.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
                    final int dbFormatVer = ois.readInt();
                    if (dbFormatVer != BABUDB_DB_FORMAT_VERSION) {
                        throw new BabuDBException(ErrorCode.IO_ERROR,"on-disk format (version "+
                                dbFormatVer+") is incompatible with this BabuDB release "+
                                "(uses on-disk format version "+BABUDB_DB_FORMAT_VERSION+")");
                    }
                    final int numDB = ois.readInt();
                    nextDbId =
                            ois.readInt();
                    for (int i = 0; i <
                            numDB; i++) {
                        Logging.logMessage(Logging.LEVEL_DEBUG, this, "loading DB...");
                        final String dbName = (String) ois.readObject();
                        final int dbId = ois.readInt();
                        final int numIndex = ois.readInt();
                        ByteRangeComparator[] comps = new ByteRangeComparator[numIndex];
                        for (int idx = 0; idx <
                                numIndex; idx++) {
                            final String className = (String) ois.readObject();
                            ByteRangeComparator comp = compInstances.get(className);
                            if (comp == null) {
                                Class<?> clazz = Class.forName(className);
                                comp = (ByteRangeComparator) clazz.newInstance();
                                compInstances.put(className, comp);
                            }

                            assert (comp != null);
                            comps[idx] = comp;
                        }

                        LSMDatabase db = new LSMDatabase(dbName, dbId, baseDir + dbName + "/", numIndex, true, comps);
                        databases.put(dbId, db);
                        dbNames.put(dbName, db);
                        Logging.logMessage(Logging.LEVEL_DEBUG, this, "loaded DB " + dbName + " successfully.");
                    }

                    ois.close();
                }

            } catch (InstantiationException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
            } catch (IllegalAccessException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot instantiate comparator", ex);
            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load database config, check path and access rights", ex);
            } catch (ClassNotFoundException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load database config, config file might be corrupted", ex);
            } catch (ClassCastException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load database config, config file might be corrupted", ex);
            }

        }

        /**
         * Replay the database operations log.
         * @return the LSN to assign to the next operation
         * @throws BabuDBException
         */
        private LSN replayLogs() throws BabuDBException {
            if (databases.size() == 0) {
                return new LSN(1, 1);
            }

            try {
                File f = new File(dbLogDir);
                String[] logs = f.list(new FilenameFilter() {

                    public boolean accept(File dir, String name) {
                        return name.endsWith(".dbl");
                    }
                });
                LSN nextLSN = null;
                if (logs != null) {
                    //read list of logs and create a list ordered from min LSN to max LSN
                    SortedSet<LSN> orderedLogList = new TreeSet<LSN>();
                    Pattern p = Pattern.compile("(\\d+)\\.(\\d+)\\.dbl");
                    for (String log : logs) {
                        Matcher m = p.matcher(log);
                        m.matches();
                        String tmp = m.group(1);
                        int viewId = Integer.valueOf(tmp);
                        tmp =
                                m.group(2);
                        int seqNo = Integer.valueOf(tmp);
                        orderedLogList.add(new LSN(viewId, seqNo));
                    }
    //apply log entries to databases...
                    for (LSN logLSN : orderedLogList) {
                        DiskLogFile dlf = new DiskLogFile(this.dbLogDir, logLSN);
                        LogEntry le = null;
                        while (dlf.hasNext()) {
                            le = dlf.next();
                            //do something
                            ReusableBuffer payload = le.getPayload();
                            InsertRecordGroup ai = InsertRecordGroup.deserialize(payload);
                            insert(ai);
                            le.free();
                        }
    //set lsn'
                        if (le != null) {
                            nextLSN = new LSN(le.getViewId(), le.getLogSequenceNo() + 1);
                        }

                    }
                }
                if (nextLSN != null) {
                    return nextLSN;
                } else {
                    return new LSN(1, 1);
                }

            } catch (IOException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "cannot load database operations log, file might be corrupted", ex);
            } catch (LogEntryException ex) {
                throw new BabuDBException(ErrorCode.IO_ERROR, "corrupted/incomplete log entry in database operations log", ex);
            } catch (Exception ex) {
                throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "cannot load database operations log, unexpected error", ex);
            }

        }

        /**
         * saves the current database config to disk
         * @throws BabuDBException
         */
        private void saveDBconfig() throws BabuDBException {
            /*File f = new File(baseDir+DBCFG_FILE);
            f.renameTo(new File(baseDir+DBCFG_FILE+".old"));*/
            synchronized (dbModificationLock) {
                try {
                    FileOutputStream fos = new FileOutputStream(baseDir + DBCFG_FILE + ".in_progress");
                    ObjectOutputStream oos = new ObjectOutputStream(fos);
                    oos.writeInt(BABUDB_DB_FORMAT_VERSION);
                    oos.writeInt(databases.size());
                    oos.writeInt(nextDbId);
                    for (int dbId : databases.keySet()) {
                        LSMDatabase db = databases.get(dbId);
                        oos.writeObject(db.getDatabaseName());
                        oos.writeInt(dbId);
                        oos.writeInt(db.getIndexCount());
                        String[] compClasses = db.getComparatorClassNames();
                        for (int i = 0; i <
                                db.getIndexCount(); i++) {
                            oos.writeObject(compClasses[i]);
                        }
                    }

                    oos.flush();
                    fos.flush();
                    fos.getFD().sync();
                    oos.close();
                    File f = new File(baseDir + DBCFG_FILE + ".in_progress");
                    f.renameTo(new File(baseDir + DBCFG_FILE));
                } catch (IOException ex) {
                    throw new BabuDBException(ErrorCode.IO_ERROR, "unable to save database configuration", ex);
                }

            }
        }

        /**
         * Insert a full record group. Only to be used by log replay.
         * @param ins
         */
        private void insert(InsertRecordGroup ins) {
            final LSMDatabase db = databases.get(ins.getDatabaseId());
            //ignore deleted databases when recovering!
            if (db == null) {
                return;
            }

            for (InsertRecord ir : ins.getInserts()) {
                LSMTree tree = db.getIndex(ir.getIndexId());
                Logging.logMessage(Logging.LEVEL_DEBUG, this, "insert " + new String(ir.getKey()) + "=" +
                        (ir.getValue() == null? null: new String(ir.getValue())) + " into " + db.getDatabaseName() + " " + ir.getIndexId());
                tree.insert(ir.getKey(), ir.getValue());
            }

        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#asyncInsert(org.xtreemfs.babudb.BabuDBInsertGroup, org.xtreemfs.babudb.BabuDBRequestListener, java.lang.Object)
         */
        public void asyncInsert(BabuDBInsertGroup ig, BabuDBRequestListener listener,
                Object context) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            final InsertRecordGroup ins = ig.getRecord();
            final int dbId = ins.getDatabaseId();
            if (!databases.containsKey(dbId)) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
            }

            LSMDBWorker w = getWorker(dbId);
            if (Logging.tracingEnabled()) {
                Logging.logMessage(Logging.LEVEL_TRACE, this, "insert request is sent to worker #" + dbId % worker.length);
            }

            try {
                w.addRequest(new LSMDBRequest(databases.get(dbId), listener, ins, context));
            } catch (InterruptedException ex) {
                throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "operation was interrupted",ex);
            }
        }
        
        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#asyncUserDefinedLookup(java.lang.String, org.xtreemfs.babudb.BabuDBRequestListener, org.xtreemfs.babudb.UserDefinedLookup, java.lang.Object)
         */
        public void asyncUserDefinedLookup(String databaseName, BabuDBRequestListener listener,
                UserDefinedLookup udl, Object context) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            final LSMDatabase db = dbNames.get(databaseName);
            if (db == null) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
            }

            LSMDBWorker w = getWorker(db.getDatabaseId());
            if (Logging.tracingEnabled()) {
                Logging.logMessage(Logging.LEVEL_TRACE, this, "udl request is sent to worker #" + db.getDatabaseId() % worker.length);
            }

            try {
                w.addRequest(new LSMDBRequest(db, listener, udl,context));
            } catch (InterruptedException ex) {
                throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "operation was interrupted",ex);
            }
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#createInsertGroup(java.lang.String)
         */
        public BabuDBInsertGroup createInsertGroup(
                String databaseName) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            final LSMDatabase db = dbNames.get(databaseName);
            if (db == null) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
            }

            return new BabuDBInsertGroup(db);
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#asyncLookup(java.lang.String, int, byte[], org.xtreemfs.babudb.BabuDBRequestListener, java.lang.Object)
         */
        public void asyncLookup(String databaseName, int indexId, byte[] key,
                BabuDBRequestListener listener, Object context) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            final LSMDatabase db = dbNames.get(databaseName);
            if (db == null) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
            }

            LSMDBWorker w = getWorker(db.getDatabaseId());
            if (Logging.tracingEnabled()) {
                Logging.logMessage(Logging.LEVEL_TRACE, this, "lookup request is sent to worker #" + db.getDatabaseId() % worker.length);
            }

            try {
                w.addRequest(new LSMDBRequest(db, indexId, listener, key, false, context));
            } catch (InterruptedException ex) {
                throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "operation was interrupted",ex);
            }
        }

        /*
         * (non-Javadoc)
         * @see org.xtreemfs.babudb.BabuDBInterface#asyncPrefixLookup(java.lang.String, int, byte[], org.xtreemfs.babudb.BabuDBRequestListener, java.lang.Object)
         */
        public void asyncPrefixLookup(String databaseName, int indexId, byte[] key,
                BabuDBRequestListener listener, Object context) throws BabuDBException {
            if (isSlave()) throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,slaveProtection);
            
            final LSMDatabase db = dbNames.get(databaseName);
            if (db == null) {
                throw new BabuDBException(ErrorCode.NO_SUCH_DB, "database does not exist");
            }

            LSMDBWorker w = getWorker(db.getDatabaseId());
            if (Logging.tracingEnabled()) {
                Logging.logMessage(Logging.LEVEL_TRACE, this, "lookup request is sent to worker #" + db.getDatabaseId() % worker.length);
            }

            try {
                w.addRequest(new LSMDBRequest(db, indexId, listener, key, true, context));
            } catch (InterruptedException ex) {
                throw new BabuDBException(ErrorCode.INTERNAL_ERROR, "operation was interrupted",ex);
            }
        }
        
        /**
         * 
         * @param dbId
         * @return a worker Thread, responsible for the DB given by its ID.
         */
        public LSMDBWorker getWorker(int dbId) {
            return worker[dbId % worker.length];
        }
        
        private boolean isSlave(){
            if (replicationApproach==null) return false;
            return !replicationApproach.isMaster();
        }
    }
    
    /**
     * Starts the BabuDB database.
     * 
     * @param baseDir directory in which the database snapshots are stored
     * @param dbLogDir directory in which the database append logs are stored (can be same as baseDir)
     * @param numThreads number of worker threads to use
     * @param maxLogfileSize a checkpoint is generated if  maxLogfileSize is exceeded
     * @param checkInterval interval between two checks in seconds, 0 disables auto checkpointing
     * @param syncMode the synchronization mode to use for the logfile
     * @param pseudoSyncWait if value > 0 then requests are immediateley aknowledged and synced to disk every
     *        pseudoSyncWait ms.
     * @param maxQ if > 0, the queue for each worker is limited to maxQ

     * @throws BabuDBException
     */
    
    public static BabuDB getBabuDB(String baseDir, String dbLogDir, int numThreads,
            long maxLogfileSize, int checkInterval, SyncMode syncMode, int pseudoSyncWait,
            int maxQ) throws BabuDBException{
        return THIS.new BabuDBImpl(baseDir,dbLogDir,numThreads,maxLogfileSize,checkInterval,syncMode,pseudoSyncWait,maxQ,null,null,0,null,false,SYNC_MODUS.SYNC);
    }
    
    /**
     * Starts the BabuDB database as Master (with Replication enabled).
     * 
     * @param baseDir directory in which the datbase snapshots are stored
     * @param dbLogDir directory in which the database append logs are stored (can be same as baseDir)
     * @param numThreads number of worker threads to use
     * @param maxLogfileSize a checkpoint is generated if  maxLogfileSize is exceeded
     * @param checkInterval interval between two checks in seconds, 0 disables auto checkpointing
     * @param syncMode the synchronization mode to use for the logfile
     * @param pseudoSyncWait if value > 0 then requests are immediateley aknowledged and synced to disk every
     *        pseudoSyncWait ms.
     * @param maxQ if > 0, the queue for each worker is limited to maxQ
     * @param slaves hosts, where the replicas should be send to.
     * @param port where the application listens at. (use 0 for default configuration)
     * @param ssl if set SSL will be used while replication.
     * 
     * @throws BabuDBException
     */
    public static BabuDB getMasterBabuDB(String baseDir, String dbLogDir, int numThreads,
            long maxLogfileSize, int checkInterval, SyncMode syncMode, int pseudoSyncWait,
            int maxQ,List<InetSocketAddress> slaves,int port,SSLOptions ssl, SYNC_MODUS repMode) throws BabuDBException{
        try {
            return THIS.new BabuDBImpl(baseDir,dbLogDir,numThreads,maxLogfileSize,checkInterval,syncMode,pseudoSyncWait,maxQ,new InetSocketAddress(InetAddress.getLocalHost(),port),slaves,port,ssl,true,repMode);
        } catch (UnknownHostException e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE,"Localhost could not be resolved. Please check your network adapter, or your JAVA configuration.");
        }
    }
    
    /**
     * Starts the BabuDB database as Slave (with Replication enabled).
     * 
     * @param baseDir directory in which the datbase snapshots are stored
     * @param dbLogDir directory in which the database append logs are stored (can be same as baseDir)
     * @param numThreads number of worker threads to use
     * @param maxLogfileSize a checkpoint is generated if  maxLogfileSize is exceeded
     * @param checkInterval interval between two checks in seconds, 0 disables auto checkpointing
     * @param syncMode the synchronization mode to use for the logfile
     * @param pseudoSyncWait if value > 0 then requests are immediateley aknowledged and synced to disk every
     *        pseudoSyncWait ms.
     * @param maxQ if > 0, the queue for each worker is limited to maxQ
     * @param master host, from which replicas are received.
     * @param slaves hosts, where the replicas should be send to.
     * @param port where the application listens at. (use 0 for default configuration)
     * @param ssl if set SSL will be used while replication.
     * 
     * @throws BabuDBException
     */
    public static BabuDB getSlaveBabuDB(String baseDir, String dbLogDir, int numThreads,
            long maxLogfileSize, int checkInterval, SyncMode syncMode, int pseudoSyncWait,
            int maxQ,InetSocketAddress master,List<InetSocketAddress> slaves,int port,SSLOptions ssl) throws BabuDBException{
        return THIS.new BabuDBImpl(baseDir,dbLogDir,numThreads,maxLogfileSize,checkInterval,syncMode,pseudoSyncWait,maxQ,master,slaves,port,ssl,false,SYNC_MODUS.SYNC);
    }
}
