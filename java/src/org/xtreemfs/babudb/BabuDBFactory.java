/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.xtreemfs.babudb.BabuDBException.ErrorCode;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;
import org.xtreemfs.include.foundation.pinky.SSLOptions;

/**
 * 
 * <p>Returns instances of the {@link BabuDB}.</p>
 * 
 * @author flangner
 */
public class BabuDBFactory {

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
            int maxQ) throws BabuDBException {
        return new BabuDBImpl(baseDir, dbLogDir, numThreads, maxLogfileSize, checkInterval, syncMode, pseudoSyncWait, maxQ, null, null, 0, null, false, 0, 0);
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
     * @param repMode <p>repMode == 0: asynchronous replication mode</br>
     *                   repMode == slaves.size(): synchronous replication mode</br>
     *                   repMode > 0 && repMode < slaves.size(): N -sync replication mode with N = repMode</p>
     * @param qLimit if > 0, the queue for the replication-requests is limited to qLimit
     * 
     * @throws BabuDBException
     */
    public static BabuDB getMasterBabuDB(String baseDir, String dbLogDir, int numThreads,
            long maxLogfileSize, int checkInterval, SyncMode syncMode, int pseudoSyncWait,
            int maxQ, List<InetSocketAddress> slaves, int port, SSLOptions ssl, int repMode, int qLimit) throws BabuDBException {
        assert(slaves!=null);
        try {
            return new BabuDBImpl(baseDir, dbLogDir, numThreads, maxLogfileSize, checkInterval, syncMode, pseudoSyncWait, maxQ, new InetSocketAddress(InetAddress.getLocalHost(), port), slaves, port, ssl, true, repMode, qLimit);
        } catch (UnknownHostException e) {
            throw new BabuDBException(ErrorCode.REPLICATION_FAILURE, "Localhost could not be resolved. Please check your network adapter, or your JAVA configuration.");
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
     * @param qLimit if > 0, the queue for the replication-requests is limited to qLimit
     * 
     * @throws BabuDBException
     */
    public static BabuDB getSlaveBabuDB(String baseDir, String dbLogDir, int numThreads,
            long maxLogfileSize, int checkInterval, SyncMode syncMode, int pseudoSyncWait,
            int maxQ, InetSocketAddress master, List<InetSocketAddress> slaves, int port, SSLOptions ssl, int qLimit) throws BabuDBException {
        assert(master!=null);
        assert(slaves!=null);
        return new BabuDBImpl(baseDir, dbLogDir, numThreads, maxLogfileSize, checkInterval, syncMode, pseudoSyncWait, maxQ, master, slaves, port, ssl, false, slaves.size(), qLimit);
    }
}
