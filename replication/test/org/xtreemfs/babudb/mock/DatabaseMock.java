/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.mock;

import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.database.UserDefinedLookup;
import org.xtreemfs.babudb.api.dev.DatabaseInternal;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;
import org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup;
import org.xtreemfs.babudb.lsmdb.LSMDatabase;
import org.xtreemfs.babudb.snapshots.SnapshotConfig;
import org.xtreemfs.foundation.logging.Logging;

/**
 * @author flangner
 * @since 03/19/2011
 */

public class DatabaseMock implements DatabaseInternal {
        
    public class LSMDBMock extends LSMDatabase {

        public LSMDBMock(String databaseName, int databaseId, int numIndices, 
                ByteRangeComparator[] comparators) throws BabuDBException {
            super(databaseName, databaseId, "", numIndices, false, comparators, false, 0, 0, false, 
                    0);
        }   
    }
    
    private final LSMDatabase lsmDB;
    
    public DatabaseMock(String dbName, int numIndices, ByteRangeComparator[] comps) 
            throws NumberFormatException, BabuDBException {
        
        this.lsmDB = new LSMDBMock(dbName, Integer.parseInt(dbName), numIndices, comps);
    }
    
    @Override
    public BabuDBInsertGroup createInsertGroup() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ByteRangeComparator[] getComparators() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DatabaseRequestResult<Object> insert(DatabaseInsertGroup irg,
            Object context) {
        
        return insert((BabuDBInsertGroup) irg, context);
    }

    @Override
    public DatabaseRequestResult<Object> singleInsert(int indexId, byte[] key,
            byte[] value, Object context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DatabaseRequestResult<byte[]> lookup(int indexId, byte[] key,
            Object context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> prefixLookup(
            int indexId, byte[] key, Object context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> rangeLookup(
            int indexId, byte[] from, byte[] to, Object context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> reversePrefixLookup(
            int indexId, byte[] key, Object context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DatabaseRequestResult<ResultSet<byte[], byte[]>> reverseRangeLookup(
            int indexId, byte[] from, byte[] to, Object context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void shutdown() throws BabuDBException {
        // TODO Auto-generated method stub
    }

    @Override
    public DatabaseRequestResult<Object> userDefinedLookup(
            UserDefinedLookup udl, Object context) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.database.Database#getName()
     */
    @Override
    public String getName() {
        
        Logging.logMessage(Logging.LEVEL_ERROR, this,
            "Received dbName '%s' from mock.", lsmDB.getDatabaseName());
        
        return lsmDB.getDatabaseName();
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#getLSMDB()
     */
    @Override
    public LSMDatabase getLSMDB() {
        
        Logging.logMessage(Logging.LEVEL_ERROR, this,
                "Received lsmDB '%s' from mock.", lsmDB.getDatabaseName());
        
        return lsmDB;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedWriteSnapshot(int[], java.lang.String, org.xtreemfs.babudb.snapshots.SnapshotConfig)
     */
    @Override
    public void proceedWriteSnapshot(int[] snapIds, String directory, SnapshotConfig cfg)
            throws BabuDBException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#setLSMDB(org.xtreemfs.babudb.lsmdb.LSMDatabase)
     */
    @Override
    public void setLSMDB(LSMDatabase lsmDatabase) {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedWriteSnapshot(int, long, int[])
     */
    @Override
    public void proceedWriteSnapshot(int viewId, long sequenceNo, int[] snapIds) throws BabuDBException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedCleanupSnapshot(int, long)
     */
    @Override
    public void proceedCleanupSnapshot(int viewId, long sequenceNo) throws BabuDBException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedCreateSnapshot()
     */
    @Override
    public int[] proceedCreateSnapshot() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#dumpSnapshot(java.lang.String)
     */
    @Override
    public void dumpSnapshot(String baseDir) throws BabuDBException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#proceedSnapshot(java.lang.String)
     */
    @Override
    public void proceedSnapshot(String destDB) throws BabuDBException, InterruptedException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#directLookup(int, int, byte[])
     */
    @Override
    public byte[] directLookup(int indexId, int snapId, byte[] key) throws BabuDBException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#directPrefixLookup(int, int, byte[], boolean)
     */
    @Override
    public ResultSet<byte[], byte[]> directPrefixLookup(int indexId, int snapId, byte[] key, boolean ascending)
            throws BabuDBException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#directRangeLookup(int, int, byte[], byte[], boolean)
     */
    @Override
    public ResultSet<byte[], byte[]> directRangeLookup(int indexId, int snapId, byte[] from, byte[] to,
            boolean ascending) throws BabuDBException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.xtreemfs.babudb.api.dev.DatabaseInternal#insert(org.xtreemfs.babudb.lsmdb.BabuDBInsertGroup, java.lang.Object)
     */
    @Override
    public DatabaseRequestResult<Object> insert(BabuDBInsertGroup irg, Object context) {
        // TODO Auto-generated method stub
        return null;
    }
}