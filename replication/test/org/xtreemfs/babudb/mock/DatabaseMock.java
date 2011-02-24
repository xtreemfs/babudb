package org.xtreemfs.babudb.mock;

import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.DatabaseInsertGroup;
import org.xtreemfs.babudb.api.database.DatabaseRequestResult;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.database.UserDefinedLookup;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.api.index.ByteRangeComparator;

public class DatabaseMock implements Database {

    private String name;
    
    public DatabaseMock(String name, int numIndices, ByteRangeComparator[] comps) {
        this.name = name;
    }
    
    @Override
    public DatabaseInsertGroup createInsertGroup() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ByteRangeComparator[] getComparators() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DatabaseRequestResult<Object> insert(DatabaseInsertGroup irg,
            Object context) {
        // TODO Auto-generated method stub
        return null;
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

}
