/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.xtreemfs.babudb.lsmdb;

import java.util.Iterator;
import java.util.Map.Entry;
import org.xtreemfs.babudb.BabuDBException;
import org.xtreemfs.babudb.index.LSMTree;

/**
 *
 * @author bjko
 */
public class LSMLookupInterface {

    private final LSMDatabase database;
    
    public LSMLookupInterface(LSMDatabase database) {
        this.database = database;
    }
    
    public byte[] lookup(int indexId, byte[] key) throws BabuDBException {
        LSMTree tree = database.getIndex(indexId);
        if (tree == null)
            throw new BabuDBException(BabuDBException.ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        return tree.lookup(key);
    }
    
    public byte[] lookup(int indexId, byte[] key, int snapId) throws BabuDBException {
        LSMTree tree = database.getIndex(indexId);
        if (tree == null)
            throw new BabuDBException(BabuDBException.ErrorCode.NO_SUCH_INDEX, "index " + indexId + " does not exist");
        return tree.lookup(key,snapId);
    }
    
    public Iterator<Entry<byte[],byte[]>> prefixLookup(int indexId, byte[] startKey) throws BabuDBException {
        LSMTree tree = database.getIndex(indexId);
        if (tree == null)
            throw new BabuDBException(BabuDBException.ErrorCode.NO_SUCH_INDEX, "index does not exist");
        return tree.prefixLookup(startKey);
    }
    
    public Iterator<Entry<byte[],byte[]>> prefixLookup(int indexId, byte[] startKey, int snapId) throws BabuDBException {
        LSMTree tree = database.getIndex(indexId);
        if (tree == null)
            throw new BabuDBException(BabuDBException.ErrorCode.NO_SUCH_INDEX, "index does not exist");
        return tree.prefixLookup(startKey,snapId);
    }
    
}
