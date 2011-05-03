/*
 * Copyright (c) 2011, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
 */
package org.xtreemfs.babudb.replication;

import org.junit.Test;
import org.xtreemfs.foundation.buffer.BufferPool;
import org.xtreemfs.foundation.buffer.ReusableBuffer;

import static junit.framework.Assert.*;


/**
 * @author flangner
 * @since 05/03/2011
 */
public class ReusableBufferUsageTest {
    
    /**
     * @throws Exception
     */
    @Test
    public void testExtendReusableBuffer() throws Exception {
        
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();
        
        // store data to the buffer
        ReusableBuffer testBuffer = BufferPool.allocate(key.length + value.length);
                
        testBuffer.put(key);
        testBuffer.put(value);
        testBuffer.flip();
        
        // retrieve data from the buffer
        byte[] rKey = new byte[key.length];
        byte[] rValue = new byte[value.length];
        
        testBuffer.get(rKey, 0, key.length);
        assertEquals(new String(key), new String(rKey));
        
        testBuffer.get(rValue, 0, value.length);
        assertEquals(new String(value), new String (rValue));
    }
}
