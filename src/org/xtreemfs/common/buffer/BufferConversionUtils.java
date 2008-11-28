/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.common.buffer;

import java.nio.ByteBuffer;

/** This class contains some convenience methods for very diverses uses
 *
 * @author Jesus Malo (jmalo)
 */
public class BufferConversionUtils {
    
    /**
     * Creates a new instance of BufferConversionUtils
     */
    public BufferConversionUtils() {
    }
    
    /** It gets the array of bytes of a ByteBuffer
     *  @param source The object containing the require array of bytes
     *  @return The array of bytes contained in the given ByteBuffer
     */
    public static byte [] arrayOf(ByteBuffer source) {
        byte [] array;
        
        if (source.hasArray()) {
            array = source.array();
        } else {
            array = new byte[source.capacity()];
            source.position(0);
            source.get(array);
        }
        
        return array;
    }
     
    
}
