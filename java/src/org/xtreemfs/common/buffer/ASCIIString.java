/*
 * Copyright (c) 2008, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/

package org.xtreemfs.common.buffer;

import java.io.Serializable;

/**
 *
 * @author bjko
 */
public final class ASCIIString implements Serializable {

    private byte[] data;

    private int hash;

    protected ASCIIString() {

    }

    /**
     * Creates a new instance of ASCIIString
     */
    public ASCIIString(String str) {
        this.data = str.getBytes();
    }

    /**
     * Creates a new instance of ASCIIString
     */
    protected ASCIIString(byte[] data) {
        this.data = data;
    }

    public String toString() {
        return new String(data);
    }

    public char charAt(int index) {

        return (char)data[index];
    }

    private byte unckeckedGetByte(int index) {
        return data[index];
    }

    public boolean equals(Object o) {
        if (o == null) return false;
        try {
            ASCIIString other = (ASCIIString)o;

            for (int i = 0; i < data.length; i++) {
                if (this.unckeckedGetByte(i) != other.unckeckedGetByte(i))
                    return false;
            }
            return true;
        } catch (ClassCastException ex) {
            return false;
        }
    }

    public void marshall(ReusableBuffer target) {
        target.putInt(data.length);
        target.put(data);
        
    }

    public static ASCIIString unmarshall(ReusableBuffer target) {

        int length = target.getInt();
        if (length < 0)
            return null;
        byte[] tmp = new byte[length];

        target.get(tmp);
        
        return new ASCIIString(tmp);
    }

    public int hashCode() {
	int h = hash;
	if (h == 0) {

            for (int i = 0; i < data.length; i++) {
                h = 31*h + data[i];
            }
            hash = h;
        }
        return h;
    }

}
