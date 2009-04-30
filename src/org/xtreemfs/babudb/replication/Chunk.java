/*
 * Copyright (c) 2009, Jan Stender, Bjoern Kolbeck, Mikael Hoegqvist,
 *                     Felix Hupfeld, Felix Langner, Zuse Institute Berlin
 * 
 * Licensed under the BSD License, see LICENSE file for details.
 * 
*/
package org.xtreemfs.babudb.replication;

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.Checksum;

import org.xtreemfs.include.common.buffer.BufferPool;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.json.JSONException;
import org.xtreemfs.include.foundation.json.JSONParser;
import org.xtreemfs.include.foundation.json.JSONString;

/**
 * <p>Holds necessary informations for identifying a Chunk (a piece of a file) explicit.</p>
 * 
 * @author flangner
 */
class Chunk implements Comparable<Chunk> {
    /** name of the file, where the chunk is located at */
    private final String fileName;
    
    /** start inclusive of the byte-range */
    private final Long begin;
    
    /** end inclusive of the byte-range */
    private final Long end;
    
    /** the data of the fileChunk */
    private ReusableBuffer data;
    
    /** default encoding of the chunkData */
    private static final Charset  ENC_UTF8 = Charset.forName("utf8");
    
    private static final boolean USE_CHECKSUMS = true;
    
    /**
     * <p>Allocates the given range (<code>r1</code>,<code>r2</code>) in the
     * correct order to begin and end of the byte range of file with <code>fName</code>.</p>
     * 
     * @param fName
     * @param r1
     * @param r2
     */
    Chunk(String fName,long r1, long r2) {
        assert(fName!=null) : "The name of a chunk can't be null!";
        
        fileName = fName;
        if (r1<=r2) {
            begin = r1;
            end = r2;
        }else {
            begin = r2;
            end = r1;
        }
        
        assert (begin!=null) : "Illegal byte-range for a chunk.("+fileName+")";
        assert (end!=null) : "Illegal byte-range for a chunk.("+fileName+")";
    }
    
    /**
     * @param csumAlgo
     * @return a Pinky/Speedy compatible {@link ReusableBuffer} representation of the Chunk. 
     * @throws JSONException if conversion was not successful.
     */
    ReusableBuffer serialize(Checksum csumAlgo) throws JSONException{
	// convert the chunk-metadata to JSON
	List<Object> metadata = new LinkedList<Object>();
        metadata.add(fileName);
        metadata.add(begin);
        metadata.add(end);
        String metaString = JSONParser.writeJSON(metadata);

        // allocate the buffer
        final int headerLength = metaString.getBytes(ENC_UTF8).length+Integer.SIZE/8*5;
        final int bufSize = headerLength+((data != null) ? data.remaining() : 0);
        ReusableBuffer result = BufferPool.allocate(bufSize);
        
        // put the data on the buffer
        result.putInt(bufSize);
        result.putInt(0); // placeholder for the checksum
        result.putInt(headerLength);
        result.putString(metaString);
        if (data!=null){
            result.put(data);
            data.flip(); // otherwise payload is not reusable
        }
        result.putInt(bufSize);
        result.flip();
        
        if (USE_CHECKSUMS) {           
            csumAlgo.update(result.array(),0,result.limit());
            int cPos = result.position();
            result.position(Integer.SIZE/8);
            result.putInt((int)csumAlgo.getValue());
            result.position(cPos);
        } 
        
        return result;
    }

    /**
     * Checks the integrity of the {@link Chunk}-buffer.
     * 
     * @param data
     * @throws Exception if integrityCheck was not successful.
     */
    static void checkIntegrity(ReusableBuffer data) throws Exception{
        int cPos = data.position();
        
        if (data.remaining() < Integer.SIZE/8)
            throw new Exception("Empty data. Cannot read log entry.");
        
        int length1 = data.getInt();
        
        if ((length1-Integer.SIZE/8) > data.remaining()) {
            data.position(cPos);
            Logging.logMessage(Logging.LEVEL_DEBUG, null,"not long enough");
            throw new Exception("The log entry is incomplete. The length indicated in the header "+
                    "exceeds the available data.");
        }
        
        data.position(cPos+length1-Integer.SIZE/8);
        
        int length2 = data.getInt();
        
        data.position(cPos);
        
        if (length1 != length2) {
            throw new Exception("Invalid Frame. The length entries do not match.");
        }
    } 
    
    /**
     * <p>Converts the given <code>buffer</code> back to a {@link Chunk}.</p>
     * 
     * @param buffer
     * @param csumAlgo
     * @throws Exception if Chunk could not be retrieved.
     * @return the retrieved {@link Chunk}.
     */
    @SuppressWarnings("unchecked")
    static Chunk deserialize(ReusableBuffer buffer,Checksum csumAlgo) throws Exception{
	Chunk result;
	
	final int bufSize = buffer.getInt();
	int chkSum = buffer.getInt();
	final int headerLength = buffer.getInt();
	String metaData = buffer.getString();
	
        List<Object> json = (List<Object>) JSONParser.parseJSON(new JSONString(metaData));
        result = new Chunk((String) json.get(0), (Long) json.get(1), (Long) json.get(2));
        
        final int dataSize = bufSize - headerLength;
	if (dataSize != 0){
    	int dataPosition = buffer.position();
            ReusableBuffer data = buffer.createViewBuffer();
            data.range(dataPosition, dataSize);
            result.addData(data);
	}
        
        if (USE_CHECKSUMS) {
            buffer.position(Integer.SIZE/8);
            buffer.putInt(0);
            buffer.position(0);
            csumAlgo.update(buffer.array(),0,buffer.limit());

            int csum = (int)csumAlgo.getValue();        

            if (csum != chkSum) {
                throw new Exception("Invalid Checksum. Checksum in log entry and calculated checksum do not match.");
            }
        } 
        
        return result;
    }
    
    /**
     * Returns the data-buffer to the {@link BufferPool}.
     */
    void free(){
	if (data!=null) BufferPool.free(data);
    }
    
    /**
     * Adds the given data to the fileChunk meta informations.
     * 
     * @param data
     */
    void addData(byte[] data){
	addData(ReusableBuffer.wrap(data));
    }
    
    /**
     * Adds the given data to the fileChunk meta informations.
     * 
     * @param data
     */
    void addData(ReusableBuffer data){
	assert (data != null);
	assert (this.data == null);
	
	this.data = data;
    }
    
    /**
     * 
     * @return the byte-range beginning position.
     */
    long getBegin() {
        return begin;
    }
    
    /**
     * 
     * @return the byte-range ending position.
     */
    long getEnd() {
        return end;
    }
    
    /**
     * 
     * @return the fileName, where the chunk is located at.
     */
    String getFileName() {
        return fileName;
    }
    
    /**
     * 
     * @return the data of the fileChunk given by its meta-data.
     */
    byte[] getData(){
	return data.array();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Chunk o) {
        if (fileName.equals(o.fileName)) {
            if (begin.equals(o.begin)) {
                if (end.equals(o.end)) {
                    return 0;
                }
                return end.compareTo(o.end);
            }
            return begin.compareTo(o.begin);
        }
        return fileName.compareTo(o.fileName);
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj==null) return false;
        
        Chunk o = (Chunk) obj;       
        return fileName.equals(o.fileName) && begin.equals(o.begin) && end.equals(o.end);
    }
    
    @Override
    public int hashCode() {
        return fileName.hashCode()+begin.hashCode()+end.hashCode();
    }
    
    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Chunk of '"+fileName+"': ["+begin+";"+end+"]"+"|"+((data==null) ? "Without" : "With")+" data attached.";
    }
}
