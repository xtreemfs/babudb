package org.xtreemfs.babudb.interfaces;

import org.xtreemfs.babudb.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class Chunk implements org.xtreemfs.babudb.interfaces.utils.Serializable
{
    public static final int TAG = 1025;

    
    public Chunk() { fileName = ""; begin = 0; end = 0; }
    public Chunk( String fileName, long begin, long end ) { this.fileName = fileName; this.begin = begin; this.end = end; }
    public Chunk( Object from_hash_map ) { fileName = ""; begin = 0; end = 0; this.deserialize( from_hash_map ); }
    public Chunk( Object[] from_array ) { fileName = ""; begin = 0; end = 0;this.deserialize( from_array ); }

    public String getFileName() { return fileName; }
    public void setFileName( String fileName ) { this.fileName = fileName; }
    public long getBegin() { return begin; }
    public void setBegin( long begin ) { this.begin = begin; }
    public long getEnd() { return end; }
    public void setEnd( long end ) { this.end = end; }

    // Object
    public String toString()
    {
        return "Chunk( " + "\"" + fileName + "\"" + ", " + Long.toString( begin ) + ", " + Long.toString( end ) + " )";
    }

    // Serializable
    public int getTag() { return 1025; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::Chunk"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.fileName = ( String )from_hash_map.get( "fileName" );
        this.begin = ( from_hash_map.get( "begin" ) instanceof Integer ) ? ( ( Integer )from_hash_map.get( "begin" ) ).longValue() : ( ( Long )from_hash_map.get( "begin" ) ).longValue();
        this.end = ( from_hash_map.get( "end" ) instanceof Integer ) ? ( ( Integer )from_hash_map.get( "end" ) ).longValue() : ( ( Long )from_hash_map.get( "end" ) ).longValue();
    }
    
    public void deserialize( Object[] from_array )
    {
        this.fileName = ( String )from_array[0];
        this.begin = ( from_array[1] instanceof Integer ) ? ( ( Integer )from_array[1] ).longValue() : ( ( Long )from_array[1] ).longValue();
        this.end = ( from_array[2] instanceof Integer ) ? ( ( Integer )from_array[2] ).longValue() : ( ( Long )from_array[2] ).longValue();        
    }

    public void deserialize( ReusableBuffer buf )
    {
        fileName = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeString( buf );
        begin = buf.getLong();
        end = buf.getLong();
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "fileName", fileName );
        to_hash_map.put( "begin", new Long( begin ) );
        to_hash_map.put( "end", new Long( end ) );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeString( fileName, writer );
        writer.putLong( begin );
        writer.putLong( end );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.stringLengthPadded(fileName);
        my_size += ( Long.SIZE / 8 );
        my_size += ( Long.SIZE / 8 );
        return my_size;
    }


    private String fileName;
    private long begin;
    private long end;    

}

