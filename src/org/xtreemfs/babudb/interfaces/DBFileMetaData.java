package org.xtreemfs.babudb.interfaces;

import org.xtreemfs.babudb.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class DBFileMetaData implements org.xtreemfs.babudb.interfaces.utils.Serializable
{
    public DBFileMetaData() { fileName = ""; fileSize = 0; maxChunkSize = 0; }
    public DBFileMetaData( String fileName, long fileSize, int maxChunkSize ) { this.fileName = fileName; this.fileSize = fileSize; this.maxChunkSize = maxChunkSize; }
    public DBFileMetaData( Object from_hash_map ) { fileName = ""; fileSize = 0; maxChunkSize = 0; this.deserialize( from_hash_map ); }
    public DBFileMetaData( Object[] from_array ) { fileName = ""; fileSize = 0; maxChunkSize = 0;this.deserialize( from_array ); }

    public String getFileName() { return fileName; }
    public void setFileName( String fileName ) { this.fileName = fileName; }
    public long getFileSize() { return fileSize; }
    public void setFileSize( long fileSize ) { this.fileSize = fileSize; }
    public int getMaxChunkSize() { return maxChunkSize; }
    public void setMaxChunkSize( int maxChunkSize ) { this.maxChunkSize = maxChunkSize; }

    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::DBFileMetaData"; }    
    public long getTypeId() { return 0; }

    public String toString()
    {
        return "DBFileMetaData( " + "\"" + fileName + "\"" + ", " + Long.toString( fileSize ) + ", " + Integer.toString( maxChunkSize ) + " )";
    }


    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.fileName = ( String )from_hash_map.get( "fileName" );
        this.fileSize = ( ( Long )from_hash_map.get( "fileSize" ) ).longValue();
        this.maxChunkSize = ( ( Integer )from_hash_map.get( "maxChunkSize" ) ).intValue();
    }
    
    public void deserialize( Object[] from_array )
    {
        this.fileName = ( String )from_array[0];
        this.fileSize = ( ( Long )from_array[1] ).longValue();
        this.maxChunkSize = ( ( Integer )from_array[2] ).intValue();        
    }

    public void deserialize( ReusableBuffer buf )
    {
        fileName = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeString( buf );
        fileSize = buf.getLong();
        maxChunkSize = buf.getInt();
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "fileName", fileName );
        to_hash_map.put( "fileSize", new Long( fileSize ) );
        to_hash_map.put( "maxChunkSize", new Integer( maxChunkSize ) );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeString( fileName, writer );
        writer.putLong( fileSize );
        writer.putInt( maxChunkSize );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.stringLengthPadded(fileName);
        my_size += ( Long.SIZE / 8 );
        my_size += ( Integer.SIZE / 8 );
        return my_size;
    }


    private String fileName;
    private long fileSize;
    private int maxChunkSize;

}

