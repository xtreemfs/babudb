package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class deleteRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1019;

    
    public deleteRequest() { lsn = new LSN(); databaseName = ""; deleteFiles = false; }
    public deleteRequest( LSN lsn, String databaseName, boolean deleteFiles ) { this.lsn = lsn; this.databaseName = databaseName; this.deleteFiles = deleteFiles; }
    public deleteRequest( Object from_hash_map ) { lsn = new LSN(); databaseName = ""; deleteFiles = false; this.deserialize( from_hash_map ); }
    public deleteRequest( Object[] from_array ) { lsn = new LSN(); databaseName = ""; deleteFiles = false;this.deserialize( from_array ); }

    public LSN getLsn() { return lsn; }
    public void setLsn( LSN lsn ) { this.lsn = lsn; }
    public String getDatabaseName() { return databaseName; }
    public void setDatabaseName( String databaseName ) { this.databaseName = databaseName; }
    public boolean getDeleteFiles() { return deleteFiles; }
    public void setDeleteFiles( boolean deleteFiles ) { this.deleteFiles = deleteFiles; }

    // Object
    public String toString()
    {
        return "deleteRequest( " + lsn.toString() + ", " + "\"" + databaseName + "\"" + ", " + Boolean.toString( deleteFiles ) + " )";
    }

    // Serializable
    public int getTag() { return 1019; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::deleteRequest"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.lsn.deserialize( from_hash_map.get( "lsn" ) );
        this.databaseName = ( String )from_hash_map.get( "databaseName" );
        this.deleteFiles = ( ( Boolean )from_hash_map.get( "deleteFiles" ) ).booleanValue();
    }
    
    public void deserialize( Object[] from_array )
    {
        this.lsn.deserialize( from_array[0] );
        this.databaseName = ( String )from_array[1];
        this.deleteFiles = ( ( Boolean )from_array[2] ).booleanValue();        
    }

    public void deserialize( ReusableBuffer buf )
    {
        lsn = new LSN(); lsn.deserialize( buf );
        databaseName = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeString( buf );
        deleteFiles = buf.getInt() != 0;
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "lsn", lsn.serialize() );
        to_hash_map.put( "databaseName", databaseName );
        to_hash_map.put( "deleteFiles", new Boolean( deleteFiles ) );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        lsn.serialize( writer );
        org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeString( databaseName, writer );
        writer.putInt( deleteFiles ? 1 : 0 );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += lsn.calculateSize();
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.stringLengthPadded(databaseName);
        my_size += 4;
        return my_size;
    }

    // Request
    public Response createDefaultResponse() { return new deleteResponse(); }


    private LSN lsn;
    private String databaseName;
    private boolean deleteFiles;    

}

