package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class createRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1017;

    
    public createRequest() { lsn = new LSN(); databaseName = ""; numIndices = 0; }
    public createRequest( LSN lsn, String databaseName, int numIndices ) { this.lsn = lsn; this.databaseName = databaseName; this.numIndices = numIndices; }
    public createRequest( Object from_hash_map ) { lsn = new LSN(); databaseName = ""; numIndices = 0; this.deserialize( from_hash_map ); }
    public createRequest( Object[] from_array ) { lsn = new LSN(); databaseName = ""; numIndices = 0;this.deserialize( from_array ); }

    public LSN getLsn() { return lsn; }
    public void setLsn( LSN lsn ) { this.lsn = lsn; }
    public String getDatabaseName() { return databaseName; }
    public void setDatabaseName( String databaseName ) { this.databaseName = databaseName; }
    public int getNumIndices() { return numIndices; }
    public void setNumIndices( int numIndices ) { this.numIndices = numIndices; }

    // Object
    public String toString()
    {
        return "createRequest( " + lsn.toString() + ", " + "\"" + databaseName + "\"" + ", " + Integer.toString( numIndices ) + " )";
    }

    // Serializable
    public int getTag() { return 1017; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::createRequest"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.lsn.deserialize( from_hash_map.get( "lsn" ) );
        this.databaseName = ( String )from_hash_map.get( "databaseName" );
        this.numIndices = ( from_hash_map.get( "numIndices" ) instanceof Integer ) ? ( ( Integer )from_hash_map.get( "numIndices" ) ).intValue() : ( ( Long )from_hash_map.get( "numIndices" ) ).intValue();
    }
    
    public void deserialize( Object[] from_array )
    {
        this.lsn.deserialize( from_array[0] );
        this.databaseName = ( String )from_array[1];
        this.numIndices = ( from_array[2] instanceof Integer ) ? ( ( Integer )from_array[2] ).intValue() : ( ( Long )from_array[2] ).intValue();        
    }

    public void deserialize( ReusableBuffer buf )
    {
        lsn = new LSN(); lsn.deserialize( buf );
        databaseName = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeString( buf );
        numIndices = buf.getInt();
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "lsn", lsn.serialize() );
        to_hash_map.put( "databaseName", databaseName );
        to_hash_map.put( "numIndices", new Integer( numIndices ) );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        lsn.serialize( writer );
        org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeString( databaseName, writer );
        writer.putInt( numIndices );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += lsn.calculateSize();
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.stringLengthPadded(databaseName);
        my_size += ( Integer.SIZE / 8 );
        return my_size;
    }

    // Request
    public Response createDefaultResponse() { return new createResponse(); }


    private LSN lsn;
    private String databaseName;
    private int numIndices;    

}

