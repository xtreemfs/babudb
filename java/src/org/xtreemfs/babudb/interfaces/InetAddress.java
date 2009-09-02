package org.xtreemfs.babudb.interfaces;

import org.xtreemfs.babudb.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class InetAddress implements org.xtreemfs.babudb.interfaces.utils.Serializable
{
    public static final int TAG = 1027;

    
    public InetAddress() { address = ""; port = 0; }
    public InetAddress( String address, int port ) { this.address = address; this.port = port; }
    public InetAddress( Object from_hash_map ) { address = ""; port = 0; this.deserialize( from_hash_map ); }
    public InetAddress( Object[] from_array ) { address = ""; port = 0;this.deserialize( from_array ); }

    public String getAddress() { return address; }
    public void setAddress( String address ) { this.address = address; }
    public int getPort() { return port; }
    public void setPort( int port ) { this.port = port; }

    // Object
    public String toString()
    {
        return "InetAddress( " + "\"" + address + "\"" + ", " + Integer.toString( port ) + " )";
    }

    // Serializable
    public int getTag() { return 1027; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::InetAddress"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.address = ( String )from_hash_map.get( "address" );
        this.port = ( from_hash_map.get( "port" ) instanceof Integer ) ? ( ( Integer )from_hash_map.get( "port" ) ).intValue() : ( ( Long )from_hash_map.get( "port" ) ).intValue();
    }
    
    public void deserialize( Object[] from_array )
    {
        this.address = ( String )from_array[0];
        this.port = ( from_array[1] instanceof Integer ) ? ( ( Integer )from_array[1] ).intValue() : ( ( Long )from_array[1] ).intValue();        
    }

    public void deserialize( ReusableBuffer buf )
    {
        address = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeString( buf );
        port = buf.getInt();
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "address", address );
        to_hash_map.put( "port", new Integer( port ) );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeString( address, writer );
        writer.putInt( port );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.stringLengthPadded(address);
        my_size += ( Integer.SIZE / 8 );
        return my_size;
    }


    private String address;
    private int port;    

}

