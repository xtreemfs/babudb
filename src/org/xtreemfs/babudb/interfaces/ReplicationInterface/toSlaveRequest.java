package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class toSlaveRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1014;

    
    public toSlaveRequest() { address = new InetAddress(); }
    public toSlaveRequest( InetAddress address ) { this.address = address; }
    public toSlaveRequest( Object from_hash_map ) { address = new InetAddress(); this.deserialize( from_hash_map ); }
    public toSlaveRequest( Object[] from_array ) { address = new InetAddress();this.deserialize( from_array ); }

    public InetAddress getAddress() { return address; }
    public void setAddress( InetAddress address ) { this.address = address; }

    // Object
    public String toString()
    {
        return "toSlaveRequest( " + address.toString() + " )";
    }

    // Serializable
    public int getTag() { return 1014; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::toSlaveRequest"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.address.deserialize( from_hash_map.get( "address" ) );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.address.deserialize( from_array[0] );        
    }

    public void deserialize( ReusableBuffer buf )
    {
        address = new InetAddress(); address.deserialize( buf );
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "address", address.serialize() );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        address.serialize( writer );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += address.calculateSize();
        return my_size;
    }

    // Request
    public Response createDefaultResponse() { return new toSlaveResponse(); }


    private InetAddress address;    

}

