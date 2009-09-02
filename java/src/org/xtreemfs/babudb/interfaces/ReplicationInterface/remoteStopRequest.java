package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class remoteStopRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1012;

    
    public remoteStopRequest() {  }
    public remoteStopRequest( Object from_hash_map ) {  this.deserialize( from_hash_map ); }
    public remoteStopRequest( Object[] from_array ) { this.deserialize( from_array ); }

    // Object
    public String toString()
    {
        return "remoteStopRequest()";
    }

    // Serializable
    public int getTag() { return 1012; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::remoteStopRequest"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {

    }
    
    public void deserialize( Object[] from_array )
    {
        
    }

    public void deserialize( ReusableBuffer buf )
    {

    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {

    }
    
    public int calculateSize()
    {
        int my_size = 0;

        return my_size;
    }

    // Request
    public Response createDefaultResponse() { return new remoteStopResponse(); }
    

}

