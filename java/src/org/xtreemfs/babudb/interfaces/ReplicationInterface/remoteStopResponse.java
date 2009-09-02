package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class remoteStopResponse implements org.xtreemfs.babudb.interfaces.utils.Response
{
    public static final int TAG = 1012;

    
    public remoteStopResponse() { returnValue = new LSN(); }
    public remoteStopResponse( LSN returnValue ) { this.returnValue = returnValue; }
    public remoteStopResponse( Object from_hash_map ) { returnValue = new LSN(); this.deserialize( from_hash_map ); }
    public remoteStopResponse( Object[] from_array ) { returnValue = new LSN();this.deserialize( from_array ); }

    public LSN getReturnValue() { return returnValue; }
    public void setReturnValue( LSN returnValue ) { this.returnValue = returnValue; }

    // Object
    public String toString()
    {
        return "remoteStopResponse( " + returnValue.toString() + " )";
    }

    // Serializable
    public int getTag() { return 1012; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::remoteStopResponse"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.returnValue.deserialize( from_hash_map.get( "returnValue" ) );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.returnValue.deserialize( from_array[0] );        
    }

    public void deserialize( ReusableBuffer buf )
    {
        returnValue = new LSN(); returnValue.deserialize( buf );
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "returnValue", returnValue.serialize() );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        returnValue.serialize( writer );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += returnValue.calculateSize();
        return my_size;
    }


    private LSN returnValue;    

}

