package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class chunkResponse implements org.xtreemfs.babudb.interfaces.utils.Response
{
    public static final int TAG = 1017;

    
    public chunkResponse() { returnValue = null; }
    public chunkResponse( ReusableBuffer returnValue ) { this.returnValue = returnValue; }
    public chunkResponse( Object from_hash_map ) { returnValue = null; this.deserialize( from_hash_map ); }
    public chunkResponse( Object[] from_array ) { returnValue = null;this.deserialize( from_array ); }

    public ReusableBuffer getReturnValue() { return returnValue; }
    public void setReturnValue( ReusableBuffer returnValue ) { this.returnValue = returnValue; }

    // Object
    public String toString()
    {
        return "chunkResponse( " + "\"" + returnValue + "\"" + " )";
    }

    // Serializable
    public int getTag() { return 1017; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::chunkResponse"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.returnValue = ( ReusableBuffer )from_hash_map.get( "returnValue" );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.returnValue = ( ReusableBuffer )from_array[0];        
    }

    public void deserialize( ReusableBuffer buf )
    {
        { returnValue = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeSerializableBuffer( buf ); }
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "returnValue", returnValue );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        { org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeSerializableBuffer( returnValue, writer ); }
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializableBufferLength( returnValue );
        return my_size;
    }


    private ReusableBuffer returnValue;    

}

