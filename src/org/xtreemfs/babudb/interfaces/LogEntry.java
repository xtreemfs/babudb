package org.xtreemfs.babudb.interfaces;

import org.xtreemfs.babudb.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class LogEntry implements org.xtreemfs.babudb.interfaces.utils.Serializable
{
    public static final int TAG = 1022;

    
    public LogEntry() { payload = null; }
    public LogEntry( ReusableBuffer payload ) { this.payload = payload; }
    public LogEntry( Object from_hash_map ) { payload = null; this.deserialize( from_hash_map ); }
    public LogEntry( Object[] from_array ) { payload = null;this.deserialize( from_array ); }

    public ReusableBuffer getPayload() { return payload; }
    public void setPayload( ReusableBuffer payload ) { this.payload = payload; }

    // Object
    public String toString()
    {
        return "LogEntry( " + "\"" + payload + "\"" + " )";
    }

    // Serializable
    public int getTag() { return 1022; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::LogEntry"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.payload = ( ReusableBuffer )from_hash_map.get( "payload" );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.payload = ( ReusableBuffer )from_array[0];        
    }

    public void deserialize( ReusableBuffer buf )
    {
        { payload = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeSerializableBuffer( buf ); }
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "payload", payload );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        { org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeSerializableBuffer( payload, writer ); }
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializableBufferLength( payload );
        return my_size;
    }


    private ReusableBuffer payload;    

}

