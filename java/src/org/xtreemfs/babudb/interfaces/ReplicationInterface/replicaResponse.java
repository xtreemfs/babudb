package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class replicaResponse implements org.xtreemfs.babudb.interfaces.utils.Response
{
    public replicaResponse() { returnValue = new LogEntries(); }
    public replicaResponse( LogEntries returnValue ) { this.returnValue = returnValue; }
    public replicaResponse( Object from_hash_map ) { returnValue = new LogEntries(); this.deserialize( from_hash_map ); }
    public replicaResponse( Object[] from_array ) { returnValue = new LogEntries();this.deserialize( from_array ); }

    public LogEntries getReturnValue() { return returnValue; }
    public void setReturnValue( LogEntries returnValue ) { this.returnValue = returnValue; }

    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::replicaResponse"; }    
    public long getTypeId() { return 2; }

    public String toString()
    {
        return "replicaResponse( " + returnValue.toString() + " )";
    }


    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.returnValue.deserialize( ( Object[] )from_hash_map.get( "returnValue" ) );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.returnValue.deserialize( ( Object[] )from_array[0] );        
    }

    public void deserialize( ReusableBuffer buf )
    {
        returnValue = new LogEntries(); returnValue.deserialize( buf );
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

    // Response
    public int getOperationNumber() { return 2; }


    private LogEntries returnValue;

}

