package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class replicaRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public replicaRequest() { range = new LSNRange(); }
    public replicaRequest( LSNRange range ) { this.range = range; }
    public replicaRequest( Object from_hash_map ) { range = new LSNRange(); this.deserialize( from_hash_map ); }
    public replicaRequest( Object[] from_array ) { range = new LSNRange();this.deserialize( from_array ); }

    public LSNRange getRange() { return range; }
    public void setRange( LSNRange range ) { this.range = range; }

    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::replicaRequest"; }    
    public long getTypeId() { return 2; }

    public String toString()
    {
        return "replicaRequest( " + range.toString() + " )";
    }


    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.range.deserialize( from_hash_map.get( "range" ) );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.range.deserialize( from_array[0] );        
    }

    public void deserialize( ReusableBuffer buf )
    {
        range = new LSNRange(); range.deserialize( buf );
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "range", range.serialize() );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        range.serialize( writer );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += range.calculateSize();
        return my_size;
    }

    // Request
    public int getOperationNumber() { return 2; }
    public Response createDefaultResponse() { return new replicaResponse(); }


    private LSNRange range;

}

