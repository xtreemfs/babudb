package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class heartbeatRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public heartbeatRequest() { lsn = new LSN(); }
    public heartbeatRequest( LSN lsn ) { this.lsn = lsn; }
    public heartbeatRequest( Object from_hash_map ) { lsn = new LSN(); this.deserialize( from_hash_map ); }
    public heartbeatRequest( Object[] from_array ) { lsn = new LSN();this.deserialize( from_array ); }

    public LSN getLsn() { return lsn; }
    public void setLsn( LSN lsn ) { this.lsn = lsn; }

    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::heartbeatRequest"; }    
    public long getTypeId() { return 5; }

    public String toString()
    {
        return "heartbeatRequest( " + lsn.toString() + " )";
    }


    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.lsn.deserialize( from_hash_map.get( "lsn" ) );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.lsn.deserialize( from_array[0] );        
    }

    public void deserialize( ReusableBuffer buf )
    {
        lsn = new LSN(); lsn.deserialize( buf );
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "lsn", lsn.serialize() );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        lsn.serialize( writer );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += lsn.calculateSize();
        return my_size;
    }

    // Request
    public int getOperationNumber() { return 5; }
    public Response createDefaultResponse() { return new heartbeatResponse(); }


    private LSN lsn;

}

