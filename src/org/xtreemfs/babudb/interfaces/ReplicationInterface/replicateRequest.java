package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class replicateRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1019;

    
    public replicateRequest() { lsn = new LSN(); logEntry = new LogEntry(); }
    public replicateRequest( LSN lsn, LogEntry logEntry ) { this.lsn = lsn; this.logEntry = logEntry; }
    public replicateRequest( Object from_hash_map ) { lsn = new LSN(); logEntry = new LogEntry(); this.deserialize( from_hash_map ); }
    public replicateRequest( Object[] from_array ) { lsn = new LSN(); logEntry = new LogEntry();this.deserialize( from_array ); }

    public LSN getLsn() { return lsn; }
    public void setLsn( LSN lsn ) { this.lsn = lsn; }
    public LogEntry getLogEntry() { return logEntry; }
    public void setLogEntry( LogEntry logEntry ) { this.logEntry = logEntry; }

    // Object
    public String toString()
    {
        return "replicateRequest( " + lsn.toString() + ", " + logEntry.toString() + " )";
    }

    // Serializable
    public int getTag() { return 1019; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::replicateRequest"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.lsn.deserialize( from_hash_map.get( "lsn" ) );
        this.logEntry.deserialize( from_hash_map.get( "logEntry" ) );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.lsn.deserialize( from_array[0] );
        this.logEntry.deserialize( from_array[1] );        
    }

    public void deserialize( ReusableBuffer buf )
    {
        lsn = new LSN(); lsn.deserialize( buf );
        logEntry = new LogEntry(); logEntry.deserialize( buf );
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "lsn", lsn.serialize() );
        to_hash_map.put( "logEntry", logEntry.serialize() );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        lsn.serialize( writer );
        logEntry.serialize( writer );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += lsn.calculateSize();
        my_size += logEntry.calculateSize();
        return my_size;
    }

    // Request
    public Response createDefaultResponse() { return new replicateResponse(); }


    private LSN lsn;
    private LogEntry logEntry;    

}

