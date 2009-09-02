package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class chunkRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1017;

    
    public chunkRequest() { chunk = new Chunk(); }
    public chunkRequest( Chunk chunk ) { this.chunk = chunk; }
    public chunkRequest( Object from_hash_map ) { chunk = new Chunk(); this.deserialize( from_hash_map ); }
    public chunkRequest( Object[] from_array ) { chunk = new Chunk();this.deserialize( from_array ); }

    public Chunk getChunk() { return chunk; }
    public void setChunk( Chunk chunk ) { this.chunk = chunk; }

    // Object
    public String toString()
    {
        return "chunkRequest( " + chunk.toString() + " )";
    }

    // Serializable
    public int getTag() { return 1017; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::chunkRequest"; }

    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.chunk.deserialize( from_hash_map.get( "chunk" ) );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.chunk.deserialize( from_array[0] );        
    }

    public void deserialize( ReusableBuffer buf )
    {
        chunk = new Chunk(); chunk.deserialize( buf );
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "chunk", chunk.serialize() );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        chunk.serialize( writer );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += chunk.calculateSize();
        return my_size;
    }

    // Request
    public Response createDefaultResponse() { return new chunkResponse(); }


    private Chunk chunk;    

}

