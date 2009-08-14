package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class loadResponse implements org.xtreemfs.babudb.interfaces.utils.Response
{
    public static final int TAG = 1013;

    
    public loadResponse() { returnValue = new DBFileMetaDataSet(); }
    public loadResponse( DBFileMetaDataSet returnValue ) { this.returnValue = returnValue; }
    public loadResponse( Object from_hash_map ) { returnValue = new DBFileMetaDataSet(); this.deserialize( from_hash_map ); }
    public loadResponse( Object[] from_array ) { returnValue = new DBFileMetaDataSet();this.deserialize( from_array ); }

    public DBFileMetaDataSet getReturnValue() { return returnValue; }
    public void setReturnValue( DBFileMetaDataSet returnValue ) { this.returnValue = returnValue; }

    // Object
    public String toString()
    {
        return "loadResponse( " + returnValue.toString() + " )";
    }

    // Serializable
    public int getTag() { return 1013; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::loadResponse"; }

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
        returnValue = new DBFileMetaDataSet(); returnValue.deserialize( buf );
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


    private DBFileMetaDataSet returnValue;    

}

