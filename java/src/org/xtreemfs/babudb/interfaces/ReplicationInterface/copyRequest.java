package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import java.util.HashMap;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class copyRequest implements org.xtreemfs.babudb.interfaces.utils.Request
{
    public copyRequest() { lsn = new LSN(); sourceDB = ""; destDB = ""; }
    public copyRequest( LSN lsn, String sourceDB, String destDB ) { this.lsn = lsn; this.sourceDB = sourceDB; this.destDB = destDB; }
    public copyRequest( Object from_hash_map ) { lsn = new LSN(); sourceDB = ""; destDB = ""; this.deserialize( from_hash_map ); }
    public copyRequest( Object[] from_array ) { lsn = new LSN(); sourceDB = ""; destDB = "";this.deserialize( from_array ); }

    public LSN getLsn() { return lsn; }
    public void setLsn( LSN lsn ) { this.lsn = lsn; }
    public String getSourceDB() { return sourceDB; }
    public void setSourceDB( String sourceDB ) { this.sourceDB = sourceDB; }
    public String getDestDB() { return destDB; }
    public void setDestDB( String destDB ) { this.destDB = destDB; }

    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::copyRequest"; }    
    public long getTypeId() { return 8; }

    public String toString()
    {
        return "copyRequest( " + lsn.toString() + ", " + "\"" + sourceDB + "\"" + ", " + "\"" + destDB + "\"" + " )";
    }


    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
        this.lsn.deserialize( from_hash_map.get( "lsn" ) );
        this.sourceDB = ( String )from_hash_map.get( "sourceDB" );
        this.destDB = ( String )from_hash_map.get( "destDB" );
    }
    
    public void deserialize( Object[] from_array )
    {
        this.lsn.deserialize( from_array[0] );
        this.sourceDB = ( String )from_array[1];
        this.destDB = ( String )from_array[2];        
    }

    public void deserialize( ReusableBuffer buf )
    {
        lsn = new LSN(); lsn.deserialize( buf );
        sourceDB = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeString( buf );
        destDB = org.xtreemfs.babudb.interfaces.utils.XDRUtils.deserializeString( buf );
    }

    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
        to_hash_map.put( "lsn", lsn.serialize() );
        to_hash_map.put( "sourceDB", sourceDB );
        to_hash_map.put( "destDB", destDB );
        return to_hash_map;        
    }

    public void serialize( ONCRPCBufferWriter writer ) 
    {
        lsn.serialize( writer );
        org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeString( sourceDB, writer );
        org.xtreemfs.babudb.interfaces.utils.XDRUtils.serializeString( destDB, writer );
    }
    
    public int calculateSize()
    {
        int my_size = 0;
        my_size += lsn.calculateSize();
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.stringLengthPadded(sourceDB);
        my_size += org.xtreemfs.babudb.interfaces.utils.XDRUtils.stringLengthPadded(destDB);
        return my_size;
    }

    // Request
    public int getOperationNumber() { return 8; }
    public Response createDefaultResponse() { return new copyResponse(); }


    private LSN lsn;
    private String sourceDB;
    private String destDB;

}

