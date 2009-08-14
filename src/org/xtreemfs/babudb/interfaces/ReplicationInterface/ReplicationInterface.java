package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;




public class ReplicationInterface
{
    public static final int DEFAULT_MASTER_PORT = 35666;
    public static final int DEFAULT_MASTERS_PORT = 35666;
    public static final int DEFAULT_SLAVE_PORT = 35667;
    public static final int DEFAULT_SLAVES_PORT = 35667;


    public static int getVersion() { return 1010; }

    public static ONCRPCException createException( int accept_stat ) throws Exception
    {
        switch( accept_stat )
        {
            case 1100: return new ProtocolException();
            case 1101: return new errnoException();

            default: throw new Exception( "unknown accept_stat " + Integer.toString( accept_stat ) );
        }
    }

    public static Request createRequest( ONCRPCRequestHeader header ) throws Exception
    {
        switch( header.getProcedure() )
        {
            case 1011: return new stateRequest();
            case 1012: return new replicaRequest();
            case 1013: return new loadRequest();
            case 1014: return new chunkRequest();
            case 1015: return new heartbeatRequest();
            case 1016: return new replicateRequest();
            case 1017: return new createRequest();
            case 1018: return new copyRequest();
            case 1019: return new deleteRequest();

            default: throw new Exception( "unknown request tag " + Integer.toString( header.getProcedure() ) );
        }
    }
            
    public static Response createResponse( ONCRPCResponseHeader header ) throws Exception
    {
        switch( header.getXID() )
        {
            case 1011: return new stateResponse();            case 1012: return new replicaResponse();            case 1013: return new loadResponse();            case 1014: return new chunkResponse();            case 1015: return new heartbeatResponse();            case 1016: return new replicateResponse();            case 1017: return new createResponse();            case 1018: return new copyResponse();            case 1019: return new deleteResponse();
            default: throw new Exception( "unknown response XID " + Integer.toString( header.getXID() ) );
        }
    }    

}
