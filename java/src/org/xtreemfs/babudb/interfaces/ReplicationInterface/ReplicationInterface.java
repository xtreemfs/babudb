package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import java.io.StringWriter;
import org.xtreemfs.*;
import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import yidl.runtime.PrettyPrinter;




public class ReplicationInterface
{
    public static final int DEFAULT_PORT = 35667;
    public static final int DEFAULT_SPORT = 35667;


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
            case 1012: return new loadRequest();
            case 1013: return new chunkRequest();
            case 1014: return new fleaseRequest();
            case 1015: return new replicaRequest();
            case 1016: return new heartbeatRequest();
            case 1017: return new replicateRequest();

            default: throw new Exception( "unknown request tag " + Integer.toString( header.getProcedure() ) );
        }
    }
            
    public static Response createResponse( ONCRPCResponseHeader header ) throws Exception
    {
        switch( header.getXID() )
        {
            case 1011: return new stateResponse();            case 1012: return new loadResponse();            case 1013: return new chunkResponse();            case 1014: return new fleaseResponse();            case 1015: return new replicaResponse();            case 1016: return new heartbeatResponse();            case 1017: return new replicateResponse();
            default: throw new Exception( "unknown response XID " + Integer.toString( header.getXID() ) );
        }
    }    

}
