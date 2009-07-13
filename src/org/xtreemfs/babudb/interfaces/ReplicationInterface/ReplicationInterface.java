package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.babudb.interfaces.Exceptions.*;




public class ReplicationInterface
{
    public static final int DEFAULT_MASTER_PORT = 35666;
    public static final int DEFAULT_MASTERS_PORT = 35666;
    public static final int DEFAULT_SLAVE_PORT = 35667;
    public static final int DEFAULT_SLAVES_PORT = 35667;


    public static int getVersion() { return 1; }

    public static Request createRequest( ONCRPCRequestHeader header ) throws Exception
    {
        switch( header.getOperationNumber() )
        {
            case 1: return new stateRequest();
            case 2: return new replicaRequest();
            case 3: return new loadRequest();
            case 4: return new chunkRequest();
            case 5: return new heartbeatRequest();
            case 6: return new replicateRequest();
            case 7: return new createRequest();
            case 8: return new copyRequest();
            case 9: return new deleteRequest();

            default: throw new Exception( "unknown request number " + Integer.toString( header.getOperationNumber() ) );
        }
    }
            
    public static Response createResponse( ONCRPCResponseHeader header ) throws Exception
    {
        switch( header.getXID() )
        {
            case 1: return new stateResponse();            case 2: return new replicaResponse();            case 3: return new loadResponse();            case 4: return new chunkResponse();            case 5: return new heartbeatResponse();            case 6: return new replicateResponse();            case 7: return new createResponse();            case 8: return new copyResponse();            case 9: return new deleteResponse();
            default: throw new Exception( "unknown response number " + Integer.toString( header.getXID() ) );
        }
    }    

}
