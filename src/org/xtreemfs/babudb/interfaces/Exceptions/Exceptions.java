package org.xtreemfs.babudb.interfaces.Exceptions;

import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.foundation.oncrpc.utils.ONCRPCBufferWriter;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import org.xtreemfs.babudb.interfaces.Exceptions.*;




public class Exceptions
{
    public static int getVersion() { return 1; }

    public static ONCRPCException createException( String exception_type_name ) throws java.io.IOException
    {
        if ( exception_type_name.equals("org::xtreemfs::babudb::interfaces::Exceptions::ProtocolException") ) return new ProtocolException();
        else if ( exception_type_name.equals("org::xtreemfs::babudb::interfaces::Exceptions::errnoException") ) return new errnoException();
        else throw new java.io.IOException( "unknown exception type " + exception_type_name );
    }

}
