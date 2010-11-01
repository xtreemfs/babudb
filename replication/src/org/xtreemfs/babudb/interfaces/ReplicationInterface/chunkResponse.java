package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import java.io.StringWriter;
import org.xtreemfs.*;
import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.oncrpc.utils.*;
import yidl.runtime.Marshaller;
import yidl.runtime.PrettyPrinter;
import yidl.runtime.Struct;
import yidl.runtime.Unmarshaller;




public class chunkResponse extends org.xtreemfs.foundation.oncrpc.utils.Response
{
    public static final int TAG = 1013;

    public chunkResponse() {  }
    public chunkResponse( ReusableBuffer returnValue ) { this.returnValue = returnValue; }

    public ReusableBuffer getReturnValue() { return returnValue; }
    public void setReturnValue( ReusableBuffer returnValue ) { this.returnValue = returnValue; }

    // java.lang.Object
    public String toString()
    {
        StringWriter string_writer = new StringWriter();
        string_writer.append(this.getClass().getCanonicalName());
        string_writer.append(" ");
        PrettyPrinter pretty_printer = new PrettyPrinter( string_writer );
        pretty_printer.writeStruct( "", this );
        return string_writer.toString();
    }

    // java.io.Serializable
    public static final long serialVersionUID = 1013;

    // yidl.runtime.Object
    public int getTag() { return 1013; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::chunkResponse"; }

    public int getXDRSize()
    {
        int my_size = 0;
        my_size += Integer.SIZE / 8 + ( returnValue != null ? ( ( returnValue.remaining() % 4 == 0 ) ? returnValue.remaining() : ( returnValue.remaining() + 4 - returnValue.remaining() % 4 ) ) : 0 ); // returnValue
        return my_size;
    }

    public void marshal( Marshaller marshaller )
    {
        marshaller.writeBuffer( "returnValue", returnValue );
    }

    public void unmarshal( Unmarshaller unmarshaller )
    {
        returnValue = ( ReusableBuffer )unmarshaller.readBuffer( "returnValue" );
    }

    private ReusableBuffer returnValue;
}
