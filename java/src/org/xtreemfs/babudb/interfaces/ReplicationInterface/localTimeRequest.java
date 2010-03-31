package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import java.io.StringWriter;
import org.xtreemfs.*;
import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import yidl.runtime.Marshaller;
import yidl.runtime.PrettyPrinter;
import yidl.runtime.Struct;
import yidl.runtime.Unmarshaller;




public class localTimeRequest extends org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1015;
    
    public localTimeRequest() {  }

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

    // Request
    public Response createDefaultResponse() { return new localTimeResponse(); }


    // java.io.Serializable
    public static final long serialVersionUID = 1015;    

    // yidl.runtime.Object
    public int getTag() { return 1015; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::localTimeRequest"; }
    
    public int getXDRSize()
    {
        int my_size = 0;

        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {

    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
    
    }
        
        

}

