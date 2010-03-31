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




public class replicaRequest extends org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1016;
    
    public replicaRequest() { range = new LSNRange();  }
    public replicaRequest( LSNRange range ) { this.range = range; }

    public LSNRange getRange() { return range; }
    public void setRange( LSNRange range ) { this.range = range; }

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
    public Response createDefaultResponse() { return new replicaResponse(); }


    // java.io.Serializable
    public static final long serialVersionUID = 1016;    

    // yidl.runtime.Object
    public int getTag() { return 1016; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::replicaRequest"; }
    
    public int getXDRSize()
    {
        int my_size = 0;
        my_size += range.getXDRSize(); // range
        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {
        marshaller.writeStruct( "range", range );
    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
        range = new LSNRange(); unmarshaller.readStruct( "range", range );    
    }
        
    

    private LSNRange range;    

}

