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




public class heartbeatRequest extends org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1017;
    
    public heartbeatRequest() { lsn = new LSN();  }
    public heartbeatRequest( LSN lsn ) { this.lsn = lsn; }

    public LSN getLsn() { return lsn; }
    public void setLsn( LSN lsn ) { this.lsn = lsn; }

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
    public Response createDefaultResponse() { return new heartbeatResponse(); }


    // java.io.Serializable
    public static final long serialVersionUID = 1017;    

    // yidl.runtime.Object
    public int getTag() { return 1017; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::heartbeatRequest"; }
    
    public int getXDRSize()
    {
        int my_size = 0;
        my_size += lsn.getXDRSize(); // lsn
        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {
        marshaller.writeStruct( "lsn", lsn );
    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
        lsn = new LSN(); unmarshaller.readStruct( "lsn", lsn );    
    }
        
    

    private LSN lsn;    

}

