package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import java.io.StringWriter;
import org.xtreemfs.*;
import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.interfaces.utils.*;

import yidl.runtime.Marshaller;
import yidl.runtime.PrettyPrinter;
import yidl.runtime.Struct;
import yidl.runtime.Unmarshaller;




public class replicaResponse extends org.xtreemfs.interfaces.utils.Response
{
    public static final int TAG = 1016;
    
    public replicaResponse() { returnValue = new LogEntries();  }
    public replicaResponse( LogEntries returnValue ) { this.returnValue = returnValue; }

    public LogEntries getReturnValue() { return returnValue; }
    public void setReturnValue( LogEntries returnValue ) { this.returnValue = returnValue; }

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
    public static final long serialVersionUID = 1016;    

    // yidl.runtime.Object
    public int getTag() { return 1016; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::replicaResponse"; }
    
    public int getXDRSize()
    {
        int my_size = 0;
        my_size += returnValue.getXDRSize(); // returnValue
        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {
        marshaller.writeSequence( "returnValue", returnValue );
    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
        returnValue = new LogEntries(); unmarshaller.readSequence( "returnValue", returnValue );    
    }
        
    

    private LogEntries returnValue;    

}

