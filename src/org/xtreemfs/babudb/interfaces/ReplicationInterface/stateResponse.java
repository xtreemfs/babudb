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




public class stateResponse extends org.xtreemfs.interfaces.utils.Response
{
    public static final int TAG = 1011;
    
    public stateResponse() { returnValue = new LSN();  }
    public stateResponse( LSN returnValue ) { this.returnValue = returnValue; }

    public LSN getReturnValue() { return returnValue; }
    public void setReturnValue( LSN returnValue ) { this.returnValue = returnValue; }

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
    public static final long serialVersionUID = 1011;    

    // yidl.runtime.Object
    public int getTag() { return 1011; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::stateResponse"; }
    
    public int getXDRSize()
    {
        int my_size = 0;
        my_size += returnValue.getXDRSize(); // returnValue
        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {
        marshaller.writeStruct( "returnValue", returnValue );
    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
        returnValue = new LSN(); unmarshaller.readStruct( "returnValue", returnValue );    
    }
        
    

    private LSN returnValue;    

}

