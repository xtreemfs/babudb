package org.xtreemfs.babudb.interfaces;

import java.io.StringWriter;
import org.xtreemfs.*;
import org.xtreemfs.babudb.*;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.interfaces.utils.*;

import yidl.runtime.Marshaller;
import yidl.runtime.PrettyPrinter;
import yidl.runtime.Struct;
import yidl.runtime.Unmarshaller;




public class LogEntry implements Struct
{
    public static final int TAG = 1022;
    
    public LogEntry() {  }
    public LogEntry( ReusableBuffer payload ) { this.payload = payload; }

    public ReusableBuffer getPayload() { return payload; }
    public void setPayload( ReusableBuffer payload ) { this.payload = payload; }

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
    public static final long serialVersionUID = 1022;    

    // yidl.runtime.Object
    public int getTag() { return 1022; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::LogEntry"; }
    
    public int getXDRSize()
    {
        int my_size = 0;
        my_size += Integer.SIZE / 8 + ( payload != null ? ( ( payload.remaining() % 4 == 0 ) ? payload.remaining() : ( payload.remaining() + 4 - payload.remaining() % 4 ) ) : 0 ); // payload
        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {
        marshaller.writeBuffer( "payload", payload );
    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
        payload = ( ReusableBuffer )unmarshaller.readBuffer( "payload" );    
    }
        
    

    private ReusableBuffer payload;    

}

