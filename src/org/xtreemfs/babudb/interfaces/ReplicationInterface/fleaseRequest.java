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




public class fleaseRequest extends org.xtreemfs.babudb.interfaces.utils.Request
{
    public static final int TAG = 1014;
    
    public fleaseRequest() {  }
    public fleaseRequest( ReusableBuffer message, String host, int port ) { this.message = message; this.host = host; this.port = port; }

    public ReusableBuffer getMessage() { return message; }
    public void setMessage( ReusableBuffer message ) { this.message = message; }
    public String getHost() { return host; }
    public void setHost( String host ) { this.host = host; }
    public int getPort() { return port; }
    public void setPort( int port ) { this.port = port; }

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
    public Response createDefaultResponse() { return new fleaseResponse(); }


    // java.io.Serializable
    public static final long serialVersionUID = 1014;    

    // yidl.runtime.Object
    public int getTag() { return 1014; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::fleaseRequest"; }
    
    public int getXDRSize()
    {
        int my_size = 0;
        my_size += Integer.SIZE / 8 + ( message != null ? ( ( message.remaining() % 4 == 0 ) ? message.remaining() : ( message.remaining() + 4 - message.remaining() % 4 ) ) : 0 ); // message
        my_size += Integer.SIZE / 8 + ( host != null ? ( ( host.getBytes().length % 4 == 0 ) ? host.getBytes().length : ( host.getBytes().length + 4 - host.getBytes().length % 4 ) ) : 0 ); // host
        my_size += Integer.SIZE / 8; // port
        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {
        marshaller.writeBuffer( "message", message );
        marshaller.writeString( "host", host );
        marshaller.writeUint32( "port", port );
    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
        message = ( ReusableBuffer )unmarshaller.readBuffer( "message" );
        host = unmarshaller.readString( "host" );
        port = unmarshaller.readUint32( "port" );    
    }
        
    

    private ReusableBuffer message;
    private String host;
    private int port;    

}

