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




public class InetAddress implements Struct
{
    public static final int TAG = 1027;
    
    public InetAddress() {  }
    public InetAddress( String address, int port ) { this.address = address; this.port = port; }

    public String getAddress() { return address; }
    public void setAddress( String address ) { this.address = address; }
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


    // java.io.Serializable
    public static final long serialVersionUID = 1027;    

    // yidl.runtime.Object
    public int getTag() { return 1027; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::InetAddress"; }
    
    public int getXDRSize()
    {
        int my_size = 0;
        my_size += Integer.SIZE / 8 + ( address != null ? ( ( address.getBytes().length % 4 == 0 ) ? address.getBytes().length : ( address.getBytes().length + 4 - address.getBytes().length % 4 ) ) : 0 ); // address
        my_size += Integer.SIZE / 8; // port
        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {
        marshaller.writeString( "address", address );
        marshaller.writeUint32( "port", port );
    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
        address = unmarshaller.readString( "address" );
        port = unmarshaller.readUint32( "port" );    
    }
        
    

    private String address;
    private int port;    

}

