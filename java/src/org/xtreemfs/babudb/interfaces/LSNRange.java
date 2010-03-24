package org.xtreemfs.babudb.interfaces;

import java.io.StringWriter;
import org.xtreemfs.*;
import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.utils.*;
import org.xtreemfs.include.common.buffer.ReusableBuffer;
import yidl.runtime.Marshaller;
import yidl.runtime.PrettyPrinter;
import yidl.runtime.Struct;
import yidl.runtime.Unmarshaller;




public class LSNRange implements Struct
{
    public static final int TAG = 1026;
    
    public LSNRange() {  }
    public LSNRange( int viewId, long sequenceStart, long sequenceEnd ) { this.viewId = viewId; this.sequenceStart = sequenceStart; this.sequenceEnd = sequenceEnd; }

    public int getViewId() { return viewId; }
    public void setViewId( int viewId ) { this.viewId = viewId; }
    public long getSequenceStart() { return sequenceStart; }
    public void setSequenceStart( long sequenceStart ) { this.sequenceStart = sequenceStart; }
    public long getSequenceEnd() { return sequenceEnd; }
    public void setSequenceEnd( long sequenceEnd ) { this.sequenceEnd = sequenceEnd; }

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
    public static final long serialVersionUID = 1026;    

    // yidl.runtime.Object
    public int getTag() { return 1026; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::LSNRange"; }
    
    public int getXDRSize()
    {
        int my_size = 0;
        my_size += Integer.SIZE / 8; // viewId
        my_size += Long.SIZE / 8; // sequenceStart
        my_size += Long.SIZE / 8; // sequenceEnd
        return my_size;
    }    
    
    public void marshal( Marshaller marshaller )
    {
        marshaller.writeUint32( "viewId", viewId );
        marshaller.writeUint64( "sequenceStart", sequenceStart );
        marshaller.writeUint64( "sequenceEnd", sequenceEnd );
    }
    
    public void unmarshal( Unmarshaller unmarshaller ) 
    {
        viewId = unmarshaller.readUint32( "viewId" );
        sequenceStart = unmarshaller.readUint64( "sequenceStart" );
        sequenceEnd = unmarshaller.readUint64( "sequenceEnd" );    
    }
        
    

    private int viewId;
    private long sequenceStart;
    private long sequenceEnd;    

}

