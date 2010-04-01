package org.xtreemfs.babudb.interfaces;

import java.io.StringWriter;
import org.xtreemfs.*;
import org.xtreemfs.babudb.*;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.oncrpc.utils.*;
import yidl.runtime.Marshaller;
import yidl.runtime.PrettyPrinter;
import yidl.runtime.Struct;
import yidl.runtime.Unmarshaller;




public class LSN implements Struct
{
    public static final int TAG = 1024;

    public LSN() {  }
    public LSN( int viewId, long sequenceNo ) { this.viewId = viewId; this.sequenceNo = sequenceNo; }

    public int getViewId() { return viewId; }
    public long getSequenceNo() { return sequenceNo; }
    public void setViewId( int viewId ) { this.viewId = viewId; }
    public void setSequenceNo( long sequenceNo ) { this.sequenceNo = sequenceNo; }

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
    public static final long serialVersionUID = 1024;

    // yidl.runtime.Object
    public int getTag() { return 1024; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::LSN"; }

    public int getXDRSize()
    {
        int my_size = 0;
        my_size += Integer.SIZE / 8; // viewId
        my_size += Long.SIZE / 8; // sequenceNo
        return my_size;
    }

    public void marshal( Marshaller marshaller )
    {
        marshaller.writeUint32( "viewId", viewId );
        marshaller.writeUint64( "sequenceNo", sequenceNo );
    }

    public void unmarshal( Unmarshaller unmarshaller )
    {
        viewId = unmarshaller.readUint32( "viewId" );
        sequenceNo = unmarshaller.readUint64( "sequenceNo" );
    }

    private int viewId;
    private long sequenceNo;
}
