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




public class Chunk implements Struct
{
    public static final int TAG = 1025;

    public Chunk() {  }
    public Chunk( String fileName, long begin, long end ) { this.fileName = fileName; this.begin = begin; this.end = end; }

    public String getFileName() { return fileName; }
    public long getBegin() { return begin; }
    public long getEnd() { return end; }
    public void setFileName( String fileName ) { this.fileName = fileName; }
    public void setBegin( long begin ) { this.begin = begin; }
    public void setEnd( long end ) { this.end = end; }

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
    public static final long serialVersionUID = 1025;

    // yidl.runtime.Object
    public int getTag() { return 1025; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::Chunk"; }

    public int getXDRSize()
    {
        int my_size = 0;
        my_size += Integer.SIZE / 8 + ( fileName != null ? ( ( fileName.getBytes().length % 4 == 0 ) ? fileName.getBytes().length : ( fileName.getBytes().length + 4 - fileName.getBytes().length % 4 ) ) : 0 ); // fileName
        my_size += Long.SIZE / 8; // begin
        my_size += Long.SIZE / 8; // end
        return my_size;
    }

    public void marshal( Marshaller marshaller )
    {
        marshaller.writeString( "fileName", fileName );
        marshaller.writeUint64( "begin", begin );
        marshaller.writeUint64( "end", end );
    }

    public void unmarshal( Unmarshaller unmarshaller )
    {
        fileName = unmarshaller.readString( "fileName" );
        begin = unmarshaller.readUint64( "begin" );
        end = unmarshaller.readUint64( "end" );
    }

    private String fileName;
    private long begin;
    private long end;
}
