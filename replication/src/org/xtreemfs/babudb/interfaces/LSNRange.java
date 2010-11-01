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




public class LSNRange implements Struct
{
    public static final int TAG = 1026;

    public LSNRange() { start = new LSN(); end = new LSN();  }
    public LSNRange( LSN start, LSN end ) { this.start = start; this.end = end; }

    public LSN getStart() { return start; }
    public LSN getEnd() { return end; }
    public void setStart( LSN start ) { this.start = start; }
    public void setEnd( LSN end ) { this.end = end; }

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
        my_size += start.getXDRSize(); // start
        my_size += end.getXDRSize(); // end
        return my_size;
    }

    public void marshal( Marshaller marshaller )
    {
        marshaller.writeStruct( "start", start );
        marshaller.writeStruct( "end", end );
    }

    public void unmarshal( Unmarshaller unmarshaller )
    {
        start = new LSN(); unmarshaller.readStruct( "start", start );
        end = new LSN(); unmarshaller.readStruct( "end", end );
    }

    private LSN start;
    private LSN end;
}
