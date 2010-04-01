package org.xtreemfs.babudb.interfaces.ReplicationInterface;

import java.io.StringWriter;
import org.xtreemfs.*;
import org.xtreemfs.babudb.*;
import org.xtreemfs.babudb.interfaces.*;
import org.xtreemfs.foundation.buffer.ReusableBuffer;
import org.xtreemfs.foundation.oncrpc.utils.*;
import yidl.runtime.Marshaller;
import yidl.runtime.PrettyPrinter;
import yidl.runtime.Struct;
import yidl.runtime.Unmarshaller;




public class chunkRequest extends org.xtreemfs.foundation.oncrpc.utils.Request
{
    public static final int TAG = 1013;

    public chunkRequest() { chunk = new Chunk();  }
    public chunkRequest( Chunk chunk ) { this.chunk = chunk; }

    public Chunk getChunk() { return chunk; }
    public void setChunk( Chunk chunk ) { this.chunk = chunk; }

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
    public Response createDefaultResponse() { return new chunkResponse(); }

    // java.io.Serializable
    public static final long serialVersionUID = 1013;

    // yidl.runtime.Object
    public int getTag() { return 1013; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::ReplicationInterface::chunkRequest"; }

    public int getXDRSize()
    {
        int my_size = 0;
        my_size += chunk.getXDRSize(); // chunk
        return my_size;
    }

    public void marshal( Marshaller marshaller )
    {
        marshaller.writeStruct( "chunk", chunk );
    }

    public void unmarshal( Unmarshaller unmarshaller )
    {
        chunk = new Chunk(); unmarshaller.readStruct( "chunk", chunk );
    }

    private Chunk chunk;
}
