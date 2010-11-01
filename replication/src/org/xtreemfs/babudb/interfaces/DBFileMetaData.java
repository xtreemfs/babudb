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




public class DBFileMetaData implements Struct
{
    public static final int TAG = 1020;

    public DBFileMetaData() {  }
    public DBFileMetaData( String fileName, long fileSize, int maxChunkSize ) { this.fileName = fileName; this.fileSize = fileSize; this.maxChunkSize = maxChunkSize; }

    public String getFileName() { return fileName; }
    public long getFileSize() { return fileSize; }
    public int getMaxChunkSize() { return maxChunkSize; }
    public void setFileName( String fileName ) { this.fileName = fileName; }
    public void setFileSize( long fileSize ) { this.fileSize = fileSize; }
    public void setMaxChunkSize( int maxChunkSize ) { this.maxChunkSize = maxChunkSize; }

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
    public static final long serialVersionUID = 1020;

    // yidl.runtime.Object
    public int getTag() { return 1020; }
    public String getTypeName() { return "org::xtreemfs::babudb::interfaces::DBFileMetaData"; }

    public int getXDRSize()
    {
        int my_size = 0;
        my_size += Integer.SIZE / 8 + ( fileName != null ? ( ( fileName.getBytes().length % 4 == 0 ) ? fileName.getBytes().length : ( fileName.getBytes().length + 4 - fileName.getBytes().length % 4 ) ) : 0 ); // fileName
        my_size += Long.SIZE / 8; // fileSize
        my_size += Integer.SIZE / 8; // maxChunkSize
        return my_size;
    }

    public void marshal( Marshaller marshaller )
    {
        marshaller.writeString( "fileName", fileName );
        marshaller.writeUint64( "fileSize", fileSize );
        marshaller.writeUint32( "maxChunkSize", maxChunkSize );
    }

    public void unmarshal( Unmarshaller unmarshaller )
    {
        fileName = unmarshaller.readString( "fileName" );
        fileSize = unmarshaller.readUint64( "fileSize" );
        maxChunkSize = unmarshaller.readUint32( "maxChunkSize" );
    }

    private String fileName;
    private long fileSize;
    private int maxChunkSize;
}
