#!/usr/bin/env python

import sys,os
from os import chdir, listdir, sep as os_sep
from os.path import abspath, dirname, exists, join, splitext
from optparse import OptionParser

# Constants
MY_DIR_PATH = dirname( abspath( sys.modules[__name__].__file__ ) )

YIDL_DIR_PATH = abspath( join( MY_DIR_PATH, "..", "..", "yidl" ) )

try:
    import yidl
except ImportError:
    sys.path.append( join( YIDL_DIR_PATH, "src", "py" ) )

from yidl.compiler.idl_parser import parse_idl
from yidl.generators import generate_proj, generate_SConscript, generate_vcproj
from yidl.generators import generate_yield_cpp
from yidl.utilities import copy_file, format_src, indent, pad, write_file
from yidl.compiler.targets.java_target import *


__all__ = []

IMPORTS = [
	 "import java.io.StringWriter;",
         "import org.xtreemfs.foundation.oncrpc.utils.*;",
         "import org.xtreemfs.foundation.buffer.ReusableBuffer;",
         "import yidl.runtime.PrettyPrinter;",
          ]

class BabuDBJavaBufferType(JavaBufferType):
    def get_java_name( self ):
        return "ReusableBuffer"

    def get_unmarshal_call( self, decl_identifier, value_identifier ):
        return value_identifier + """ = ( ReusableBuffer )unmarshaller.readBuffer( %(decl_identifier)s );""" % locals()


class BabuDBJavaExceptionType(JavaExceptionType):
    def generate( self ):
        BabuDBJavaStructType( self.get_scope(), self.get_qname(), self.get_tag(), ( "org.xtreemfs.foundation.oncrpc.utils.ONCRPCException", ), self.get_members() ).generate()

    def get_factory( self ):
        return "case %i: return new %s();" % ( self.get_tag(), self.get_name() )


class BabuDBJavaInterface(JavaInterface, JavaClass):
    def generate( self ):
        class_header = self.get_class_header()
        constants = indent( INDENT_SPACES, pad( "\n", "\n".join( [repr( constant ) for constant in self.get_constants()] ), "\n\n" ) )
        prog = 0x20000000 + self.get_tag()
        version = self.get_tag()
        out = """\
%(class_header)s%(constants)s
    public static long getProg() { return %(prog)ul; }
    public static int getVersion() { return %(version)u; }
""" % locals()

        exception_factories = indent( INDENT_SPACES * 3, "\n".join( [exception_type.get_factory() for exception_type in self.get_exception_types()] ) )
        if len( exception_factories ) > 0:
            out += """
    public static ONCRPCException createException( int accept_stat ) throws Exception
    {
        switch( accept_stat )
        {
%(exception_factories)s
            default: throw new Exception( "unknown accept_stat " + Integer.toString( accept_stat ) );
        }
    }
""" % locals()

        request_factories = indent( INDENT_SPACES * 3, "\n".join( [operation.get_request_type().get_factory() for operation in self.get_operations()] ) )
        if len( request_factories ) > 0:
            out += """
    public static Request createRequest( ONCRPCRequestHeader header ) throws Exception
    {
        switch( header.getProcedure() )
        {
%(request_factories)s
            default: throw new Exception( "unknown request tag " + Integer.toString( header.getProcedure() ) );
        }
    }
""" % locals()

        response_factories = indent( INDENT_SPACES * 3, "\n".join( [operation.get_response_type().get_factory() for operation in self.get_operations() if not operation.is_oneway()] ) )
        if len( response_factories ) > 0:
            out += """
    public static Response createResponse( ONCRPCResponseHeader header ) throws Exception
    {
        switch( header.getXID() )
        {
%(response_factories)s
            default: throw new Exception( "unknown response XID " + Integer.toString( header.getXID() ) );
        }
    }
""" % locals()

        out += self.get_class_footer()

        write_file( self.get_file_path(), out )

        for operation in self.get_operations():
            operation.generate()

        for exception_type in self.get_exception_types():
            exception_type.generate()

    def get_imports( self ):
        return JavaClass.get_imports( self ) + IMPORTS

    def get_package_dir_path( self ):
        return os_sep.join( self.get_qname() )

    def get_package_name( self ):
        return ".".join( self.get_qname() )


class BabuDBJavaMapType(JavaMapType):
    def get_imports( self ):
        return JavaMapType.get_imports( self ) + IMPORTS

    def get_other_methods( self ):
        return """\
// java.lang.Object
public String toString()
{
    StringWriter string_writer = new StringWriter();
    string_writer.append(this.getClass().getCanonicalName());
    string_writer.append(" ");
    PrettyPrinter pretty_printer = new PrettyPrinter( string_writer );
    pretty_printer.writeMap( "", this );
    return string_writer.toString();
}"""


class BabuDBJavaSequenceType(JavaSequenceType):
    def get_imports( self ):
        return JavaSequenceType.get_imports( self ) + IMPORTS

    def get_other_methods( self ):
        return """\
// java.lang.Object
public String toString()
{
    StringWriter string_writer = new StringWriter();
    string_writer.append(this.getClass().getCanonicalName());
    string_writer.append(" ");
    PrettyPrinter pretty_printer = new PrettyPrinter( string_writer );
    pretty_printer.writeSequence( "", this );
    return string_writer.toString();
}"""


class BabuDBJavaStructType(JavaStructType):
    def get_imports( self ):
        return JavaStructType.get_imports( self ) + IMPORTS

    def get_other_methods( self ):
        return """\
// java.lang.Object
public String toString()
{
    StringWriter string_writer = new StringWriter();
    string_writer.append(this.getClass().getCanonicalName());
    string_writer.append(" ");
    PrettyPrinter pretty_printer = new PrettyPrinter( string_writer );
    pretty_printer.writeStruct( "", this );
    return string_writer.toString();
}"""

class BabuDBJavaOperation(JavaOperation):
    def generate( self ):
        self.get_request_type().generate()
        self.get_response_type().generate()

    def get_request_type( self ):
        try:
            return self.__request_type
        except AttributeError:
            request_type_name = self.get_name() + "Request"
            request_params = [] 
            for in_param in self.get_in_parameters():
                if not in_param in self.get_out_parameters():
                    request_params.append( in_param )
            self.__request_type = self._create_construct( "RequestType", BabuDBJavaRequestType, self.get_qname()[:-1] + [request_type_name], self.get_tag(), None, request_params )
            return self.__request_type        

    def get_response_type( self ):
        return self._get_response_type( "returnValue" )


class BabuDBJavaRequestType(BabuDBJavaStructType):
    def get_factory( self ):
        return "case %i: return new %s();" % ( self.get_tag(), self.get_name() )

    def get_other_methods( self ):
        response_type_name = self.get_name()[:self.get_name().index( "Request" )] + "Response"
        return BabuDBJavaStructType.get_other_methods( self ) + """

// Request
public Response createDefaultResponse() { return new %(response_type_name)s(); }""" % locals()

    def get_parent_names( self ):
        return ( "org.xtreemfs.foundation.oncrpc.utils.Request", )


class BabuDBJavaResponseType(BabuDBJavaStructType):
    def get_factory( self ):
        return "case %i: return new %s();" % ( self.get_tag(), self.get_name() )

    def get_parent_names( self ):
        return ( "org.xtreemfs.foundation.oncrpc.utils.Response", )


class BabuDBJavaTarget(JavaTarget): pass
                                
           
if __name__ == "__main__":
    os.chdir( os.path.join( MY_DIR_PATH, "..", "src" ) )        
        
    interfaces_dir_path = os.path.join(MY_DIR_PATH)  
    for interface_idl_file_name in os.listdir( interfaces_dir_path ):
        if interface_idl_file_name.endswith( ".idl" ):
            target = BabuDBJavaTarget()
            parse_idl( os.path.join( interfaces_dir_path, interface_idl_file_name ), target )
            target.generate()
