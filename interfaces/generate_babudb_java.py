#!/usr/bin/env python

import os.path, sys
sys.path.append("/home/flangner/share/yidl/src/py")

from yidl.compiler.idl_parser import parseIDL
from yidl.compiler.targets.java_target import *
from yidl.utilities import pad, writeGeneratedFile 


__all__ = []


# Constants
MY_DIR_PATH = os.path.dirname( os.path.abspath( sys.modules[__name__].__file__ ) )
BABUDB_COMMON_IMPORTS = [
						 "import java.io.StringWriter;",
                         "import org.xtreemfs.babudb.interfaces.utils.*;",
                         "import org.xtreemfs.include.common.buffer.ReusableBuffer;",
                         "import yidl.runtime.PrettyPrinter;",
                        ]

class BabuDBJavaBufferType(JavaBufferType):
    def getDeclarationTypeName( self ): return "ReusableBuffer"
    def getUnmarshalCall( self, decl_identifier, value_identifier ): return value_identifier + """ = ( ReusableBuffer )unmarshaller.readBuffer( %(decl_identifier)s );""" % locals()
    

class BabuDBJavaExceptionType(JavaExceptionType):
    def generate( self ): BabuDBJavaStructType( self.getScope(), self.getQualifiedName(), self.getTag(), ( "org.xtreemfs.babudb.interfaces.utils.ONCRPCException", ), self.getMembers() ).generate()
    def getExceptionFactory( self ): return ( INDENT_SPACES * 3 ) + "case %i: return new %s();\n" % ( self.getTag(), self.getName() )


class BabuDBJavaInterface(JavaInterface, JavaClass):    
    def generate( self ):                            
        class_header = self.getClassHeader()        
        constants = pad( "\n" + INDENT_SPACES, ( "\n" + INDENT_SPACES ).join( [repr( constant ) for constant in self.getConstants()] ), "\n\n" )        
        tag = self.getTag()            
        out = """\
%(class_header)s%(constants)s
    public static int getVersion() { return %(tag)s; }
""" % locals()

        exception_factories = "".join( [exception_type.getExceptionFactory() for exception_type in self.getExceptionTypes()] )
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
        
        request_factories = "".join( [operation.getRequestFactory() for operation in self.getOperations()] )
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

        response_factories = "".join( [operation.getResponseFactory() for operation in self.getOperations()] )
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

        out += self.getClassFooter()
                
        writeGeneratedFile( self.getFilePath(), out )            

        for operation in self.getOperations():
            operation.generate()
            
        for exception_type in self.getExceptionTypes():
            exception_type.generate()
            
    def getImports( self ): 
        return JavaClass.getImports( self ) + BABUDB_COMMON_IMPORTS

    def getPackageDirPath( self ):                
        return os.sep.join( self.getQualifiedName() )
    
    def getPackageName( self ): 
        return ".".join( self.getQualifiedName() )

class BabuDBJavaMapType(JavaMapType):
    def getImports( self ): 
        return JavaMapType.getImports( self ) + BABUDB_COMMON_IMPORTS

    def getOtherMethods( self ):
        return """
    // java.lang.Object
    public String toString() 
    { 
        StringWriter string_writer = new StringWriter();
        string_writer.append(this.getClass().getCanonicalName());
        string_writer.append(" ");
        PrettyPrinter pretty_printer = new PrettyPrinter( string_writer );
        pretty_printer.writeMap( "", this );
        return string_writer.toString();
    }
"""

class BabuDBFSJavaSequenceType(JavaSequenceType):
    def getImports( self ): 
        return JavaSequenceType.getImports( self ) + BABUDB_COMMON_IMPORTS
    
    def getOtherMethods( self ):
        return """
    // java.lang.Object
    public String toString() 
    { 
        StringWriter string_writer = new StringWriter();
        string_writer.append(this.getClass().getCanonicalName());
        string_writer.append(" ");
        PrettyPrinter pretty_printer = new PrettyPrinter( string_writer );
        pretty_printer.writeSequence( "", this );
        return string_writer.toString();
    }
"""

class BabuDBJavaStructType(JavaStructType):        
    def getImports( self ):
        return JavaStructType.getImports( self ) + BABUDB_COMMON_IMPORTS    

    def getOtherMethods( self ):
        return """
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
"""

class BabuDBJavaOperation(JavaOperation):        
    def generate( self ):
        self._getRequestType().generate()
        self._getResponseType( "returnValue" ).generate()
                
    def getRequestFactory( self ): return ( INDENT_SPACES * 3 ) + "case %i: return new %sRequest();\n" % ( self.getTag(), self.getName() )                    
    def getResponseFactory( self ): return not self.isOneway() and ( ( INDENT_SPACES * 3 ) + "case %i: return new %sResponse();" % ( self.getTag(), self.getName() ) ) or ""                

class BabuDBJavaRequestType(BabuDBJavaStructType):
    def getOtherMethods( self ):        
        response_type_name = self.getName()[:self.getName().index( "Request" )] + "Response"   
        return BabuDBJavaStructType.getOtherMethods( self ) + """
    // Request
    public Response createDefaultResponse() { return new %(response_type_name)s(); }
""" % locals()

    def getParentTypeNames( self ):
        return ( "org.xtreemfs.babudb.interfaces.utils.Request", )   

class BabuDBJavaResponseType(BabuDBJavaStructType):
    def getParentTypeNames( self ):
        return ( "org.xtreemfs.babudb.interfaces.utils.Response", )            


class BabuDBJavaTarget(JavaTarget): pass
                                
           
if __name__ == "__main__":
    os.chdir( os.path.join( MY_DIR_PATH, "..", "src" ) )        
        
    interfaces_dir_path = os.path.join(MY_DIR_PATH)  
    for interface_idl_file_name in os.listdir( interfaces_dir_path ):
        if interface_idl_file_name.endswith( ".idl" ):
            target = BabuDBJavaTarget()
            parseIDL( os.path.join( interfaces_dir_path, interface_idl_file_name ), target )
            target.generate()
