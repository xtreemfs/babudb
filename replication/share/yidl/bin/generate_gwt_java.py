#!/usr/bin/env python

import os.path
    
from yidl.generator import *
from yidl.java_target import *
from yidl.string_utils import *


__all__ = []
           

class GWTJavaInterface(JavaInterface, JavaClass): 
    def generate( self ):
        JavaInterface.generate( self )                               
        
        package_name = self.getPackageName()        
        imports = pad( "\n\n", "\n".join( self.getImports() ), "\n\n\n" )
        name = self.getName()
        operations = "\n".join( [INDENT_SPACES + repr( operation ) for operation in self.getOperations()] )
                    
        writeGeneratedFile( os.path.join( self.getPackageDirPath(), "..", name + ".java" ), """\
package %(package_name)s;%(imports)s

public interface %(name)s
{
%(operations)s
}
""" % locals() )


class GWTJavaOperation(JavaOperation):
    def __repr__( self ):
        return_type = self.getReturnType() is not None and self.getReturnType().getDeclarationTypeName() or "void"
        name = self.getName()        
        param_decls = []
        for param in self.getParameters():
            assert not param.isOutbound()
            param_decls.append( param.getType().getDeclarationTypeName() + " " + param.getIdentifier() )
        param_decls = pad( " ", ", ".join( param_decls ), " " )
        return "%(return_type)s %(name)s(%(param_decls)s);" % locals()
            

class GWTJavaStructType(JavaStructType):
    def getParentTypeNames( self ):
        return ( None, "java.io.Serializable" )
            
    
class GWTJavaTarget(JavaTarget): pass

                   
if __name__ == "__main__":     
    generator_main( GWTJavaTarget )
