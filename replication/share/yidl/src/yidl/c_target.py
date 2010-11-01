from random import randint

from target import *
from string_utils import *


__all__ = [
           "CBoolType", 
           "CBufferType", 
           "CConstant",
           "CEnumeratedType", "CEnumerator",
           "CExceptionType",
           "CInclude",
           "CInterface", 
           "CMapType", 
           "CModule", 
           "CNumericType", 
           "COperation", "COperationParameter",
           "CPointerType",
           "CReferenceType",
           "CSequenceType", 
           "CStringType", 
           "CStructType",
           "CTarget",
           "CVariadicType",
          ]


class CConstruct: pass


class CType(CConstruct ):
    def getReturnTypeName( self ): return self.getDeclarationTypeName()


class CBoolType(BoolType, CType): pass    
class CConstant(Constant, CConstruct): pass


class CEnumeratedType(EnumeratedType, CType): 
    def __repr__( self ):
        return "enum " + self.getName() + " {" + pad( " ", ", ".join( [repr( enumerator ) for enumerator in self.getEnumerators()] ), " " ) + "};"
     
class CEnumerator(Enumerator):
    def __repr__( self ):
        if self.getValue() is not None:
            return self.getIdentifier() + " = " + str( self.getValue() )
        else:
            return self.getIdentifier()


class CExceptionType(ExceptionType, CType): pass


class CInclude(Include, CConstruct):
    def __repr__( self ):
        file_path = self.getFilePath()
        if file_path.endswith( ".idl" ):
            if not "/" in file_path:
                file_path = file_path[:-3] + "h"
            else:
                return ""            
        return "#include " + ( self.isLocal() and '"' or '<' ) + file_path + ( self.isLocal() and '"' or '>' ) 


class CInterface(Interface, CConstruct): pass
class CMapType(MapType, CType): pass
class CModule(Module, CConstruct): pass


class CNumericType(NumericType, CType):
    def getDefaultValue( self ): return "0"
    def getDummyValue( self ): return "0"
    

class COperation(Operation, CConstruct): pass
class COperationParameter(OperationParameter, CConstruct): pass


class CPointerType(PointerType, CType):                
    def getDefaultValue( self ): return "NULL"
    def getDummyValue( self ): return "NULL"


class CReferenceType(ReferenceType, CType):
    def getDummyValue( self ): return "NULL"


class CSequenceType(SequenceType, CType): pass
class CBufferType(BufferType, CType): pass


class CStringType(StringType, CType):
    def getConstant( self, identifier, value ): return "const static char* %(identifier)s = \"%(value)s\";" % locals() 
    def getStaticConstantTypeName( self ): return "char*"


class CStructType(StructType, CType): pass 


class CTarget(Target, CConstruct):
    def __repr__( self ):
        # No guard here; that's added by scripts that know the output file path (like Yield's format_src)
        return rpad( "\n".join( [repr( include ) for include in self.getIncludes()] ), "\n\n\n" ) + "\n\n".join( [repr( module ) for module in self.getModules()] )


class CVariadicType(VariadicType, CType): pass
        