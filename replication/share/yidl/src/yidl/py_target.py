from yidl.target import *
from yidl.string_utils import *


__all__ = []


# Constants
INDENT_SPACES = "    "


class PyType:
    def getMarshalCall( self, var_name, qualified_var_name ): return "marshaller.write%s( \"%s\", %s )" % ( self.getMarshallerTypeName(), var_name, qualified_var_name )
    def getUnmarshalCall( self, var_name, qualified_var_name ): return qualified_var_name + " = unmarshaller.read%s( \"%s\" )" % ( self.getMarshallerTypeName(), var_name )

    
class PyPrimitiveType(PyType):
    def getConstantValue( self, value ): return value


class PyBoolType(BoolType, PyPrimitiveType):
    def getBoxedTypeName( self ): return "bool"
    def getCast( self, var_name ): return var_name
    def getConstantValue( self, value ): return ( ( "true" in value.lower() ) and "True" or "False" )
    def getDefaultValue( self ): return "False"


class PyCompoundType(PyType):
    def getCast( self, var_name ): name = self.getName(); return """( %(var_name)s is not None and ( isinstance( %(var_name)s, %(name)s ) and %(var_name)s or %(name)s( %(var_name)s ) ) or %(name)s() )""" % locals()
    def getDefaultValue( self ): return "None"


class PyConstant(Constant):
    def __repr__( self ):
        return self.getIdentifier() + " = " + self.getType().getConstantValue( self.getValue() )


class PyInclude(Include):
    def __repr__( self ):
        return ""
    
class PyInterface(Interface): 
    def __repr__( self ):                
        name = self.getName()
        operations = "\n".join( [INDENT_SPACES + repr( operation ) for operation in self.getOperations()] )
                    
        return """\
class %(name)s(object):
%(operations)s
""" % locals()
   

class PyMapType(MapType, PyCompoundType):
    def __repr__( self ):
        name = self.getName()
        return """
class %(name)s(dict): pass
""" % locals()
        
    
class PyNumericType(NumericType, PyPrimitiveType):    
    def getBoxedTypeName( self ):
        if self.getName() == "float" or self.getName() == "double": return "float"
        elif self.getName().endswith( "int64_t" ): return "long" 
        else: return "int"

    def getCast( self, var_name ):
        if self.getName() == "float" or self.getName() == "double": return "type( %(var_name)s ) == float and %(var_name)s or float( %(var_name)s )" % locals()
        else: return "( type( %(var_name)s ) == int or type( %(var_name)s ) == long ) and %(var_name)s or long( %(var_name)s )" % locals()

    def getDefaultValue( self ): return "0"
    
    def getMarshallerTypeName( self ):
        name = self.getName()
        if name.endswith( "_t" ): name = name[:-2]
        return "".join( ["".join( ( space_part[0].upper(), space_part[1:] ) ) for space_part in name.split( " " )] )


class PyOperation(Operation):
    def __repr__( self ):
        name = self.getName()        
        param_decls = ["self"]
        for param in self.getParameters():
            assert not param.isOutbound()
            param_decls.append( param.getIdentifier() )            
        param_decls = pad( " ", ", ".join( param_decls ), " " )
        return "def %(name)s(%(param_decls)s): raise NotImplementedError" % locals()

    
class PyPointerType(PointerType, PyPrimitiveType):
    def getBoxedTypeName( self ): return "buffer"
    def getCast( self, var_name ): return var_name
    def getDefaultValue( self ): return "\"\""
       
    
class PySequenceType(SequenceType, PyCompoundType):
    def __repr__( self ):
        name = self.getName()
        value_new = self.getValueType().getBoxedTypeName()
        value_cast = self.getValueType().getCast( "value" )
        value_write = self.getValueType().getMarshalCall( "( *this )[i]", "value" )
        value_read = self.getValueType().getUnmarshalCall( "( *this )[i]", "self[i]" )
        return """
class %(name)s(list):
    def __init__( self, init_list=[] ):
        list.__init__( self )
        self.extend( init_list )

    def resize( self, to_len ): list.extend( self, [%(value_new)s() for i in xrange( to_len )] )
    def extend( self, values ): list.extend( self, [%(value_cast)s for value in values] )
    def append( self, value ): list.append( self, %(value_cast)s )
    def __setvalue__( self, key, value ): list.__setvalue__( self, key, %(value_cast)s )

    def marshal( self, marshaller ):
        for value in self:
            %(value_write)s

    def unmarshal( self, marshaller ):
        for i in xrange( len( self ) ):
            %(value_read)s
        return self
             
""" % locals()


class PyStringType(StringType, PyPrimitiveType):
    def getBoxedTypeName( self ): return "str"
    def getCast( self, var_name ): return """isinstance( %(var_name)s, str ) and %(var_name)s or str( %(var_name)s )""" % locals()
    def getDefaultValue( self ): return "\"\""
    def getConstantValue( self, value ): return "\"" + value + "\""
    
    
class PyStructType(StructType, PyCompoundType):
    def __repr__( self ):
        name = self.getName()
        parent_type_names = ", ".join( self.getParentTypeNames() )
        other_methods = lpad( "\n\n", indent( self.getOtherMethods(), INDENT_SPACES ) )

        if len( self.getMembers() ) > 0:
            init_vars = ", ".join( [member.getIdentifier() + "=" + member.getType().getDefaultValue() for member in self.getMembers()] )
            init_statements =  ( "\n" + INDENT_SPACES * 2 ).join( ["self." + member.getIdentifier() + " = " + member.getType().getCast( member.getIdentifier() ) for member in self.getMembers()] )
            marshal_statements = ( "\n" + INDENT_SPACES * 2 ).join( [member.getIdentifier() + " = " + member.getType().getCast( "self." + member.getIdentifier() ) + "; " + member.getType().getMarshalCall( member.getIdentifier(), member.getIdentifier() ) for member in self.getMembers()] )
            unmarshal_statements = ( "\n" + INDENT_SPACES * 2 ).join( [member.getType().getUnmarshalCall( member.getIdentifier(), "self." + member.getIdentifier() ) for member in self.getMembers()] )
            return """\
class %(name)s(%(parent_type_names)s):
    def __init__( self, %(init_vars)s ):
        %(init_statements)s

    def marshal( self, marshaller ):
        %(marshal_statements)s

    def unmarshal( self, unmarshaller ):
        %(unmarshal_statements)s
        return self%(other_methods)s
            
""" % locals()
        else:
            return """\
class %(name)s(%(parent_type_names)s): pass     
""" % locals()

    def getOtherMethods( self ):
        return ""
    
    
class PyTarget(Target): pass
    
    
    