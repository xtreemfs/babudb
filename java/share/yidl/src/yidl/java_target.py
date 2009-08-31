import sys, os.path

from target import *
from string_utils import *
from generator import *


__all__ = [
           "INDENT_SPACES",
           "JavaClass",
           "JavaBoolType",            
           "JavaBufferType",
           "JavaConstant", 
           "JavaEnumeratedType", "JavaEnumerator",
           "JavaExceptionType",
           "JavaInterface", 
           "JavaMapType", 
           "JavaModule", 
           "JavaNumericType", 
           "JavaOperation",
           "JavaSequenceType", 
           "JavaStringType", 
           "JavaStructType",
           "JavaTarget",
          ]


# Constants
INDENT_SPACES = " " * 4


class JavaClass:
    def getClassHeader( self ):
        package_name = self.getPackageName()        
        imports = pad( "\n\n", "\n".join( self.getImports() ), "\n\n\n" )
        name = self.getName()
        try: parent_type_names = self.getParentTypeNames()
        except AttributeError: parent_type_names = self.getParentInterfaceNames()
        assert isinstance( parent_type_names, tuple ) or isinstance( parent_type_names, list )
        if len( parent_type_names ) > 0:
            parents = []
            if parent_type_names[0] is not None:
                parents.append( "extends " + parent_type_names[0] )
            if len( parent_type_names ) >  1:
                parents.extend( ["implements " + parent_type_name for parent_type_name in parent_type_names[1:]] )
            parents = lpad( " ", ", ".join( parents ) )                
        else:
            parents = ""
                    
        return """\
package %(package_name)s;%(imports)s

public class %(name)s%(parents)s
{""" % locals()

    def getClassFooter( self ):
        return """
}
"""

    def getFilePath( self ): return os.path.join( self.getPackageDirPath(), self.getName() + ".java" )
    def getImports( self, exclude_package_names=None ):
        qname = self.getQualifiedName()
        assert isinstance( qname, list )
        if len( qname ) > 3:
            imports = []        
            for qname_part_i in xrange( 2, len( qname ) - 2 ):
                package_name = ".".join( qname[:qname_part_i+1] )                
                if exclude_package_names is None or not package_name in exclude_package_names:
                    imports.append( "import " + package_name + ".*;" )
            return imports
        else:
            return []
            
    def getPackageDirPath( self ): return os.sep.join( self.getQualifiedName()[:-1] )
    def getPackageName( self ): return ".".join( self.getQualifiedName()[:-1] )           


class JavaPackage:
    def __init__( self ):
        try: os.makedirs( self.getPackageDirPath() )
        except: pass
                    
    def getPackageDirPath( self ): return os.sep.join( self.getQualifiedName() )
    def getPackageName( self ): return ".".join( self.getQualifiedName() )           
            
        
class JavaType(JavaClass):  
    def getBoxedTypeName( self ): return self.getDeclarationTypeName()    
    def getConstantValue( self, value ): return value
    def getDeclarationTypeName( self ): return self.getName()
    def getDefaultInitializer( self, identifier ): name = self.getName(); return "%(identifier)s = new %(name)s();" % locals()        
    def getGetter( self, identifier ): return self.getDeclarationTypeName() + " get" + identifier[0].capitalize() + identifier[1:] + "() { return %(identifier)s; }" % locals()    
    def getSetter( self, identifier ): return "void set" + identifier[0].capitalize() + identifier[1:] + "( " + self.getDeclarationTypeName() + " %(identifier)s ) { this.%(identifier)s = %(identifier)s; }" % locals()
    def getToStringCall( self, identifier ): return "%(identifier)s.toString()" % locals()
                       

class JavaBoolType(BoolType, JavaType):
    def getBoxedTypeName( self ): return "Boolean"
    def getCastFromObject( self, source_identifier ): return "( Boolean )%(source_identifier)s" % locals()
    def getDeclarationTypeName( self ): return "boolean"
    def getDefaultInitializer( self, identifier ): return "%(identifier)s = false;" % locals()
    def getObjectDeserializeCall( self, source_identifier, dest_identifier ): return "%(dest_identifier)s = ( ( Boolean )%(source_identifier)s ).booleanValue();" % locals()
    def getObjectSerializeCall( self, identifier ): return "new Boolean( %(identifier)s )" % locals()
    def getToStringCall( self, identifier ): return "Boolean.toString( %(identifier)s )" % locals()    


class JavaBufferType(BufferType, JavaType):
    def getConstantValue( self, value ): return "\"%(value)s\"" % locals()
    def getDefaultInitializer( self, identifier ): return "%(identifier)s = null;" % locals()
    def getObjectDeserializeCall( self, source_identifier, dest_identifier ): declaration_type_name = self.getDeclarationTypeName(); return "%(dest_identifier)s = ( %(declaration_type_name)s )%(source_identifier)s;" % locals() 
    def getObjectSerializeCall( self, identifier ): return identifier        
    def getToStringCall( self, identifier ): return '"\\"" + %(identifier)s + "\\""' % locals()


class JavaConstant(Constant):
    def __repr__( self ):
        return "public static final " + self.getType().getDeclarationTypeName() + " " + \
            self.getIdentifier() + " = " + self.getType().getConstantValue( self.getValue() ) + ';'    


class JavaEnumeratedType(EnumeratedType, JavaType):
    def generate( self ):
        package_name = self.getPackageName()
        name = self.getName()
        enumerators = pad( "\n" + INDENT_SPACES, ( ",\n" + INDENT_SPACES ).join( [repr( enumerator ) for enumerator in self.getEnumerators()] ), ";" )
        writeGeneratedFile( self.getFilePath(), """\
package %(package_name)s;
        
        
public enum %(name)s 
{%(enumerators)s    

    private int __value; 
    
    %(name)s() { this.__value = 0; }
    %(name)s( int value ) { this.__value = value; }    
    public int intValue() { return __value; }
    
    public static %(name)s parseInt( int value )
    {
        %(name)s check_values[] = %(name)s.values();
        for ( int check_value_i = 0; check_value_i < check_values.length; check_value_i++ )
        {
            if ( check_values[check_value_i].intValue() == value )
                return check_values[check_value_i];            
        }
        return null;        
    }
}
""" % locals() )

    def getDefaultInitializer( self, identifier ):
        if len( self.getEnumerators() ) > 0:
            return identifier + " = " + self.getName() + "." + self.getEnumerators()[0].getIdentifier() + ";"
        else:
            return ""
                
    def getObjectDeserializeCall( self, source_identifier, dest_identifier ):
        return ""
#        name = self.getName() 
#        return "%(dest_identifier)s = %(name)s( %(source_identifier)s );" % locals()
    
    def getObjectSerializeCall( self, identifier ):
        return identifier


class JavaEnumerator(Enumerator):
    def __repr__( self ):
        if self.getValue() is not None:
            return self.getIdentifier() + "( " + str( self.getValue() ) + " )"
        else:
            return self.getIdentifier()
            

class JavaExceptionType(ExceptionType):
    def generate( self ):
        JavaStructType( self.getScope(), self.getQualifiedName(), self.getTag(), ( "Exception", ), self.getMembers() ).generate()


class JavaInclude(Include): pass


class JavaInterface(Interface, JavaPackage):
    def __init__( self, *args, **kwds ):
        Interface.__init__( self, *args, **kwds )
        JavaPackage.__init__( self )
            

class JavaMapType(MapType, JavaType):
    def generate( self ):
        class_header = self.getClassHeader()
        name = self.getName()
        key_boxed_type_name = self.getKeyType().getBoxedTypeName()
        key_toString_call = self.getKeyType().getToStringCall( "key" )
        value_boxed_type_name = self.getValueType().getBoxedTypeName()
        value_toString_call = self.getValueType().getToStringCall( "value" )
        serialize_methods = self.getSerializeMethods()
        deserialize_methods = self.getDeserializeMethods()
        other_methods = self.getOtherMethods()
        class_footer = self.getClassFooter()
        
        writeGeneratedFile( self.getFilePath(), """\
%(class_header)s
    public %(name)s()
    { }

    public %(name)s( Object from_hash_map )
    {
        this.deserialize( from_hash_map );
    }
        
    public String toString()
    {
        String to_string = new String();
        to_string += "{ ";
        for ( Iterator<%(key_boxed_type_name)s> key_i = keySet().iterator(); key_i.hasNext(); )
        {
             %(key_boxed_type_name)s key = key_i.next();
             %(value_boxed_type_name)s value = get( key );
             to_string += %(key_toString_call)s + ": " + %(value_toString_call)s + ", ";             
        }            
        to_string += "}";
        return to_string;        
    }    
    %(serialize_methods)s%(deserialize_methods)s%(other_methods)s
%(class_footer)s    
""" % locals() )

    def getCastFromObject( self, source_identifier ): name = self.getName(); return "new %(name)s( ( HashMap<String, Object> )%(source_identifier)s" % locals()
    
    def getDeserializeMethods( self ):
        key_boxed_type_name = self.getKeyType().getBoxedTypeName()
        value_boxed_type_name = self.getValueType().getBoxedTypeName()
        value_cast_from_object = self.getValueType().getCastFromObject( "from_hash_map_value" )
        
        return """
    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<%(key_boxed_type_name)s, Object> )from_hash_map );
    }
    
    public void deserialize( HashMap<%(key_boxed_type_name)s, %(value_boxed_type_name)s> from_hash_map )
    {    
        for ( Iterator<%(key_boxed_type_name)s> from_hash_map_key_i = from_hash_map.keySet().iterator(); from_hash_map_key_i.hasNext(); )
        {
            %(key_boxed_type_name)s from_hash_map_key = from_hash_map_key_i.next();
            Object from_hash_map_value = from_hash_map.get( from_hash_map_key );
            this.put( from_hash_map_key, %(value_cast_from_object)s );
        }    
    }        
""" % locals()
    
    def getImports( self, *args, **kwds ): return JavaType.getImports( self, *args, **kwds ) + ["import java.util.HashMap;", "import java.util.Iterator;"]
    def getObjectDeserializeCall( self, source_identifier, dest_identifier ): return "%(dest_identifier)s.deserialize( ( HashMap<String, Object> )%(source_identifier)s );" % locals()
    def getObjectSerializeCall( self, identifier ): return "%(identifier)s.serialize()" % locals()
    def getOtherMethods( self ): return ""

    def getSerializeMethods( self ):
        key_boxed_type_name = self.getKeyType().getBoxedTypeName()
        value_boxed_type_name = self.getValueType().getBoxedTypeName()
        value_object_serialize_call = self.getValueType().getObjectSerializeCall( "value" )
                
        return """
    public Object serialize()
    {
        HashMap<%(key_boxed_type_name)s, Object> to_hash_map = new HashMap<%(key_boxed_type_name)s, Object>();
        for ( Iterator<%(key_boxed_type_name)s> key_i = keySet().iterator(); key_i.hasNext(); )
        {
             %(key_boxed_type_name)s key = key_i.next();
             %(value_boxed_type_name)s value = get( key );
             to_hash_map.put( key, %(value_object_serialize_call)s );
        }
        return to_hash_map;
    }
""" % locals()
    
    def getParentTypeNames( self ):
        key_boxed_type_name = self.getKeyType().getBoxedTypeName()
        value_boxed_type_name = self.getValueType().getBoxedTypeName()         
        return ( "HashMap<%(key_boxed_type_name)s, %(value_boxed_type_name)s>" % locals(), )


class JavaModule(Module, JavaPackage):
    def __init__( self, *args, **kwds ):
        Module.__init__( self, *args, **kwds )
        JavaPackage.__init__( self )
            
    def generate( self ):
        Module.generate( self )

        if len( self.getConstants() ) > 0:       
            writeGeneratedFile( os.path.join( self.getPackageDirPath(), "Constants.java" ), """\
package %s;


public interface Constants
{
    %s
};
""" % ( self.getPackageName(), ( "\n" + INDENT_SPACES ).join( [repr( constant ) for constant in self.getConstants()] ) ) ) 
        
        for enumerated_type in self.getEnumeratedTypes():
            enumerated_type.generate()
            

class JavaNumericType(NumericType, JavaType):
    def getBoxedTypeName( self ):
        decl_type = self.getDeclarationTypeName()
        if decl_type == "int": return "Integer"
        else: return decl_type[0].upper() + decl_type[1:]

    def getCastFromObject( self, source_identifier ): 
        return "( " + self.getBoxedTypeName() + " )%(source_identifier)s"  % locals()
    
    def getDeclarationTypeName( self ):
        name = self.getName()
        if name == "float" or name == "double": return name
        elif name.endswith( "int8_t" ): return "int"
        elif name.endswith( "int16_t" ): return "int"
        elif name.endswith( "int32_t" ): return "int"
        elif name.endswith( "int64_t" ): return "long"
        else: return "long" # mode_t, etc.

    def getDefaultInitializer( self, identifier ): 
        return "%(identifier)s = 0;" % locals()
    
    def getObjectDeserializeCall( self, source_identifier, dest_identifier ): 
        decl_type_name = self.getDeclarationTypeName()
        if decl_type_name == "int":
            return "%(dest_identifier)s = ( %(source_identifier)s instanceof Integer ) ? ( ( Integer )%(source_identifier)s ).intValue() : ( ( Long )%(source_identifier)s ).intValue();" % locals()
        elif decl_type_name == "long":
            return "%(dest_identifier)s = ( %(source_identifier)s instanceof Integer ) ? ( ( Integer )%(source_identifier)s ).longValue() : ( ( Long )%(source_identifier)s ).longValue();" % locals()
        else:
            return "%(dest_identifier)s = ( ( " % locals() + self.getBoxedTypeName() + " )%(source_identifier)s )." % locals() + self.getDeclarationTypeName() + "Value();"
     
    def getObjectSerializeCall( self, identifier ): 
        return "new " + self.getBoxedTypeName() + "( %(identifier)s )" % locals()
        
    def getToStringCall( self, identifier ): 
        return self.getBoxedTypeName() + ".toString( %(identifier)s )" % locals()


class JavaSequenceType(SequenceType, JavaType):
    def generate( self ):
        class_header = self.getClassHeader()
        constructors = self.getConstructors()
        toString_method = lpad( "\n", self.getToStringMethod() )
        serialize_methods = lpad( "\n", self.getSerializeMethods() )
        deserialize_methods = lpad( "\n", self.getDeserializeMethods() )
        other_methods = lpad( "\n", self.getOtherMethods() )
        class_footer = self.getClassFooter()
        writeGeneratedFile( self.getFilePath(), """\
%(class_header)s
%(constructors)s%(toString_method)s%(serialize_methods)s%(deserialize_methods)s%(other_methods)s
%(class_footer)s
""" % locals() )
    
    def getCastFromObject( self, source_identifier ): type.name = self.getName(); return "new %(name)s( ( Object[] )%(source_identifier)s )" % locals()

    def getConstructors( self ):
        name = self.getName()        
        return """
    public %(name)s()
    { }

    public %(name)s( Object from_array )
    {
        this.deserialize( from_array );
    }

    public %(name)s( Object[] from_array )
    {
        this.deserialize( from_array );
    }        
""" % locals()
    
    def getDeserializeMethods( self ):
        value_cast_from_object = self.getValueType().getCastFromObject( "from_array[from_array_i]" )
        return """
    public void deserialize( Object from_array )
    {
        this.deserialize( ( Object[] )from_array );
    }
        
    public void deserialize( Object[] from_array )
    {
        for ( int from_array_i = 0; from_array_i < from_array.length; from_array_i++ )
            this.add( %(value_cast_from_object)s );
    }        
""" % locals()
        
    def getImports( self, *args, **kwds ): return JavaType.getImports( self, *args, **kwds ) + ["import java.util.ArrayList;", "import java.util.Iterator;"]
    def getObjectDeserializeCall( self, source_identifier, dest_identifier ): return "%(dest_identifier)s.deserialize( ( Object[] )%(source_identifier)s );" % locals()
    def getObjectSerializeCall( self, identifier ): return "%(identifier)s.serialize()" % locals()
    def getOtherMethods( self ): return ""
    def getParentTypeNames( self ): return ( "ArrayList<%s>" % self.getValueType().getBoxedTypeName(), )
    
    def getSerializeMethods( self ):
        value_boxed_type_name = self.getValueType().getBoxedTypeName()        
        value_object_serialize_call = self.getValueType().getObjectSerializeCall( "next_value" )
        return """    
    public Object serialize() 
    {
        Object[] to_array = new Object[size()];        
        for ( int value_i = 0; value_i < size(); value_i++ )
        {
            %(value_boxed_type_name)s next_value = get( value_i );                    
            to_array[value_i] = %(value_object_serialize_call)s;
        }
        return to_array;
    }
""" % locals()

    def getToStringMethod( self ):
        value_boxed_type_name = self.getValueType().getBoxedTypeName()
        value_toString_call = self.getValueType().getToStringCall( "i.next()" )                
        return """\
    public String toString()
    {
        String to_string = new String();
        for ( Iterator<%(value_boxed_type_name)s> i = iterator(); i.hasNext(); )
            to_string += %(value_toString_call)s + ", ";
        return to_string;
    }
""" % locals()
        
    
class JavaStringType(StringType, JavaType):
    def getCastFromObject( self, source_identifier ): return "( String )%(source_identifier)s" % locals()
    def getConstantValue( self, value ): return "\"%(value)s\"" % locals()
    def getDeclarationTypeName( self ): return "String"
    def getDefaultInitializer( self, identifier ): return "%(identifier)s = \"\";" % locals()
    def getObjectDeserializeCall( self, source_identifier, dest_identifier ): return "%(dest_identifier)s = ( String )%(source_identifier)s;" % locals()
    def getObjectSerializeCall( self, identifier ): return identifier
    def getToStringCall( self, identifier ): return '"\\"" + %(identifier)s + "\\""' % locals()


class JavaStructType(StructType, JavaType):
    def generate( self ):
        class_header = self.getClassHeader()
        class_footer = self.getClassFooter()
        qname = self.getQualifiedName( "::" )
        tag = self.getTag()
        constructors = self.getConstructors()        
        default_initializers = rpad( self.getDefaultInitializers(), " " )
        accessors = lpad( "\n\n", self.getAccessors() )
        toString_method = lpad( "\n\n", self.getToStringMethod() )
        deserialize_methods = lpad( "\n\n", self.getDeserializeMethods() )        
        serialize_methods = lpad( "\n", self.getSerializeMethods() )
        other_methods = lpad( "\n", self.getOtherMethods() )
        declarations = lpad( "\n\n", self.getDeclarations() )
        writeGeneratedFile( self.getFilePath(), """\
%(class_header)s
    public static final int TAG = %(tag)s;

    
%(constructors)s%(accessors)s%(toString_method)s
    // Serializable
    public int getTag() { return %(tag)s; }
    public String getTypeName() { return "%(qname)s"; }%(deserialize_methods)s%(serialize_methods)s%(other_methods)s%(declarations)s    
%(class_footer)s
""" % locals() )

    def getAccessors( self ):
        return "\n".join( [INDENT_SPACES + "public " + member.getType().getGetter( member.getIdentifier()) + "\n" +\
                           INDENT_SPACES + "public " + member.getType().getSetter( member.getIdentifier() )                        
                           for member in self.getMembers()] )        
        
    def getCastFromObject( self, source_identifier ):
        name = self.getName()
        return "new %(name)s( %(source_identifier)s )" % locals()

    def getConstructors( self ):
        name = self.getName()                
        default_initializers = self.getDefaultInitializers()
        constructors = ["public %(name)s() { %(default_initializers)s }" % locals()]
        if len( self.getMembers() ) > 0:
            constructors.append( "public " + self.getName() + "( " + \
                          ", ".join( [member.getType().getDeclarationTypeName() + " " + member.getIdentifier() for member in self.getMembers()] ) + \
                          " ) { " + " ".join( ["this." + member.getIdentifier() + " = " + member.getIdentifier() + ";" for member in self.getMembers()] ) + " }" )
        constructors.append( "public %(name)s( Object from_hash_map ) { %(default_initializers)s this.deserialize( from_hash_map ); }" % locals() )
        constructors.append( "public %(name)s( Object[] from_array ) { %(default_initializers)sthis.deserialize( from_array ); }" % locals() )
        return "\n".join( [INDENT_SPACES + constructor for constructor in constructors] )
        
    def getDeclarations( self ):
        return "\n".join( [INDENT_SPACES + "private " + member.getType().getDeclarationTypeName() + " " + member.getIdentifier() + ";" for member in self.getMembers()] )        

    def getDefaultInitializers( self ):
        return " ".join( [member.getType().getDefaultInitializer( member.getIdentifier() ) for member in self.getMembers()] )            
    
    def getDeserializeMethods( self ):
        member_hash_map_deserialize_calls = "\n".join( [INDENT_SPACES * 2 + member.getType().getObjectDeserializeCall( "from_hash_map.get( \"" + member.getIdentifier() + "\" )", "this." + member.getIdentifier() ) for member in self.getMembers()] )
        member_array_deserialize_calls = "\n".join( [INDENT_SPACES * 2 + self.getMembers()[member_i].getType().getObjectDeserializeCall( "from_array[%(member_i)u]" % locals(), "this." + self.getMembers()[member_i].getIdentifier() ) for member_i in xrange( len( self.getMembers() ) )] )        
        return """\
    public void deserialize( Object from_hash_map )
    {
        this.deserialize( ( HashMap<String, Object> )from_hash_map );
    }
        
    public void deserialize( HashMap<String, Object> from_hash_map )
    {
%(member_hash_map_deserialize_calls)s
    }
    
    public void deserialize( Object[] from_array )
    {
%(member_array_deserialize_calls)s        
    }
""" % locals()
    
    def getImports( self, *args, **kwds ): 
        return JavaType.getImports( self, *args, **kwds ) + ["import java.util.HashMap;"]

#    def getToStringMethod( self ):
#        return " + \", \" + ".join( [member.getType().getToStringCall( member.getIdentifier() ) for member in self.getMembers()] )
    
    def getObjectDeserializeCall( self, source_identifier, dest_identifier ): 
        return "%(dest_identifier)s.deserialize( %(source_identifier)s );" % locals()
    
    def getObjectSerializeCall( self, identifier ): 
        return "%(identifier)s.serialize()" % locals()

    def getOtherMethods( self ):
        return ""
    
    def getSerializeMethods( self ): 
        member_hash_map_serialize_calls = rpad( "\n".join( [INDENT_SPACES * 2 + "to_hash_map.put( \"" + member.getIdentifier() + "\", " + member.getType().getObjectSerializeCall( member.getIdentifier() ) + " );" for member in self.getMembers()] ), "\n" + INDENT_SPACES * 2 )            
        return """\
    public Object serialize()
    {
        HashMap<String, Object> to_hash_map = new HashMap<String, Object>();
%(member_hash_map_serialize_calls)sreturn to_hash_map;        
    }
""" % locals()
    
    def getToStringMethod( self ):
        name = self.getName()
        if len( self.getMembers() ) > 0:            
            member_toString_calls = pad( ' " + ', ' + ", " + '.join( [member.getType().getToStringCall( member.getIdentifier() ) for member in self.getMembers()] ), ' + " ' )
        else:
            member_toString_calls = ""
        return """\
    // Object
    public String toString()
    {
        return "%(name)s(%(member_toString_calls)s)";
    }
""" % locals()  
    
class JavaOperation(Operation): pass


class JavaTarget(Target): pass
