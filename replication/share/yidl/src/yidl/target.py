import sys
from copy import copy
from inspect import isclass

from string_utils import *


__all__ = [
           "BoolType",
           "BufferType", 
           "Constant",                 
           "Declaration",                        
           "EnumeratedType", "Enumerator",
           "ExceptionType", "ExceptionTypeMember",
           "Include",
           "Interface", 
           "MapType",
           "Module", 
           "NumericType",
           "Operation", "OperationParameter",
           "PointerType",
           "ReferenceType",
           "SequenceType", 
           "StringType", 
           "StructType", "StructTypeMember",
           "Target",
           "VariadicType",
          ]


# Helper functions
def _getBaseModuleNames( class_ ):
    base_module_names = []
    for base in class_.__bases__:
        if base.__module__ != "__builtin__":
            base_module_names.append( base.__module__ )
            base_module_names.extend( _getBaseModuleNames( base ) )
    return base_module_names


class Construct:
    def __init__( self, scope, qname, tag=0 ):
        self.__scope = scope
        if isinstance( qname, list ): pass
        elif isinstance( qname, tuple ): qname = list( qname )
        elif isinstance( qname, basestring ): qname = [qname]
        else: raise TypeError, "qname must be sequence or basestring, not " + qname.__class__.__name__
        assert len( qname ) >= 1
        for qname_part in qname: assert isinstance( qname_part, basestring )
        assert isinstance( tag, int ) or isinstance( tag, long ), "expected numeric tag, got " + str( type( tag ) )
        self.__qname = qname        
        self.__tag = tag

    def __repr__( self ):
        return self.__class__.__name__ + " " + self.getName() + " # " + str( self.getTag() )

    def _addConstruct( self, construct, construct_name, construct_base_class, add_to_constructs, *args, **kwds ):
        if not isinstance( construct, construct_base_class ):
            construct = self._createConstruct( construct_name, construct_base_class, *( ( construct, ) + args ), **kwds )
        add_to_constructs.append( construct )
        return construct

    def _createConstruct( self, construct_name, construct_base_class, *args, **kwds ):
        search_module_names = ["__main__", self.__class__.__module__]            
        search_module_names.extend( _getBaseModuleNames( self.__class__ ) )
        scope = self.getScope()
        while scope is not None:
            search_module_names.append( scope.__class__.__module__ )                
            scope = scope.getScope()
        search_module_names = list( set( search_module_names ) )

        construct_class = None
        for module_name in search_module_names:
            module = sys.modules[module_name]                
            
            for attr in dir( module ):                    
                if isinstance( attr, basestring ) and \
                   attr.endswith( construct_name ) and \
                   ( construct_class is None or len( attr ) > len( construct_class.__name__ ) ):
                        test_construct_class = getattr( module, attr )
                        if isclass( test_construct_class ) and \
                           issubclass( test_construct_class, construct_base_class ):
                                construct_class = test_construct_class
                     
        if construct_class is not None:
            try: 
                return construct_class( self, *args, **kwds )
            except:
                print "Error creating", construct_name, "construct with class", construct_class.__name__
                raise
                                                                              
        raise RuntimeError, construct_name + " not defined"

    def generate( self ): pass
    def getIncludes( self ): return []
    def getName( self ): return self.__qname[-1]
        
    def getQualifiedName( self, scope_separator=None ): 
        if scope_separator is not None:
            return scope_separator.join( self.__qname )
        else:
            return self.__qname
    
    def getScope( self ): return self.__scope        
    def getTag( self ): return self.__tag    
   

class Scope(Construct):
    def __init__( self, scope, qname, local=False, tag=0 ):            
        Construct.__init__( self, scope, qname, tag )
        self.__local = local
        self.__constants = []

    def __repr__( self ):
        return rpad( "\n".join( [repr( constant ) for constant in self.getConstants()] ), "\n\n" )
                    
    def addConstant( self, constant, *args, **kwds ): return self._addConstruct( constant, "Constant", Constant, self.__constants, *args, **kwds )                
    def getConstants( self ): return self.__constants
    def isLocal( self ): return self.__local        


class Type(Construct):
    def __repr__( self ):  
        return self.getQualifiedName( "::" )
    
    def getMarshallerTypeName( self ):
        return "".join( ["".join( ( space_part[0].upper(), space_part[1:] ) ) for space_part in self.getName().split( " " )] )        


class BoolType(Type): pass


class BufferType(Type): pass


class Declaration(Construct):
    def __init__( self, scope, type, qname, tag=0, default_value=None ):        
        assert isinstance( type, Type )
        Construct.__init__( self, scope, qname, tag )
        self.__type = type
        self.__default_value = default_value

    def __repr__( self ):
        return repr( self.getType() ) + " " + self.getIdentifier()

    def getDefaultValue( self ): return self.__default_value
    def getIdentifier( self ): return self.getName()    
    def getType( self ): return self.__type    


class Constant(Declaration):
    def __init__( self, scope, type, qname, value ):
        Declaration.__init__( self, scope, type, qname )
        self.__value = value
        
    def __repr__( self ):
        return "const " + Declaration.__repr__( self )

    def getValue( self ): return self.__value


class EnumeratedType(Type):
    def __init__( self, *args, **kwds ):
        Type.__init__( self, *args, **kwds )
        self.__enumerators = []
    
    def __repr__( self ):
        return self.__class__.__name__ + " " + self.getName() + "\n{\n" + "\n".join( [" " + repr( enumerator ) + ";" for enumerator in self.getEnumerators()] ) + "\n};"
                
    def addEnumerator( self, enumerator, *args, **kwds ): return self._addConstruct( enumerator, "Enumerator", Construct, self.__enumerators, *args, **kwds )
    def getEnumerators( self ): return self.__enumerators

class Enumerator(Construct):
    def __init__( self, scope, qname, value=None ):
        Construct.__init__( self, scope, qname )
        self.__value = value

    def getIdentifier( self ): return self.getName()        
    def getValue( self ): return self.__value
        

class ExceptionType(Type):
    def __init__( self, *args, **kwds ):
        Type.__init__( self, *args, **kwds )
        self.__members = []
       
    def __repr__( self ):
        return self.__class__.__name__ + " " + self.getName() + "\n{\n" + "\n".join( [" " + repr( member ) + ";" for member in self.getMembers()] ) + "\n};"        
    
    def addMember( self, member, *args, **kwds ): return self._addConstruct( member, "ExceptionTypeMember", Declaration, self.__members, *args, **kwds )
    def getMembers( self ): return self.__members
    
class ExceptionTypeMember(Declaration): pass
    

class Include(Construct):
    def __init__( self, scope, file_path, local=True ):
        Construct.__init__( self, scope, [file_path] )        
        self.__local = local

    def __eq__( self, other ): 
        return self.getFilePath() == other.getFilePath() and self.isLocal() == other.isLocal()
    
    def __hash__( self ):
        return hash( self.__repr__() )
            
    def getFilePath( self ): return self.getName()
    def isLocal( self ): return self.__local
                    

class Interface(Scope):
    def __init__( self, scope, qname, local=False, tag=0, parent_interface_names=None ):
        assert parent_interface_names is None or isinstance( parent_interface_names, list )        
        Scope.__init__( self, scope, qname, local, tag )                
        self.__parent_interface_names = parent_interface_names is not None and parent_interface_names or []
        self.__exception_types = []
        self.__operations = []        

    def __repr__( self ):
        return Scope.__repr__( self ) + \
               rpad( "\n".join( [repr( exception_type ) for exception_type in self.getExceptionTypes()] ), "\n" ) + \
               "\n".join( [repr( operation ) for operation in self.getOperations()] )

    def generate( self ):
        for construct in self.getExceptionTypes() + self.getOperations(): 
            construct.generate()

    def addExceptionType( self, exception_type, *args, **kwds ): return self._addConstruct( exception_type, "ExceptionType", ExceptionType, self.__exception_types, *args, **kwds )         
    def addOperation( self, operation, *args, **kwds ): return self._addConstruct( operation, "Operation", Operation, self.__operations, *args, **kwds )
    
    def getExceptionTypes( self ): return self.__exception_types
    
    def getIncludes( self ):
        includes = []
        for construct in self.getExceptionTypes() + self.getOperations(): 
            includes.extend( construct.getIncludes() )
        return includes
    
    def getOperations( self ): return self.__operations
    def getParentInterfaceNames( self ): return self.__parent_interface_names            

           
class MapType(Type):
    def __init__( self, scope, qname, tag, key_type, value_type ):
        assert isinstance( key_type, Type )
        assert isinstance( value_type, Type )
        Type.__init__( self, scope, qname, tag )        
        self.__key_type = key_type
        self.__value_type = value_type        

    def __repr__( self ):
        return self.__class__.__name__ + " " + self.getName() + "<" + repr( self.getKeyType() ) + ", " + repr( self.getValueType() ) + ">;"

    def getKeyType( self ): return self.__key_type
    def getMarshallerTypeName( self ): return "Map"
    def getValueType( self ): return self.__value_type    

        
class Module(Scope):
    def __init__( self, scope, qname, local=False, tag=0 ):
        Scope.__init__( self, scope, qname, local, tag )
        self.__enumerated_types = []        
        self.__interfaces = []
        self.__modules = []
        self.__types = []        
                
    def __repr__( self ):
        return Scope.__repr__( self ) + \
               rpad( "\n".join( [repr( enumerated_type ) for enumerated_type in self.getEnumeratedTypes()] ), "\n\n" ) + \
               rpad( "\n\n".join( [repr( module ) for module in self.getModules()] ), "\n\n" ) + \
               rpad( "".join( [lpad( "\n", repr( type ) ) for type in self.getTypes() if not isinstance( type, StructType ) or type.getMembers() is not None] ), "\n\n" ) + \
               "\n\n".join( [repr( interface ) for interface in self.getInterfaces()] )

    def generate( self ):        
        for interface in self.getInterfaces(): 
            interface.generate()
            
        for module in self.getModules():
            module.generate()
            
        for type in self.getTypes():
            if not isinstance( type, StructType ) or type.getMembers() is not None:
                type.generate()

    def addEnumeratedType( self, type, *args, **kwds ): return self._addConstruct( type, "EnumeratedType", EnumeratedType, self.__enumerated_types, *args, **kwds )
    def addInterface( self, interface, *args, **kwds ): return self._addConstruct( interface, "Interface", Interface, self.__interfaces, *args, **kwds )
    def addMapType( self, type, *args, **kwds ): return self._addConstruct( type, "MapType", MapType, self.__types, *args, **kwds )
    def addModule( self, module, *args, **kwds ): return self._addConstruct( module, "Module", Module, self.__modules, *args, **kwds )
    def addSequenceType( self, type, *args, **kwds ): return self._addConstruct( type, "SequenceType", SequenceType, self.__types, *args, **kwds )
    def addStructType( self, type, *args, **kwds ): return self._addConstruct( type, "StructType", StructType, self.__types, *args, **kwds )
    
    def getEnumeratedTypes( self ): return self.__enumerated_types
    
    def getIncludes( self ):
        includes = []
        for construct in self.getInterfaces() + self.getModules() + self.getTypes(): 
            includes.extend( construct.getIncludes() )
        return includes
    
    def getInterfaces( self ): return self.__interfaces
    def getModules( self ): return self.__modules
    def getTypes( self ): return self.__types                   


class NumericType(Type):
    def getMarshallerTypeName( self ):
        name = self.getName()
        if name.endswith( "_t" ): name = name[:-2]
        return "".join( ["".join( ( space_part[0].upper(), space_part[1:] ) ) for space_part in name.split( " " )] )


class Operation(Construct):
    def __init__( self, scope, qname, oneway=False, return_type=None, const=False, tag=0 ):
        if oneway: assert return_type is None
        else: assert return_type is None or isinstance( return_type, Type )        
        assert isinstance( const, bool )
        Construct.__init__( self, scope, qname, tag )
        self.__oneway = oneway
        self.__return_type = return_type
        self.__const = const
        self.__parameters = []     

    def __repr__( self ):
        return self.__class__.__name__ + " " + ( self.isOneway() and "oneway " or "" ) + ( self.getReturnType() is None and "void" or repr( self.getReturnType() ) ) + " " + self.getName() + "( " + ", ".join( [repr( param ) for param in self.getParameters()] ) + " );"
    
    def addParameter( self, parameter, *args, **kwds ): return self._addConstruct( parameter, "OperationParameter", OperationParameter, self.__parameters, *args, **kwds )
    def getInboundParameters( self ): return [param for param in self.getParameters() if param.isInbound()]
    def getOutboundParameters( self, include_return_value=None ): return [param for param in self.getParameters() if param.isOutbound()]
    def getParameters( self ): return self.__parameters

    def _getRequestType( self, return_value_identifier="_return_value" ):
        try:        
            return self.__request_type
        except AttributeError:
            request_type_name = self.getName() + "Request"
            request_params = copy( self.getInboundParameters() )
            self.__request_type = self._createConstruct( "RequestType", Type, self.getQualifiedName()[:-1] + [request_type_name], self.getTag(), None, request_params )
            return self.__request_type
            
    def _getResponseType( self, return_value_identifier="_return_value" ):
        try:
            return self.__response_type
        except AttributeError:
            response_params = copy( self.getOutboundParameters() )
            if self.getReturnType() is not None:
                response_params.append( OperationParameter( in_=False, out_=True, scope=self.getScope(), type=self.getReturnType(), qname=self.getQualifiedName()+[return_value_identifier] ) )
            self.__response_type = self._createConstruct( "ResponseType", Type, self.getQualifiedName()[:-1] + [self.getQualifiedName()[-1] + "Response"], self.getTag(), None, response_params )
            return self.__response_type
        
    def getReturnType( self ): return self.__return_type                   
    def isConst( self ): return self.__const
    def isOneway( self ): return self.__oneway

class OperationParameter(Declaration):
    def __init__( self, scope, type, qname, in_=True, out_=False, tag=0, default_value=None ):
        assert in_ or out_        
        Declaration.__init__( self, scope, type, qname, tag, default_value )
        self.__in = in_
        self.__out = out_
        
    def __repr__( self ):
        inout = []
        if self.isInbound(): inout.append( "in" )
        if self.isOutbound: inout.append( "out" )
        return "".join( inout ) + " " + Declaration.__repr__( self )

    def isInbound( self ): return self.__in
    def isOutbound( self ): return self.__out
        
        
class PointerType(Type):
    def getMarshallerTypeName(): return "Pointer"    


class ReferenceType(PointerType): pass


class SequenceType(Type):
    def __init__( self, scope, qname, tag, value_type ):
        Type.__init__( self, scope, qname, tag )
        self.__value_type = value_type

    def __repr__( self ):
        return self.__class__.__name__ + " " + self.getName() + "<" + repr( self.getValueType() ) + ">;"        

    def getMarshallerTypeName( self ): return "Sequence"
    def getValueType( self ): return self.__value_type


class StringType(Type):
    def getMarshallerTypeName( self ): return "String"    

        
class StructType(Type):
    def __init__( self, scope, qname, tag=0, parent_type_names=None, members=[] ):
        Type.__init__( self, scope, qname, tag )
        self.__parent_type_names = parent_type_names is not None and parent_type_names or []
        if members is None: # A forward declaration or an undefined type
            self.__members = None
        else:
            self.__members = []            
            if len( members ) > 0:      
                for member in members:
                    assert isinstance( member, Declaration )
                    self.__members.append( member )

    def __repr__( self ):
        return self.__class__.__name__ + " " + self.getName() + "\n{\n" + "\n".join( [" " + repr( member ) + ";" for member in self.getMembers()] ) + "\n};"        

    def addMember( self, member, *args, **kwds ): return self._addConstruct( member, "StructTypeMember", Declaration, self.__members, *args, **kwds )        
    def getMembers( self ): return self.__members
    def getMarshallerTypeName( self ): return "Struct"
    def getParentTypeNames( self ): return self.__parent_type_names
                    
class StructTypeMember(Declaration): pass


class Target(Scope):
    def __init__( self ):
        Construct.__init__( self, None, [self.__class__.__name__] )
        self.__builtin_types = []
        self.__includes = []
        self.__modules = []

    def __repr__( self ):
        return rpad( "\n".join( [repr( include ) for include in self.getIncludes()] ), "\n\n" ) + \
               "\n".join( [repr( module ) for module in self.getModules()] )

    def generate( self ):
        for module in self.getModules(): module.generate() 
            
    def addBuiltinType( self, builtin_type, *args, **kwds ):
        if isinstance( builtin_type, Type ):
            self.__builtin_types.append( builtin_type )
            return builtin_type
        else:
            assert isinstance( builtin_type, list ) and len( builtin_type ) >= 1
            assert len( args ) == 0
            assert len( kwds ) == 0
            name = builtin_type[-1]
            if name == "string": return self._addConstruct( builtin_type, "StringType", StringType, self.__builtin_types, *args, **kwds )
            elif name == "bool": return self._addConstruct( builtin_type, "BoolType", BoolType, self.__builtin_types, *args, **kwds )
            elif name == "buffer": return self._addConstruct( builtin_type, "BufferType", BufferType, self.__builtin_types, *args, **kwds )
            elif name == "any" or name == "ptr": return self._addConstruct( builtin_type, "PointerType", PointerType, self.__builtin_types, *args, **kwds )
            elif name.endswith( "*" ): return self._addConstruct( builtin_type, "PointerType", PointerType, self.__builtin_types, *args, **kwds )
            elif name.endswith( "&" ): return self._addConstruct( builtin_type, "ReferenceType", ReferenceType, self.__builtin_types, *args, **kwds )
            else: return self._addConstruct( builtin_type, "NumericType", NumericType, self.__builtin_types, *args, **kwds )         
        
    def addInclude( self, include, *args, **kwds  ): return self._addConstruct( include, "Include", Include, self.__includes, *args, **kwds )
    def addModule( self, module, *args, **kwds ): return self._addConstruct( module, "Module", Module, self.__modules, *args, **kwds )
    
    def getIncludes( self ): 
        includes = copy( self.__includes )
        for construct in self.__builtin_types + self.getModules():
            includes.extend( construct.getIncludes() )
        local_includes_dict = dict( ( include.getFilePath(), include ) for include in includes if include.isLocal() )
        nonlocal_includes_dict = dict( ( include.getFilePath(), include ) for include in includes if not include.isLocal() )
        local_includes_dict_keys = local_includes_dict.keys()
        local_includes_dict_keys.sort()
        nonlocal_includes_dict_keys = nonlocal_includes_dict.keys()
        nonlocal_includes_dict_keys.sort()        
        includes = []
        for includes_dict_key in local_includes_dict_keys:
            includes.append( local_includes_dict[includes_dict_key] )            
        for includes_dict_key in nonlocal_includes_dict_keys:
            includes.append( nonlocal_includes_dict[includes_dict_key] )                        
        return includes
    
    def getModules( self ): return self.__modules


class VariadicType(Type): pass
