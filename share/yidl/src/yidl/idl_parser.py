# Copyright 2003-2009 Minor Gordon, with original implementations and ideas contributed by Felix Hupfeld.
# This source comes from the Yield project. It is licensed under the GPLv2 (see COPYING for terms and conditions).

import sys, os.path, traceback
from copy import copy

from pyparsing import Literal, CaselessLiteral, Keyword, Word, ZeroOrMore, OneOrMore, \
        Forward, Group, Optional, Combine, \
        alphas, nums, alphanums, printables, QuotedString, restOfLine, cStyleComment

from target import Target 

        
__all__ = ["parseIDL"]


# Constants
NUMERIC_TYPE_NAMES = ( 
                      "char", "unsigned char", "octet", "int8_t", "uint8_t",
                      "short", "unsigned short", "int16_t", "uint16_t",
                      "int", "unsigned int", "int32_t", "uint32_t",
                      "long", "unsigned long",
                      "long long", "unsigned long long", "int64_t", "uint64_t",
                      "float", "double",
                      "off_t", "off64_t", "size_t", "ssize_t", "mode_t",
                      "YIELD::fd_t", "YIELD::socket_t", "YIELD::timeout_ns_t"
                     )

PRIMITIVE_TYPE_NAMES = NUMERIC_TYPE_NAMES + ( "bool", "boolean", "buffer", "string", "wchar", "wstring", "any", "ptr", "void*" )

IDL_INT_TYPE_TO_STDINT_TYPE_MAP =\
{ 
  "char" : "int8_t", "signed char": "int8_t", "unsigned char": "uint8_t", "octet": "uint8_t",
  "short" : "int16_t", "signed short": "int16_t", "unsigned short": "uint16_t",
  "int" : "int32_t", "signed int": "int32_t", "unsigned int": "uint32_t",  
  "long" : "int64_t", "signed long": "int64_t", "unsigned long": "uint64_t",
  "long long": "int64_t", "signed long long": "int64_t", "unsigned long long": "uint64_t"
}
            
            
# Helper functions
def _flattenTokens( tokens ):
    flattened_tokens = []
    for token in tokens:
        if hasattr( token, "asList" ):
            flattened_tokens.extend( _flattenTokens( token.asList() ) )
        elif isinstance( token, list ):
            flattened_tokens.extend( _flattenTokens( token ) )
        else:
            flattened_tokens.append( token )
    return flattened_tokens

def _mergeDicts( a, b ):
    if len( a ) > 0 or len( b ) > 0:    
        a_copy = a.copy()    
        b_copy = b.copy()
        for a_key, a_value in a_copy.iteritems():
            try: 
                b_value = b_copy[a_key]
                del b_copy[a_key]
            except KeyError:
                continue
        
            assert a_value.__class__ == b_value.__class__                
            if isinstance( b_value, list ):                        
                a_value.extend( b_value )
            elif isinstance( b_value, dict ):
                _mergeDicts( a_value, b_value )

        a_copy.update( b_copy )
        return a_copy
    else:
        return a

        
def parseIDL( idl, target, use_stdint_types=True ):
    if isinstance( idl, basestring ):
        assert isinstance( target, Target )
        
        try:
            idl_file_path = idl
            idl_str = open( idl_file_path ).read()
        except:
            idl_file_path = None
            idl_str = idl
    
        try:
            return _ParsedIDL( idl_file_path, idl_str, target, use_stdint_types )
        except:
            if idl_file_path is not None:
                print "Error parsing", idl_file_path
            raise
    else:
        return idl


class _ParsedIDL:
    def __init__( self, idl_file_path, idl_str, target, use_stdint_types=True ):        
        self.__target = target
        self.__use_stdint_types = use_stdint_types
                
        try:
            grammar = self.__class__.__grammar
        except AttributeError:
            type_name = Word( alphas+"_", alphanums+"_:" )
            for primitive_type_name in PRIMITIVE_TYPE_NAMES: type_name ^= primitive_type_name
            type_name ^= Literal( "..." )
            type_name += Optional( OneOrMore( Literal( "*" ) ) | Literal( "&" ) )            
            identifier = Word( alphas+"_", alphanums+"_" )
            real = Combine( Word( nums+"+-", nums ) + '.' + Optional( Word( nums ) ) + Optional( CaselessLiteral( "E" ) + Word( nums+"+-", nums ) ) )
            integer = Combine( CaselessLiteral( "0x" ) + Word( nums+"abcdefABCDEF" ) ) | Word( nums+"+-", nums )
            value = real | integer | QuotedString( "\"", "\\" ) | QuotedString( "'", "\\" )
            tag = "#" + integer
            parent_type_names = Optional( ':' + type_name + ZeroOrMore( ',' + type_name ) )
            decl = Optional( Keyword( "const" ) ) + Optional( Keyword( "struct" ) ) + type_name + identifier
            exception = Keyword( "exception" ) + identifier + tag + '{' + ZeroOrMore( decl + ';' ) +  Literal( '}' ) + ';'            
            const = Keyword( "const" ) + type_name + identifier + '=' + value + ';'
            struct = Keyword( "struct" ) + identifier + tag + parent_type_names + '{' + ZeroOrMore( decl + ';' ) + Literal( '}' ) + ';'
            sequence = Keyword( "sequence" ) + '<' + type_name + '>'
            map = Keyword( "map" ) + '<' + type_name + ',' + type_name + '>'
            typedef = Keyword( "typedef" ) + ( sequence | map ) + identifier + tag + ';'
            enum = Keyword( "enum" ) + identifier + '{' + identifier + Optional( '=' + value ) + ZeroOrMore( ',' + identifier + Optional( '=' + value ) ) + Literal( '}' ) + ';'
            operation_parameter = Optional( Keyword( "in" ) | Keyword( "out" ) | Keyword( "inout" ) ) + decl
            operation = Optional( Keyword( "oneway" ) ) + ( Keyword( "void" ) | type_name ) + identifier + '(' + Optional( operation_parameter + ZeroOrMore( ',' + operation_parameter ) ) + Literal( ')' ) + Optional( Keyword( "const" ) ) + tag + ';'
            interface = Optional( Keyword( "local" ) ) + Keyword( "interface" ) + identifier + tag + parent_type_names + '{' + ZeroOrMore( operation | exception | const ) + Literal( '}' ) + Optional( Literal( ';' ) )
            module = Forward()
            module << ( Optional( Keyword( "local" ) ) + Keyword( "module" ) + identifier + tag + '{' + ZeroOrMore( const | enum | struct | typedef | interface | module ) + Literal( '}' ) + Optional( Literal( ';' ) ) )
            include_file_path = Word( alphanums+"_-./" )
            include = Keyword( "#include" ) + ( Group( '<' + include_file_path + '>' ) | Group( '"' + include_file_path + '"' ) )
            
            grammar = ZeroOrMore( include ) + OneOrMore( module )
            grammar.ignore( "//" + restOfLine )
            grammar.ignore( cStyleComment )
            self.__class__.__grammar = grammar

        self.__tokens = _flattenTokens( grammar.parseString( idl_str ) )
        if len( self.__tokens ) == 0: raise ValueError

        self.__token_i = 0
        
        if idl_file_path is not None:
            idl_dir_path = os.path.dirname( os.path.abspath( idl_file_path ) )
        else:
            idl_dir_path = os.path.dirname( os.path.abspath( sys.modules[__name__].__file__ ) )
            
        self.__type_map = {}
        while self.__token_i < len( self.__tokens ) and self.__tokens[self.__token_i] == "#include":
            include = self.__parseInclude()
            if include.getFilePath().endswith( ".idl" ):
                include_idl_file_path = os.path.abspath( os.path.join( idl_dir_path, include.getFilePath().replace( '/', os.sep ) ) )
                include_parsed_idl = parseIDL( include_idl_file_path, Target() )
                self.__type_map = _mergeDicts( include_parsed_idl.getTypeMap(), self.__type_map )
        if len( self.__type_map ) == 0:
            self.__type_map = { None: {} } # { module_name: { child_type_name: type }
                               
        while self.__token_i < len( self.__tokens ) and ( self.__tokens[self.__token_i] == "module" or self.__tokens[self.__token_i] == "local" ):
            self.__parseModule()

    def __assertNextToken( self, expected_contents ):
        next_token = self.__getNextToken()
        if next_token == expected_contents:
            return
        else:
            context_tokens = []
            if self.__token_i >= 5: context_tokens.extend( self.__tokens[self.__token_i-5:self.__token_i] )
            else: context_tokens.extend( self.__tokens[0:self.__token_i] )
            if self.__token_i+5 < len( self.__tokens ): context_tokens.extend( self.__tokens[self.__token_i:self.__token_i+5] )
            else: context_tokens.extend( self.__tokens[self.__token_i:] )
            context_tokens = " ".join( context_tokens )
                        
        assert next_token == expected_contents, "expected token " + str( expected_contents ) + " but got " + str( next_token ) + ", surrounding tokens = '" + context_tokens + "'"
    
    def __getNextToken( self ):
        next_token = self.__tokens[self.__token_i]            
        self.__token_i += 1
        return next_token

    def __getType( self, current_module, name ):                
        try:
            
            return self.__getTypeMapForScope( current_module )[name]
        except KeyError:
            pass

        builtin_type = False
        if name == "bool" or name == "boolean":
            name = "bool"
            builtin_type = True
        elif name == "string" or name == "wstring":
            name = "string"    
            builtin_type = True         
        elif name in NUMERIC_TYPE_NAMES:
            if self.__use_stdint_types:
                try: name = IDL_INT_TYPE_TO_STDINT_TYPE_MAP[name]
                except KeyError: pass
            builtin_type = True
        elif name == "buffer": builtin_type = True             
        elif name == "any" or name == "ptr": builtin_type = True
        elif name.endswith( "*" ): builtin_type = True
        elif name.endswith( "&" ): builtin_type = True
            
        if "::" in name: qname = name.split( "::" )
        elif "." in name: qname = name.split( "." )
        else: qname = [name]

        if builtin_type:
            try: 
                return self.__type_map[None][name]
            except KeyError:
                self.__type_map[None][name] = type = self.__target.addBuiltinType( qname )
                return type         
        else: # Assume it's a struct
            print __name__ + ": warning: assuming", name, "in", current_module.getQualifiedName( "::" ), "is a forward-declared struct"
            self.__getTypeMapForScope( current_module )[name] = type = current_module.addStructType( qname, 0, None, None )
            return type

    def getTypeMap( self ):
        return self.__type_map
        
    def __getTypeMapForScope( self, scope ):
        type_map_for_scope = self.__type_map        
        for qname_part in scope.getQualifiedName():
            try:
                type_map_for_scope = type_map_for_scope[qname_part]
            except KeyError:
                type_map_for_scope[qname_part] = type_map_for_scope = {}
        return type_map_for_scope
                                
    # Methods to parse objects out of tokens
    def __parseConstant( self, current_module, current_scope ):
        self.__assertNextToken( "const" )
        type, identifier = self.__parseDeclaration( current_module )
        qname = current_scope.getQualifiedName() + [identifier]
        self.__assertNextToken( '=' )
        value = self.__getNextToken()
        self.__assertNextToken( ';' )
        return current_scope.addConstant( type, qname, value )

    def __parseDeclaration( self, current_module ):
        token_i_start = self.__token_i
        token = self.__tokens[self.__token_i]                            
        while token not in ( ',', ';', ')', '=' ):
            self.__token_i += 1
            token = self.__tokens[self.__token_i]
            
        decl_tokens = self.__tokens[token_i_start:self.__token_i]
        
        identifier = decl_tokens[-1]
        decl_tokens.pop( -1 )

        try:
            first_star = decl_tokens.index( '*' )
            decl_tokens = decl_tokens[:first_star] + ["".join( decl_tokens[first_star:] )]
        except ValueError:
            pass
                    
        type_name = " ".join( decl_tokens )
        type = self.__getType( current_module, type_name )

        return type, identifier

    def __parseEnumeratedType( self, current_module ):
        self.__assertNextToken( "enum" )
        name = self.__getNextToken()
        qname = current_module.getQualifiedName() + [name]
        self.__assertNextToken( '{' )
        enum_type = current_module.addEnumeratedType( qname )
        while self.__tokens[self.__token_i] != '}':
            identifier = self.__getNextToken()
            if self.__tokens[self.__token_i]  == '=':
                self.__assertNextToken( '=' )
                value = self.__getNextToken()
            else:
                value = None
            enum_type.addEnumerator( *( identifier, value ) )
            if self.__tokens[self.__token_i]!= '}':
                self.__assertNextToken( ',' )                
        self.__assertNextToken( '}' )
        self.__assertNextToken( ';' )
        self.__getTypeMapForScope( current_module )[name] = enum_type
        return enum_type
                    
    def __parseExceptionType( self, current_module, current_interface ):
        self.__assertNextToken( "exception" )        
        name = self.__getNextToken()
        qname = current_interface.getQualifiedName() + [name]
        tag = current_interface.getTag() + self.__parseTag()
        self.__assertNextToken( '{' )        
        exception_type = current_interface.addExceptionType( qname, tag )
        while self.__tokens[self.__token_i] != "}":         
            exception_type.addMember( *self.__parseDeclaration( current_module ) )   
            self.__assertNextToken( ';' )
        self.__assertNextToken( '}' )
        self.__assertNextToken( ';' )
        self.__getTypeMapForScope( current_interface )[name] = exception_type
        return exception_type
        
    def __parseInclude( self ):
        self.__assertNextToken( "#include" )
        local = self.__getNextToken()
        file_path = self.__getNextToken()
        self.__assertNextToken( local )
        return self.__target.addInclude( file_path, local=='"' )

    def __parseInterface( self, current_module ):
        if self.__tokens[self.__token_i] == "local":
            local = True
            self.__token_i += 1
        else:
            local = False

        self.__assertNextToken( "interface" )

        name = self.__getNextToken()
        qname = current_module.getQualifiedName() + [name]
        tag = current_module.getTag() + self.__parseTag()
 
        parent_interface_type_names = self.__parseParentTypeNames()                    
        
        self.__assertNextToken( '{' )
        
        interface = current_module.addInterface( qname, local, tag, parent_interface_type_names )
                
        while self.__tokens[self.__token_i] != '}':
            if self.__tokens[self.__token_i] == "const":
                self.__parseConstant( current_module, interface )
            elif self.__tokens[self.__token_i] == "exception":
                self.__parseExceptionType( current_module, interface )
            else:
                self.__parseOperation( current_module, interface )

        self.__assertNextToken( '}' )
        self.__assertNextToken( ';' )

        return interface
            
    def __parseModule( self, current_module=None ):
        if self.__tokens[self.__token_i] == "local":
            local = True
            self.__token_i += 1
        else:
            local = False

        self.__assertNextToken( "module" )
    
        name = self.__getNextToken()
        qname = current_module is not None and ( current_module.getQualifiedName() + [name] ) or [name]
                        
        tag = self.__parseTag()
                        
        if current_module is not None:
            tag += current_module.getTag()
            module = current_module.addModule( qname, local, tag ) 
        else:
            module = self.__target.addModule( qname, local, tag )

        self.__assertNextToken( '{' )

        token = self.__tokens[self.__token_i]
        while token != '}': # Within a module
            if token == "const":
                self.__parseConstant( module, module )
            elif token == "enum":
                self.__parseEnumeratedType( module )
            elif token == "local" or token == "interface":
                self.__parseInterface( module )
            elif token == "module":
                self.__parseModule( module )
            elif token == "struct" or token == "typedef":
                if token == "struct": self.__parseStructType( module )
                else: self.__parseTypedef( module )
            else:
                raise ValueError, "unrecognized token " + token + " at index " + str( self.__token_i )

            token = self.__tokens[self.__token_i]

        self.__assertNextToken( '}' )
        self.__assertNextToken( ';' )

        return module

    def __parseOperation( self, current_module, current_interface ):
        operation_start_tokens = []
        while self.__tokens[self.__token_i] != '(':
            operation_start_tokens.append( self.__tokens[self.__token_i] )
            self.__token_i += 1
        self.__assertNextToken( '(' )
            
        try:
            operation_start_tokens.remove( "oneway" )
            oneway = True
        except ValueError:
            oneway = False

        name = operation_start_tokens.pop( -1 )        
        qname = current_interface.getQualifiedName() + [name]
                
        return_type_name = " ".join( operation_start_tokens )

        if return_type_name == "void":
            return_type = None
        else:
            return_type = self.__getType( current_module, return_type_name )

        params = [] # Have to storeo tuples instead of creating the operation and then add()ing because the operation Tag comes after the parameters
        while self.__tokens[self.__token_i] != ')':
            params.append( self.__parseOperationParameter( current_module, oneway ) )
        self.__assertNextToken( ')' )
        
        if self.__tokens[self.__token_i] == "const":
            const = True
            self.__getNextToken()
        else:
            const = False

        tag = current_interface.getTag() + self.__parseTag()
             
        self.__assertNextToken( ';' )

        operation = current_interface.addOperation( qname, oneway, return_type, const, tag )
        for param in params:
            inout, type, identifier = param
            operation.addParameter( type, qname + [identifier], inout != "out", inout.endswith( "out" ), 0 )                        
        return operation

    def __parseOperationParameter( self, current_module, oneway ):
        inout = self.__tokens[self.__token_i]
        if inout == "out" or inout == "inout":                
            self.__token_i += 1
            if oneway:
                raise TypeError, "no out parameters allowed on oneway operations (operation name: %(name)s)" % locals()                                         
        elif inout == "in":
            self.__token_i += 1
        else:
            inout = "in"
                
        type, identifier = self.__parseDeclaration( current_module )    
                            
        if self.__tokens[self.__token_i] == ',':
            self.__getNextToken()                

        return inout, type, identifier

    def __parseParentTypeNames( self ):
        parent_type_names = None
        if self.__tokens[self.__token_i] == ':':
            self.__assertNextToken( ':' )
            while self.__tokens[self.__token_i] != '{':
                if self.__tokens[self.__token_i] != ',':
                    next_token = self.__getNextToken()
                    try: parent_type_names.append( next_token )
                    except AttributeError: parent_type_names = [next_token]
        return parent_type_names        
        
    def __parseStructType( self, current_module ):
        self.__assertNextToken( "struct" )
        name = self.__getNextToken()
        qname = qname = current_module.getQualifiedName() + [name]
        tag = current_module.getTag() + self.__parseTag()
        parent_type_names = self.__parseParentTypeNames()
        struct_type = current_module.addStructType( qname, tag, parent_type_names )
        self.__assertNextToken( '{' )                
        while self.__tokens[self.__token_i] != "}":      
            struct_type.addMember( *self.__parseDeclaration( current_module ) )      
            self.__assertNextToken( ';' )
        self.__assertNextToken( '}' )
        self.__assertNextToken( ';' )
        self.__getTypeMapForScope( current_module )[name] = struct_type
        return struct_type
            
    def __parseTypedef( self, current_module ):
        self.__assertNextToken( "typedef" )
        original_type_name = self.__getNextToken()                
        self.__assertNextToken( '<' )        
                        
        if original_type_name == "sequence":
            value_type_name = self.__getNextToken()
            value_type = self.__getType( current_module, value_type_name )
            self.__assertNextToken( '>' )         
            new_type_name = self.__getNextToken()
            new_type_qname = current_module.getQualifiedName() + [new_type_name]
            tag = current_module.getTag() + self.__parseTag()                   
            type = current_module.addSequenceType( new_type_qname, tag, value_type )
        elif original_type_name == "map":
            key_type_name = self.__getNextToken()
            key_type = self.__getType( current_module, key_type_name )
            self.__assertNextToken( ',' )
            value_type_name = self.__getNextToken()
            value_type = self.__getType( current_module, value_type_name )
            self.__assertNextToken( '>' )
            new_type_name = self.__getNextToken()
            new_type_qname = current_module.getQualifiedName() + [new_type_name]            
            tag = current_module.getTag() + self.__parseTag()                   
            type = current_module.addMapType( new_type_qname, tag, key_type, value_type )
        else:
            raise NotImplementedError, "typedef with type " + self.__tokens[self.__token_i+1]

        self.__assertNextToken( ';' )
        
        self.__getTypeMapForScope( current_module )[new_type_name] = type
        
        return type
    
    def __parseTag( self ):
        self.__assertNextToken( '#' )
        return int( self.__getNextToken() )
