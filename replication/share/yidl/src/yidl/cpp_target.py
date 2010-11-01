from c_target import *
from string_utils import *


__all__ = [
           "CPPBoolType", 
           "CPPBufferType", 
           "CPPConstant",
           "CPPEnumeratedType",
           "CPPExceptionType",
           "CPPInclude",
           "CPPInterface", 
           "CPPMapType", 
           "CPPModule", 
           "CPPNumericType", 
           "CPPOperation", "CPPOperationParameter",
           "CPPPointerType",
           "CPPReferenceType",
           "CPPSequenceType", 
           "CPPStringType", 
           "CPPStructType",
           "CPPTarget",
          ]


class CPPConstruct:
    def getParentQualifiedNameMacro( self ): return "_".join( self.getQualifiedName()[:-1] ).upper() 


class CPPType(CPPConstruct):
    def getComparableValue( self, identifier ): return identifier
    def getConstant( self, identifier, value ): qname = self.getQualifiedName( "::" ); return "const static %(qname)s %(identifier)s = %(value)s;" % locals() 
    def getDeclarationTypeName( self ): return self.getQualifiedName( "::" )
    def getDefaultValue( self ): return None    
    def getDummyValue( self ): raise NotImplementedError
    def getGetterDefinition( self, identifier ): return self.getDeclarationTypeName() + " get_%(identifier)s() const { return %(identifier)s; }" % locals()
    def getInConstructorDeclaration( self, identifier, default_value=None, with_stl=True ): return self.getDeclarationTypeName() + lpad( " ", identifier ) + ( default_value is not None and ( " = " + str( default_value ) ) or "" ) 
    def getInConstructorInitialValue( self, identifier, with_stl=True ): return identifier    
    def getOutConstructorDeclaration( self, identifier, with_stl=True ): return self.getDeclarationTypeName() + ( not self.getDeclarationTypeName().endswith( "&" ) and "& " or "" ) + identifier
    def getSetterDefinitions( self, identifier ): qname = self.getDeclarationTypeName(); return ( "void set_%(identifier)s( %(qname)s %(identifier)s ) { this->%(identifier)s = %(identifier)s; }" % locals(), )    
    def getTempValueTypeName( self ): return self.getName()


class CPPCompoundType(CPPType):
    def getConstant( self, *args, **kwds ): raise NotImplementedError
    def getDummyValue( self ): return self.getDeclarationTypeName() + "()"
    def getGetterDefinition( self, identifier ): return "const " + self.getDeclarationTypeName() + "& get_%(identifier)s() const { return %(identifier)s; }" % locals()
    def getInConstructorDeclaration( self, identifier, default_value=None, with_stl=True ): return "const " + self.getDeclarationTypeName() + "& " + identifier     
    def getSetterDefinitions( self, identifier ): const_reference_decl = "const " + self.getDeclarationTypeName() + "& "; return ( """void set_%(identifier)s( %(const_reference_decl)s %(identifier)s ) { this->%(identifier)s = %(identifier)s; }""" % locals(), )
    def getTempValueTypeName( self ): return self.getDeclarationTypeName()
    

class CPPConstant(CConstant, CPPConstruct):
    def __repr__( self ):
        return self.getType().getConstant( self.getIdentifier(), self.getValue() ) 
    

class CPPBoolType(CBoolType, CPPType):
    def getDefaultValue( self ): return "false"
    def getDummyValue( self ): return "false"


class CPPBufferType(CBufferType, CPPType): pass


class CPPEnumeratedType(CEnumeratedType, CPPType):
    def getDefaultValue( self ):
        return self.getEnumerators()[0].getIdentifier()


class CPPExceptionType(CExceptionType, CPPType): pass
class CPPInclude(CInclude, CPPConstruct): pass
class CPPInterface(CInterface, CPPConstruct): pass
class CPPMapType(CMapType, CPPCompoundType): pass    


class CPPModule(CModule, CPPConstruct):
    def __repr__( self ):            
        return "namespace " + self.getName() + "\n{\n" + indent( CModule.__repr__( self ), "  " ) + "\n};\n"


class CPPNumericType(CNumericType, CPPType): pass
class CPPOperation(COperation, CPPConstruct): pass
class CPPOperationParameter(COperationParameter, CPPConstruct): pass


class CPPPointerType(CPointerType, CPPType):
    def getDeclarationTypeName( self ):
        if self.getName() == "ptr" or self.getName() == "any":
            return "void*"
        else:
            return self.getQualifiedName( "::" )    


class CPPReferenceType(CReferenceType, CPPType): pass    
class CPPSequenceType(CSequenceType, CPPCompoundType): pass
    
    
class CPPStringType(CStringType, CPPType):
    def getDeclarationTypeName( self ): return "std::string"
    def getDummyValue( self ): return "std::string()"
    def getGetterDefinition( self, identifier ): return "const std::string& get_%(identifier)s() const { return %(identifier)s; }" % locals()
    def getIncludes( self ): return CStringType.getIncludes( self ) + [CPPInclude( self.getScope(), "string", False )]
    def getInConstructorDeclaration( self, identifier, default_value=None, with_stl=True ): 
        if with_stl:
            in_constructor_declaration = "const std::string& " + identifier
            if default_value is not None: in_constructor_declaration += " = " + str( default_value )
        else:
            if default_value is not None:
                default_value = str( default_value )
                in_constructor_declaration = "const char* %(identifier)s = %(default_value)s, %(identifier)s_len = strlen( %(default_value ) )" % locals()
            else:
                in_constructor_declaration = "const char* %(identifier)s, size_t %(identifier)s_len" % locals()
        return in_constructor_declaration
    def getInConstructorInitialValue( self, identifier, with_stl=True ): return with_stl and identifier or ( "%(identifier)s, %(identifier)s_len" % locals() )
    def getOutConstructorDeclaration( self, identifier, with_stl=True ): return "std::string& " + identifier
    def getReturnTypeName( self ): return "std::string"
    def getSetterDefinitions( self, identifier ): return ( """void set_%(identifier)s( const std::string& %(identifier)s ) { set_%(identifier)s( %(identifier)s.c_str(), %(identifier)s.size() ); }""" % locals(),
                                               """void set_%(identifier)s( const char* %(identifier)s, size_t %(identifier)s_len ) { this->%(identifier)s.assign( %(identifier)s, %(identifier)s_len ); }""" % locals() )
    def getTempValueTypeName( self ): return "std::string"


class CPPStructType(CStructType, CPPCompoundType):
    def getAccessors( self):
        if len( self.getMembersDefinedInThisClass() ) > 0:
            accessors = []
            for member in self.getMembersDefinedInThisClass():
                accessors.extend( member.getType().getSetterDefinitions( member.getIdentifier() ) )                
                accessors.append( member.getType().getGetterDefinition( member.getIdentifier() ) )
            return "\n\n  " + "\n  ".join( accessors )
        else:
            return ""

    # The _ take members parameters so the OperationRequestTypes can add parameters to the constructor alone
    def _getConstructorPrototype( self, with_stl ):
        name = self.getName()
        return "%(name)s( " % locals() + ", ".join( [member.getType().getInConstructorDeclaration( member.getIdentifier(), member.getDefaultValue(), with_stl=with_stl ) for member in self.getMembers()] ) + " )"            
            
    def _getConstructorInitializers( self, with_stl ):
        return self.__getConstructorInitializers( [member.getType().getInConstructorInitialValue( member.getIdentifier(), with_stl ) for member in self.getMembers()] )

    # Helper function shared between _getDefaultConstructorInitializers and _getConstructorInitializers
    def __getConstructorInitializers( self, member_initial_values ):
        if len( self.getMembers() ) > 0:
            assert len( member_initial_values ) == len( self.getMembers() )
        
            this_constructor_initializers = []        
            parent_constructor_initializers = {} # parent -> initializers    
        
            for member_i in xrange( len( self.getMembers() ) ):
                member = self.getMembers()[member_i]
                member_initial_value = member_initial_values[member_i]
                if member_initial_value is not None:
                    try:
                        parents = self.getMemberToParentTypeMap()[member_i]
                        if isinstance( parents, basestring ): parents = ( parents, )
                    except KeyError: 
                        parents = ( None, ) # This class only
                        
                    for parent in parents:
                        if parent is None:
                            this_constructor_initializers.append( member.getIdentifier() + "( " + member_initial_value + " )" )
                        else:
                            parent_constructor_initializers.setdefault( parent, [] ).append( member_initial_value )
                
            for parent, parent_constructor_initializer in parent_constructor_initializers.iteritems():
                this_constructor_initializers = [parent + "( " + ", ".join( parent_constructor_initializer ) + " )"] + this_constructor_initializers

            return ", ".join( this_constructor_initializers )
        else:
            return ""
    
    def getDefaultConstructor( self ):        
        have_member_without_default_value = False
        for member in self.getMembers():
            if member.getDefaultValue() is None:
                have_member_without_default_value = True
                break
               
        if len( self.getMembers() ) == 0 or have_member_without_default_value:
            default_constructor = self.getName() + "()" # A real default constructor, no parameters
        else:
            default_constructor = self._getConstructorPrototype( with_stl=True ) # A default constructor with all default parameters, to replace the STL and no STL constructors
            
        default_constructor_initializers = self._getDefaultConstructorInitializers()
        if len( default_constructor_initializers ) > 0:
            default_constructor += " : " + default_constructor_initializers
        return default_constructor + " { }"

    def _getDefaultConstructorInitializers( self ):
        member_initial_values = []
        
        have_member_without_default_value = False
        for member in self.getMembers():
            if member.getDefaultValue() is None:
                have_member_without_default_value = True
                break

        for member_i in xrange( len( self.getMembers() ) ):            
            member = self.getMembers()[member_i]
            if member.getDefaultValue() is not None: # If the member has a default value from its declaration, use member.getIdentifier() as the initial value
                if have_member_without_default_value: # If we have a member without a default value this is a real default constructor => no values will be coming from the constructor parameters
                    member_initial_value = member.getDefaultValue()
                else: # This is a default constructor with only default values
                    member_initial_value = member.getIdentifier()
            elif member.getType().getDefaultValue() is not None: # If the member needs to be initialized (pointers, numeric types), give it a sensible default value
                member_initial_value = member.getType().getDefaultValue()
            else:                
                if self.getMemberToParentTypeMap().has_key( member_i ): # If the parent is part of a parent, give it a dummy placeholder value
                    member_initial_value = member.getType().getDummyValue()
                else:
                    member_initial_value = None # Otherwise don't initialize the member in the constructor             
                
            member_initial_values.append( member_initial_value )
            
        return self.__getConstructorInitializers( member_initial_values )

    def getMemberDeclarations( self ):
        if len( self.getMembers() ) > 0:
            return "\n\nprotected:\n  " + "\n  ".join( [member.getType().getDeclarationTypeName() + " " + member.getIdentifier() + ";" for member in self.getMembersDefinedInThisClass()] )
        else:
            return ""

    def getMembersDefinedInThisClass( self ):
        return [self.getMembers()[member_i] for member_i in xrange( len( self.getMembers() ) ) if not self.getMemberToParentTypeMap().has_key( member_i )]

    def getMemberToParentTypeMap( self ):
        return {}

    def getNoSTLConstructor( self ):
        if len( self.getMembers() ) > 0:
            for member in self.getMembers(): 
                if member.getDefaultValue() is None:                 
                    no_stl_constructor = "\n  " + self._getConstructorPrototype( with_stl=False ) + " : " + self._getConstructorInitializers( with_stl=False ) + " { }"
                    if no_stl_constructor == self.getSTLConstructor(): no_stl_constructor = ""            
                    return no_stl_constructor                    
        return ""
                                    
    def getOperatorEquals( self ):
        name = self.getName()
        if len( self.getMembersDefinedInThisClass() ) > 0:
            operator_equals = []
            for member in self.getMembersDefinedInThisClass():
                operator_equals.append( member.getType().getComparableValue( member.getIdentifier() ) + " == " + member.getType().getComparableValue( "other." + member.getIdentifier() ) )
            return "bool operator==( const %(name)s& other ) const { return " % locals() + " && ".join( operator_equals ) + "; }\n"
        else:
            return "bool operator==( const %(name)s& ) const { return true; }\n" % locals()
                                    
    def getSTLConstructor( self ):
        if len( self.getMembers() ) > 0:
            for member in self.getMembers(): 
                if member.getDefaultValue() is None: 
                    return "\n  " + self._getConstructorPrototype( with_stl=True ) + " : " + self._getConstructorInitializers( with_stl=True ) + " { }"
        return ""                


class CPPTarget(CTarget): pass
    
    