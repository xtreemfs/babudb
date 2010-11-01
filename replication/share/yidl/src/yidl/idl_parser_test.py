import sys, os.path
from glob import glob
from unittest import TestCase, TestSuite, TextTestRunner

from idl_parser import parseIDL
from target import Target


# Constants
#M_BEGIN = "module test { "
#M_END = " };"
#IF_BEGIN = "interface Test { "
#IF_END = " };"
#MIF_BEGIN = M_BEGIN + IF_BEGIN
#MIF_END = IF_END + M_END
#
#
## Test data
#VALID_IDL = [
#MIF_BEGIN + "void test();" + MIF_END,
#MIF_BEGIN + "void test( string test );" + MIF_END,
#MIF_BEGIN + "void test( in string test );" + MIF_END,
#MIF_BEGIN + "void test( boolean testbool, int testint, string teststring );" + MIF_END,
#M_BEGIN + "struct TestStruct {};" + M_END,
#M_BEGIN + "struct TestStruct {}; typedef sequence<TestStruct> TestStructs;" + M_END,
#M_BEGIN + "struct TestStruct { int testint; };" + IF_BEGIN + "void test( TestStruct teststruct );" + MIF_END,
#M_BEGIN + "struct TestStruct { int testint; }; typedef sequence<TestStruct> TestStructs;" + IF_BEGIN + "void test( TestStructs teststructs );" + MIF_END,
#]
#
#INVALID_IDL = [
#MIF_BEGIN + "void test()" + MIF_END, # Missing semicolon
#MIF_BEGIN + "void test( string );" + MIF_END, # Missing identifier
#MIF_BEGIN + "void test( in string );" + MIF_END, # Missing identifier
#M_BEGIN + "struct TestStruct {}" + M_END, # Missing semicolon after }
#M_BEGIN + "struct TestStruct { int };" + M_END, # Missing identifier
#M_BEGIN + "struct TestStruct {}; typedef sequence<TestStructX> TestStructs;" + M_END, # Non-existent type in typedef
#]


class IDLParserTest(TestCase):
    def __init__( self, idl_file_path ):
        TestCase.__init__( self )
        self.idl_file_path = idl_file_path = idl_file_path

    def shortDescription( self ):
        return self.__class__.__name__ + "( " + self.idl_file_path + " )"


class WellFormedIDLParserTest(IDLParserTest):
    def runTest( self ):
        parseIDL( self.idl_file_path, Target() )


class IllFormedIDLParserTest(IDLParserTest):
    def runTest( self ):                
        try:
            parseIDL( self.idl_file_path )
            passed = True
        except:
            passed = False
            
        if passed:
            self.fail()


suite = TestSuite()

tests_dir_path = os.path.abspath( os.path.join( os.path.dirname( os.path.abspath( sys.modules[__name__].__file__ ) ), "..", "..", "tests" ) )

for idl_file_path in glob( os.path.join( tests_dir_path, "wellformed", "*.idl" ) ):
    suite.addTest( WellFormedIDLParserTest( idl_file_path ) )

for idl_file_path in glob( os.path.join( os.path.join( tests_dir_path, "illformed" ), "*.idl" ) ):
    suite.addTest( IllFormedIDLParserTest( idl_file_path ) )


if __name__ == "__main__":
    TextTestRunner().run( suite )
    