import sys, os.path, traceback
from unittest import TestCase, TestSuite, TextTestRunner
from optparse import OptionParser
from hashlib import md5
from cStringIO import StringIO

from idl_parser import parseIDL


__all__ = ["writeGeneratedFile", "generator_main"]


# Helper functions
def _isSourceLine( line, exclude_h_guard=True ): 
    if len( line ) > 2 and not line.startswith( "// " ) and not line.startswith( "# " ):
        return True
    elif exclude_h_guard and line.endswith( "_H" ) and ( line.startswith( "#ifndef" ) or line.startswith( "#define" ) ):
        return False
    else:
        return True
    
def writeGeneratedFile( file_path, new_file_contents, exclude_h_guard=True ):
    new_file_contents = new_file_contents.replace( "\r\n", "\n" )
    
    try:        
        old_file_contents_lines = [line.strip() for line in open( file_path, "r" ).readlines()]
        old_file_contents_hash = md5( "".join( [line for line in old_file_contents_lines if _isSourceLine( line, exclude_h_guard )] ) ).digest()        
        new_file_contents_lines = [line.strip() for line in StringIO( new_file_contents ).readlines()]          
        new_file_contents_hash = md5( "".join( [line for line in new_file_contents_lines if _isSourceLine( line, exclude_h_guard )] ) ).digest()        
    except IOError:
        old_file_contents_hash = 0
        new_file_contents_hash = 1
        
    if old_file_contents_hash != new_file_contents_hash:
        open( file_path, "wb" ).write( new_file_contents )
        print "wrote", file_path


class __GeneratorTest(TestCase):
    def __init__( self, idl_file_path, target_class ):
        TestCase.__init__( self )
        self.idl_file_path = idl_file_path
        self.target_class = target_class
    
    def runTest( self ):
        target = self.target_class()
        parseIDL( self.idl_file_path, target )   
        target_repr = repr( target )
        assert len( target_repr ) > 0 
        
        
def generator_main( target_class, out_file_ext=None ):    
    option_parser = OptionParser()
    option_parser.add_option( "-i", action="store", dest="in_path", default="." )
    option_parser.add_option( "-o", action="store", dest="out_path", default="." )
    option_parser.add_option( "-e", "--exclude", action="append", dest="excluded_file_names", default=[] )
    option_parser.add_option( "--test", dest="test", action="store_true", default=False )
    options = option_parser.parse_args()[0]

    if options.test:
        in_path = out_path = os.path.abspath( os.path.join( os.path.dirname( os.path.abspath( sys.modules[__name__].__file__ ) ), "..", "..", "tests", "wellformed" ) )
        test_suite = TestSuite()        
    else:
        in_path = os.path.abspath( options.in_path )
        out_path = os.path.abspath( options.out_path )    
    
    if os.path.exists( in_path ):    
        in_path_isdir = os.path.isdir( in_path )
        generate_paths = []
        out_path_isdir = os.path.isdir( out_path )
        if in_path_isdir and out_path_isdir:
            for root_dir_path, dir_names, file_names in os.walk( in_path ):
                for idl_file_name in [file_name for file_name in file_names if file_name.endswith( ".idl" )]:
                    idl_file_path = os.path.join( root_dir_path, idl_file_name )
                    if not idl_file_name in options.excluded_file_names and not idl_file_path in options.excluded_file_names:                        
                        if out_file_ext is not None or options.test:
                            out_file_path = os.path.join( out_path, os.path.dirname( idl_file_path[len( in_path ) + 1:] ), os.path.splitext( os.path.split( idl_file_path )[1] )[0] + out_file_ext )
                            if os.path.exists( out_file_path ) or options.test:
                                generate_paths.append( ( idl_file_path, out_file_path ) )                                
                        else:
                            generate_paths.append( ( idl_file_path, out_path ) )
        elif not in_path_isdir and not out_path_isdir:
            generate_paths.append( ( in_path, out_path ) )
        else:    
            raise ValueError, "both input and output paths must be a directory or both must be a file, not mixed (in_path=%s, in_path_isdir=%s, out_path=%s, out_path_isdir=%s)" % ( in_path, in_path_isdir, out_path, out_path_isdir )
    else:
        raise ValueError, "in_path does not exist (in_path=%s)" % in_path

    if len( generate_paths ) > 0:
        for idl_file_path, out_path in generate_paths:
            if options.test:
                test_suite.addTest( __GeneratorTest( idl_file_path, target_class ) )
            else:
                target = target_class()            
                parseIDL( idl_file_path, target )
                if os.path.isdir( out_path ): 
                    os.chdir( out_path )
                    target.generate() # Java targets that create one file per class
                else:
                    file_contents = repr( target ) # C++, Python targets that go into a single file
                    if len( file_contents ) > 0:
                        writeGeneratedFile( out_path, file_contents )

        if options.test:
            TextTestRunner().run( test_suite )