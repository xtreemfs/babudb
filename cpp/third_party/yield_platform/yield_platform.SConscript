import sys, os.path


try:
    Import( "build_env", "build_conf" )
except:
    build_env = {} # Init Environment() from this so that it doesn't start with default values for e.g. CC, which induces pkginfo popens on Sun

    build_env["CPPPATH"] = os.environ.has_key( "CPPPATH" ) and os.environ["CPPPATH"].split( ":" ) or []
    build_env["CCFLAGS"] = os.environ.get( "CCFLAGS", "" )
    build_env["LIBPATH"] = os.environ.has_key( "LIBPATH" ) and os.environ["LIBPATH"].split( ":" ) or []
    build_env["LINKFLAGS"] = os.environ.get( "LINKFLAGS", "" )
    build_env["LIBS"] = os.environ.has_key( "LIBS" ) and os.environ["LIBS"].split( " " ) or []

    if sys.platform.startswith( "win" ):
        build_env["CCFLAGS"] += '/EHsc /GR- /D "_CRT_DISABLE_PERFCRIT_LOCKS" /D "WIN32" ' # GR- is -fno-rtti, EHsc is to enable exception handling
        if ARGUMENTS.get( "release", 0 ): build_env["CCFLAGS"] += "/MD "
        else: build_env["CCFLAGS"] += "/MDd /ZI /W3 "

        for include_dir_path in []:
            if not include_dir_path in build_env["CPPPATH"]: build_env["CPPPATH"].append( include_dir_path )
        for lib_dir_path in []:
            if not lib_dir_path in build_env["LIBPATH"]: build_env["LIBPATH"].append( lib_dir_path )
    else:
        # -fPIC (Platform Independent Code) to compile a library as part of a shared object
        # -fno-rtti to disable RTTI
        build_env["CCFLAGS"] += "-fno-rtti -fPIC "
        if sys.platform == "linux2": build_env["LIBS"].extend( ( "pthread", "util", "dl", "rt", "stdc++" ) )
        elif sys.platform == "darwin": build_env["LINKFLAGS"] += "-framework Carbon "; build_env["LIBS"].append( "iconv" )
        elif sys.platform == "freebsd5": build_env["LIBS"].extend( ( "intl", "iconv" ) )
        elif sys.platform == "openbsd4": build_env["LIBS"].extend( ( "m", "pthread", "util", "iconv" ) )
        elif sys.platform == "sunos5": build_env["tools"] = ["gcc", "g++", "gnulink", "ar"]; build_env["CCFLAGS"] += "-Dupgrade_the_compiler_to_use_STL=1 -D_REENTRANT "; build_env["LIBS"].extend( ( "stdc++", "m", "socket", "nsl", "kstat", "rt", "iconv", "cpc" ) )
        if ARGUMENTS.get( "release", 0 ): build_env["CCFLAGS"] += "-O2 "
        else: build_env["CCFLAGS"] += "-g -D_DEBUG "
        if ARGUMENTS.get( "profile-cpu", 0 ):  build_env["CCFLAGS"] += "-pg "; build_env["LINKFLAGS"] += "-pg "
        if ARGUMENTS.get( "profile-heap", 0 ): build_env["CCFLAGS"] += "-fno-omit-frame-pointer "; build_env["LIBS"].append( "tcmalloc" )

        for include_dir_path in []:
            if not include_dir_path in build_env["CPPPATH"]: build_env["CPPPATH"].append( include_dir_path )
        for lib_dir_path in []:
            if not lib_dir_path in build_env["LIBPATH"]: build_env["LIBPATH"].append( lib_dir_path )

    build_env = Environment( **build_env )
    build_conf = build_env.Configure()
    Export( "build_env", "build_conf" )


for include_dir_path in ( 'include', ):
    if not include_dir_path in build_env["CPPPATH"]: build_env["CPPPATH"].append( include_dir_path )
for lib_dir_path in ( 'lib', ):
    if not lib_dir_path in build_env["LIBPATH"]: build_env["LIBPATH"].append( lib_dir_path )


if FindFile( "yield_platform_custom.SConscript", "." ):
    SConscript( "yield_platform_custom.SConscript" )

    
    


# Don't add libs until after yield_platform_custom.SConscript and dependency SConscripts, to avoid failing build_conf checks because of missing -l libs
for lib in []:
   if not lib in build_env["LIBS"]: build_env["LIBS"].insert( 0, lib )

if sys.platform.startswith( "win" ):
    for lib in []:
       if not lib in build_env["LIBS"]: build_env["LIBS"].insert( 0, lib )
else:
    for lib in []:
       if not lib in build_env["LIBS"]: build_env["LIBS"].insert( 0, lib )


( build_env.Library( r"lib/yield_platform", (
r"src/yield/platform/directory_walker.cpp",
r"src/yield/platform/disk_operations.cpp",
r"src/yield/platform/file.cpp",
r"src/yield/platform/locks.cpp",
r"src/yield/platform/memory_mapped_file.cpp",
r"src/yield/platform/path.cpp",
r"src/yield/platform/platform_exception.cpp",
r"src/yield/platform/process.cpp",
r"src/yield/platform/processor_set.cpp",
r"src/yield/platform/shared_library.cpp",
r"src/yield/platform/socket_lib.cpp",
r"src/yield/platform/stat.cpp",
r"src/yield/platform/thread.cpp",
r"src/yield/platform/time.cpp"
) ) )
