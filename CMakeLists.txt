# cmake support for building rippled. The rippled specific settings
# below can be set at the command line as `-D<setting>=<value>`.
#
# * `target` is a period separated tuple from the sets
#    {gcc,clang,msvc} x {debug, release} x {unity, nounity} x {coverage} x {profile}
#
#     Example, build gcc debug nonunity build
#       -Dtarget=gcc.debug.nounity
#     Example, clang release unity build
#       -Dtarget=clang.release.unity
#     Example, visual studio debug unity build
#       -Dtarget=msvc.release.unity
#     Example, build gcc release unity build suited for profiling with perf
#       -Dtarget=gcc.release.unity.profile
#     Example, build gcc debug unity build suited for measuring code coverage
#     with gcov
#       -Dtarget=gcc.release.unity.coverage
#
#
#   The default is a unity debug build using gcc (linux), clang (osx), and
#   msvc (windows).
#
#   Note the generated Visual Studio solution will always have two projects,
#   one unity and one non-unity. If the `target` is unity, the default project
#   will be named `rippled` and second non-default (non-unity) project
#   will be called `rippled_classic`. Likewise, if the `target` is non-unity,
#   the project will have a default project called `rippled` (now non-unity)
#   and second non-default (unity) project `rippled_unity`. In either
#   case, only the `rippled` build will be enabled by default.
#
# * `assert` whether to enable asserts in release build
#
#      Example, enable asserts even in release builds
#        -Dassert=True
#
#    Default is not to enable asserts in release builds.
#
# * `san` enable clang sanitizers
#
#     Example, enable thread sanitizer
#       -Dsan=thread
#     Example, enable address sanitizer
#       -Dsan=address
#
# * `static`, on linux, link protobuf, openssl, libc++, and boost
#   statically.
#
#     Example, enable static linking
#       -Dstatic=True
#
# * `jemalloc`, on linux, enables jemalloc for heap profiling.
#
#    Example, enable jemalloc
#      -Djemallc=True
#
#########################################################
# CMAKE_C_COMPILER and CMAKE_CXX_COMPILER must be defined
# before the project statement; However, the project
# statement will clear CMAKE_BUILD_TYPE. CACHE variables,
# along with the order of this code, are used to work
# around these constraints.
#
# Don't put any code above or in this block, unless it
# has similar constraints.
cmake_minimum_required(VERSION 3.8.0)
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/Builds/CMake")
include(CMakeFuncs)
set(openssl_min 1.0.2)
parse_target()
project(rippled)
#########################################################
get_property(is_multiconfig GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)

if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
  set(dir "build")
  set(cmd "cmake")
  if (target)
    set(dir "${dir}/${target}")
    set(cmd "${cmd} -Dtarget=${target}")
  elseif(CMAKE_BUILD_TYPE)
    set(dir "${dir}/${CMAKE_BUILD_TYPE}")
    set(cmd "${cmd} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
  else()
    set(dir "${dir}/default")
  endif()
  set(cmd "${cmd} ${CMAKE_SOURCE_DIR}")

  message(FATAL_ERROR "Builds are not allowed in ${CMAKE_SOURCE_DIR}.\n"
    "Instead:\n"
    "1) Remove the CMakeCache.txt file and CMakeFiles directory "
   "from ${CMAKE_SOURCE_DIR}.\n"
    "2) Create a directory to hold your build files, for example: ${dir}.\n"
    "3) Change to that directory.\n"
    "4) Run cmake targetting ${CMAKE_SOURCE_DIR}, for example: ${cmd}")
endif()
if("${CMAKE_GENERATOR}" MATCHES "Visual Studio" AND
    NOT ("${CMAKE_GENERATOR}" MATCHES .*Win64.*))
  message(FATAL_ERROR "Visual Studio 32-bit build is unsupported. Use
    -G\"${CMAKE_GENERATOR} Win64\"")
endif()

setup_build_cache()

if(nonunity)
    get_cmake_property(allvars VARIABLES)
    string(REGEX MATCHALL "[^;]*(DEBUG|RELEASE)[^;]*" matchvars "${allvars}")
    foreach(var IN LISTS matchvars)
        string(REGEX REPLACE "(DEBUG|RELEASE)" "\\1CLASSIC" newvar ${var})
        set(${newvar} ${${var}})
    endforeach()

    get_cmake_property(allvars CACHE_VARIABLES)
    string(REGEX MATCHALL "[^;]*(DEBUG|RELEASE)[^;]*" matchvars "${allvars}")
    foreach(var IN LISTS matchvars)
        string(REGEX REPLACE "(DEBUG|RELEASE)" "\\1CLASSIC" newvar ${var})
        set(${newvar} ${${var}} CACHE STRING "Copied from ${var}")
    endforeach()
endif()

determine_build_type()

check_gcc4_abi()

setup_build_boilerplate()

special_build_flags()

############################################################

include_directories(
  src
  src/beast
  src/beast/include
  src/beast/extras)

############################################################

use_boost(
  # resist the temptation to alphabetize these. coroutine
  # must come before context.
  chrono
  coroutine
  context
  date_time
  filesystem
  program_options
  regex
  serialization
  system
  thread)

use_pthread()

use_openssl(${openssl_min})

######################################################################
### CMake is not a dependency manager. We, however, chose to       ###
### incorporate some deps as subtrees/submodules. For simple       ###
### sources, we then define our own library based directly on      ###
### sources. For projects that have a well-defined build system    ###
### (e.g. cmake), we use ExternalProject to invoke the build. A    ###
### better solution to all of this would be a *real* dependency    ###
### management system or relying on native package managers.       ###
######################################################################

set(RIPPLED_NIH_LIBRARIES)
include (ExternalProject)
######################################################################
###   NIH dep: secp256k1                                           ###
######################################################################

add_library (secp256k1 STATIC
  src/secp256k1/src/secp256k1.c)
target_compile_definitions (secp256k1
  PRIVATE
    USE_NUM_NONE
    USE_FIELD_10X26
    USE_FIELD_INV_BUILTIN
    USE_SCALAR_8X32
    USE_SCALAR_INV_BUILTIN)
target_include_directories (secp256k1
  PUBLIC  ${CMAKE_SOURCE_DIR}/src
  PRIVATE ${CMAKE_SOURCE_DIR}/src/secp256k1)
target_compile_options (secp256k1
  PRIVATE
  $<$<BOOL:${MSVC}>:
    -wd4319
  >
  $<$<NOT:$<BOOL:${MSVC}>>:
    -Wno-deprecated-declarations
    -Wno-unused-function
    -Wno-nonnull-compare
  >)
add_library (NIH::secp256k1 ALIAS secp256k1)
list (APPEND RIPPLED_NIH_LIBRARIES NIH::secp256k1)

######################################################################
###   NIH dep: ed25519-donna                                       ###
######################################################################

add_library (ed25519-donna STATIC
  src/ed25519-donna/ed25519.c)
target_include_directories (ed25519-donna
  PUBLIC  ${CMAKE_SOURCE_DIR}/src
  PRIVATE ${CMAKE_SOURCE_DIR}/src/ed25519-donna)
target_compile_options (ed25519-donna
  PRIVATE
  ## see https://github.com/floodyberry/ed25519-donna/issues/29
  $<$<BOOL:${APPLE}>:
    -Wno-macro-redefined>)
add_library (NIH::ed25519-donna ALIAS ed25519-donna)
list (APPEND RIPPLED_NIH_LIBRARIES NIH::ed25519-donna)

######################################################################
###   NIH dep: lz4                                                 ###
######################################################################

add_library (lz4 STATIC
  src/lz4/lib/lz4.c
  src/lz4/lib/lz4hc.c
  src/lz4/lib/lz4frame.c
  src/lz4/lib/xxhash.c)
target_compile_definitions (lz4
  PRIVATE XXH_NAMESPACE=LZ4_)
target_include_directories (lz4
  PUBLIC  ${CMAKE_SOURCE_DIR}/src/lz4/lib)
add_library (NIH::lz4 ALIAS lz4)
set_target_properties (lz4 PROPERTIES POSITION_INDEPENDENT_CODE ON)
list (APPEND RIPPLED_NIH_LIBRARIES NIH::lz4)

######################################################################
###   NIH dep: sqlite                                              ###
######################################################################

add_library (sqlite3 STATIC
  src/sqlite/sqlite/sqlite3.c)
##
## When compiled with SQLITE_THREADSAFE=1, SQLite operates in serialized mode.
## In this mode, SQLite can be safely used by multiple threads with no restriction.
##
##   VFALCO NOTE This implies a global mutex!
##
##  When compiled with SQLITE_THREADSAFE=2, SQLite can be used in a
##  multithreaded program so long as no two threads attempt to use the
##    same database connection at the same time.
##
##   VFALCO NOTE This is the preferred threading model.
##
target_compile_definitions (sqlite3
  PRIVATE SQLITE_THREADSAFE=1)
target_compile_options (sqlite3
  PRIVATE
  $<$<BOOL:${MSVC}>:
    -wd4100
    -wd4127
    -wd4232
    -wd4244
    -wd4701
    -wd4706
    -wd4996
  >
  $<$<NOT:$<BOOL:${MSVC}>>:
    -Wno-array-bounds
  >)
add_library (NIH::sqlite3 ALIAS sqlite3)
list (APPEND RIPPLED_NIH_LIBRARIES NIH::sqlite3)
## HACK ..pseudo install our files. We should really be using
## an installed package for sqlite
file (MAKE_DIRECTORY
    ${CMAKE_BINARY_DIR}/sqlite3/include
    ${CMAKE_BINARY_DIR}/sqlite3/lib)
target_include_directories (sqlite3
  PUBLIC  ${CMAKE_BINARY_DIR}/sqlite3/include)
add_custom_command (TARGET sqlite3 POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_SOURCE_DIR}/src/sqlite/sqlite ${CMAKE_BINARY_DIR}/sqlite3/include
  COMMAND ${CMAKE_COMMAND} -E copy
    $<TARGET_FILE:sqlite3> ${CMAKE_BINARY_DIR}/sqlite3/lib)

######################################################################
###   NIH dep: soci                                                ###
######################################################################

ExternalProject_Add (soci
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/soci
  DOWNLOAD_COMMAND ""
  CMAKE_ARGS
    $<$<BOOL:${CMAKE_VERBOSE_MAKEFILE}>:
      -DCMAKE_VERBOSE_MAKEFILE=ON
    >
    -DCMAKE_INSTALL_PREFIX=_installed
    -DSOCI_CXX_C11=ON
    #-DCMAKE_CXX_STANDARD=14
    -DSOCI_STATIC=ON
    -DSOCI_LIBDIR=lib
    -DSOCI_SHARED=OFF
    -DSOCI_TESTS=OFF
    -DBOOST_ROOT=${BOOST_ROOT}
    -DWITH_BOOST=ON
    -DSOCI_DB2=OFF
    -DSOCI_FIREBIRD=OFF
    -DSOCI_MYSQL=OFF
    -DSOCI_ODBC=OFF
    -DSOCI_ORACLE=OFF
    -DSOCI_POSTGRESQL=OFF
    -DSOCI_SQLITE3=ON
    -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR}/sqlite3
    -DCMAKE_INCLUDE_PATH=$<TARGET_PROPERTY:sqlite3,INTERFACE_INCLUDE_DIRECTORIES>
    $<$<BOOL:${APPLE}>:
      -DCMAKE_FIND_FRAMEWORK=LAST
    >
    $<$<BOOL:${MSVC}>:
      "-DCMAKE_CXX_FLAGS=-GR -Gd -fp:precise -FS -EHa -MP"
      "-DCMAKE_CXX_FLAGS_DEBUG=-MTd"
      "-DCMAKE_CXX_FLAGS_RELEASE=-MT"
      "-DCMAKE_CXX_FLAGS_RELWITHDEBINFO=-MT"
      "-DCMAKE_CXX_FLAGS_MINSIZEREL=-MT"
    >
    $<$<NOT:$<BOOL:${MSVC}>>:
      "-DCMAKE_CXX_FLAGS=-Wno-deprecated-declarations"
    >
  LOG_BUILD ON
  LOG_CONFIGURE ON
  BUILD_COMMAND
    ${CMAKE_COMMAND}
    --build .
    --config $<CONFIG>
  TEST_COMMAND ""
  DEPENDS sqlite3
)
# SOCI has a weird library naming scheme...
set(LIB_PRE ${CMAKE_STATIC_LIBRARY_PREFIX})
set(LIB_POST ${CMAKE_STATIC_LIBRARY_SUFFIX})
if (WIN32)
  set(LIB_PRE lib) # for some reason soci on windows still prepends lib (non-standard)
  # this name can change whenever soci is updated
  set(LIB_POST "_4_0${LIB_POST}")
endif()
ExternalProject_Get_Property (soci BINARY_DIR)
set (install_path ${BINARY_DIR}/_installed)
add_library (soci_core STATIC IMPORTED GLOBAL)
set_target_properties (soci_core PROPERTIES
  IMPORTED_LOCATION ${install_path}/lib/${LIB_PRE}soci_core${LIB_POST}
  INTERFACE_INCLUDE_DIRECTORIES ${install_path}/include)
add_dependencies (soci_core soci) # something has to depend on the ExternalProject to trigger it
## we have to pre-create include paths to workaround cmake issue
## see https://gitlab.kitware.com/cmake/cmake/issues/15052
file (MAKE_DIRECTORY ${install_path}/include)
if (${CMAKE_VERSION} VERSION_GREATER_EQUAL 3.11)
  add_library (NIH::soci_core ALIAS soci_core)
  list (APPEND RIPPLED_NIH_LIBRARIES NIH::soci_core)
else ()
  list (APPEND RIPPLED_NIH_LIBRARIES soci_core)
endif ()

add_library (soci_empty STATIC IMPORTED GLOBAL)
set_target_properties (soci_empty PROPERTIES
  IMPORTED_LOCATION ${install_path}/lib/${LIB_PRE}soci_empty${LIB_POST})
target_link_libraries(soci_empty INTERFACE soci_core)
if (${CMAKE_VERSION} VERSION_GREATER_EQUAL 3.11)
  add_library (NIH::soci_empty ALIAS soci_empty)
  list (APPEND RIPPLED_NIH_LIBRARIES NIH::soci_empty)
else ()
  list (APPEND RIPPLED_NIH_LIBRARIES soci_empty)
endif ()

add_library (soci_sqlite STATIC IMPORTED GLOBAL)
set_target_properties (soci_sqlite PROPERTIES
  IMPORTED_LOCATION ${install_path}/lib/${LIB_PRE}soci_sqlite3${LIB_POST})
target_link_libraries (soci_sqlite
  INTERFACE sqlite3)
target_link_libraries(soci_sqlite INTERFACE soci_core)
if (${CMAKE_VERSION} VERSION_GREATER_EQUAL 3.11)
  add_library (NIH::soci_sqlite ALIAS soci_sqlite)
  list (APPEND RIPPLED_NIH_LIBRARIES NIH::soci_sqlite)
else ()
  list (APPEND RIPPLED_NIH_LIBRARIES soci_sqlite)
endif ()


######################################################################
###   NIH dep: snappy                                              ###
######################################################################

ExternalProject_Add (snappy
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/src/snappy/snappy
  DOWNLOAD_COMMAND ""
  CMAKE_ARGS
    -DCMAKE_CXX_STANDARD=14
    $<$<BOOL:${CMAKE_VERBOSE_MAKEFILE}>:
      -DCMAKE_VERBOSE_MAKEFILE=ON
    >
    -DCMAKE_INSTALL_PREFIX=_installed
    -DBUILD_SHARED_LIBS=OFF
    -DSNAPPY_BUILD_TESTS=OFF
    $<$<BOOL:${MSVC}>:
      "-DCMAKE_CXX_FLAGS=-wd4244 -wd4267 -GR -Gd -fp:precise -FS -EHa -MP"
      "-DCMAKE_CXX_FLAGS_DEBUG=-MTd"
      "-DCMAKE_CXX_FLAGS_RELEASE=-MT"
      "-DCMAKE_CXX_FLAGS_RELWITHDEBINFO=-MT"
      "-DCMAKE_CXX_FLAGS_MINSIZEREL=-MT"
    >
  LOG_BUILD ON
  LOG_CONFIGURE ON
  BUILD_COMMAND
    ${CMAKE_COMMAND}
    --build .
    --config $<CONFIG>
  TEST_COMMAND ""
)
## TODO: does this depend on zlib and/or lzo?)
ExternalProject_Get_Property (snappy BINARY_DIR)
set (install_path ${BINARY_DIR}/_installed)
add_library (snappy_lib STATIC IMPORTED GLOBAL)
set_target_properties (snappy_lib PROPERTIES
    IMPORTED_LOCATION ${install_path}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX}
    INTERFACE_INCLUDE_DIRECTORIES ${install_path}/include)
add_dependencies (snappy_lib snappy)
file (MAKE_DIRECTORY ${install_path}/include)
if (${CMAKE_VERSION} VERSION_GREATER_EQUAL 3.11)
  add_library (NIH::snappy ALIAS snappy_lib)
  list (APPEND RIPPLED_NIH_LIBRARIES NIH::snappy)
else ()
  list (APPEND RIPPLED_NIH_LIBRARIES snappy_lib)
endif ()

######################################################################
###   NIH dep: rocksdb                                             ###
######################################################################

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(NOT MSVC AND COMPILER_SUPPORTS_CXX11)
  ExternalProject_Add (rocksdb
    ## here we copy locally from our source tree to
    ## the standard EP source dir because we want to do
    ## an in-source makefile build and don't want this to
    ## be done in teh main source tree.
    DOWNLOAD_COMMAND
      ${CMAKE_COMMAND} -E copy_directory
      ${CMAKE_SOURCE_DIR}/src/rocksdb2 <SOURCE_DIR>
    CONFIGURE_COMMAND ""
    LOG_BUILD ON
    BUILD_IN_SOURCE ON
    BUILD_COMMAND
      make
      USE_RTTI=1
      DISABLE_JEMALLOC=1
      "CFLAGS=-I$<JOIN:$<TARGET_PROPERTY:snappy_lib,INTERFACE_INCLUDE_DIRECTORIES>, -I>"
      INSTALL_PATH=_installed
      install-static
    INSTALL_COMMAND ""
    TEST_COMMAND ""
    DEPENDS snappy_lib
  )
  ExternalProject_Get_Property (rocksdb SOURCE_DIR)
  set (install_path ${SOURCE_DIR}/_installed)
  add_library (rocksdb_lib STATIC IMPORTED GLOBAL)
  add_dependencies (rocksdb_lib rocksdb snappy_lib)
  set_target_properties (rocksdb_lib PROPERTIES
    IMPORTED_LOCATION ${install_path}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}rocksdb${CMAKE_STATIC_LIBRARY_SUFFIX}
    INTERFACE_INCLUDE_DIRECTORIES ${install_path}/include
    INTERFACE_COMPILE_DEFINITIONS RIPPLE_ROCKSDB_AVAILABLE=1
    INTERFACE_COMPILE_OPTIONS $<$<BOOL:${is_clang}>:--system-header-prefix=rocksdb>)
  file (MAKE_DIRECTORY ${install_path}/include)
else()
  add_library (rocksdb_lib INTERFACE)
  target_compile_definitions (rocksdb_lib
    INTERFACE RIPPLE_ROCKSDB_AVAILABLE=0)
endif()
if (${CMAKE_VERSION} VERSION_GREATER_EQUAL 3.11)
  add_library (NIH::rocksdb ALIAS rocksdb_lib)
  list (APPEND RIPPLED_NIH_LIBRARIES NIH::rocksdb)
else ()
  list (APPEND RIPPLED_NIH_LIBRARIES rocksdb_lib)
endif ()

######################################################################
###   NIH dep: nudb                                                ###
######################################################################

add_library (nudb INTERFACE)
target_include_directories (nudb
  INTERFACE src/nudb/include)

if(TARGET Boost::thread)
  target_link_libraries(nudb
    INTERFACE
      Boost::thread
      Boost::system)
else ()
  target_link_libraries(nudb
    INTERFACE ${Boost_LIBRARIES})
endif ()
add_library (NIH::nudb ALIAS nudb)
list (APPEND RIPPLED_NIH_LIBRARIES NIH::nudb)

######################################################################
###   NIH dep: libarchive                                          ###
######################################################################

ExternalProject_Add (libarchive
  GIT_REPOSITORY https://github.com/libarchive/libarchive.git
  GIT_TAG v3.3.1
  CMAKE_ARGS
    $<$<BOOL:${CMAKE_VERBOSE_MAKEFILE}>:
      -DCMAKE_VERBOSE_MAKEFILE=ON
    >
    -DCMAKE_INSTALL_PREFIX=_installed
    -DENABLE_LZ4=ON
    -DCMAKE_PREFIX_PATH=${CMAKE_BINARY_DIR}
    -DCMAKE_INCLUDE_PATH=$<TARGET_PROPERTY:lz4,INTERFACE_INCLUDE_DIRECTORIES>
    -DLZ4_INCLUDE_DIR=$<TARGET_PROPERTY:lz4,INTERFACE_INCLUDE_DIRECTORIES>
    -DENABLE_TAR=OFF
    -DENABLE_TAR_SHARED=OFF
    -DENABLE_INSTALL=ON
    -DENABLE_NETTLE=OFF
    -DENABLE_OPENSSL=OFF
    -DENABLE_LZO=OFF
    -DENABLE_LZMA=OFF
    -DENABLE_ZLIB=OFF
    -DENABLE_BZip2=OFF
    -DENABLE_LIBXML2=OFF
    -DENABLE_EXPAT=OFF
    -DENABLE_PCREPOSIX=OFF
    -DENABLE_LibGCC=ON
    -DENABLE_CNG=OFF
    -DENABLE_CPIO=OFF
    -DENABLE_CPIO_SHARED=OFF
    -DENABLE_CAT=OFF
    -DENABLE_CAT_SHARED=OFF
    -DENABLE_XATTR=OFF
    -DENABLE_ACL=OFF
    -DENABLE_ICONV=OFF
    -DENABLE_TEST=OFF
    -DENABLE_COVERAGE=OFF
    $<$<BOOL:${MSVC}>:
      "-DCMAKE_C_FLAGS=-GR -Gd -fp:precise -FS -MP"
      "-DCMAKE_C_FLAGS_DEBUG=-MTd"
      "-DCMAKE_C_FLAGS_RELEASE=-MT"
      "-DCMAKE_C_FLAGS_RELWITHDEBINFO=-MT"
      "-DCMAKE_C_FLAGS_MINSIZEREL=-MT"
    >
  LOG_BUILD ON
  LOG_CONFIGURE ON
  BUILD_COMMAND
    ${CMAKE_COMMAND}
    --build .
    --config $<CONFIG>
    --target archive_static
  TEST_COMMAND ""
  DEPENDS lz4
)
ExternalProject_Get_Property (libarchive BINARY_DIR)
set (install_path ${BINARY_DIR}/_installed)
add_library (archive_lib STATIC IMPORTED GLOBAL)
set_target_properties (archive_lib PROPERTIES
  IMPORTED_LOCATION ${install_path}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}archive${CMAKE_STATIC_LIBRARY_SUFFIX}
  INTERFACE_INCLUDE_DIRECTORIES ${install_path}/include
  INTERFACE_COMPILE_DEFINITIONS LIBARCHIVE_STATIC)
file (MAKE_DIRECTORY ${install_path}/include)
add_dependencies (archive_lib libarchive)
target_link_libraries(archive_lib INTERFACE NIH::lz4)
if (${CMAKE_VERSION} VERSION_GREATER_EQUAL 3.11)
  add_library (NIH::libarchive ALIAS archive_lib)
  list (APPEND RIPPLED_NIH_LIBRARIES NIH::libarchive)
else ()
  list (APPEND RIPPLED_NIH_LIBRARIES archive_lib)
endif()


set(RIPPLED_LIBRARIES)
######################################################################
### create a lib to separate/isolate the "old" beast code          ###
### TODO: continue to review these sources for removal             ###
###       or replacement                                           ###
######################################################################

file(GLOB_RECURSE rb_headers
  src/ripple/beast/*.h
  src/ripple/beast/*.hpp)

add_library (ripple_beast STATIC
  $<$<BOOL:${unity}>:
      src/ripple/beast/core/core.unity.cpp
      src/ripple/beast/unity/beast_hash_unity.cpp
      src/ripple/beast/unity/beast_insight_unity.cpp
      src/ripple/beast/unity/beast_net_unity.cpp
      src/ripple/beast/unity/beast_utility_unity.cpp
  >
  $<$<NOT:$<BOOL:${unity}>>:
      src/ripple/beast/core/CurrentThreadName.cpp
      src/ripple/beast/core/SemanticVersion.cpp
      src/ripple/beast/core/WaitableEvent.cpp
      src/ripple/beast/hash/impl/xxhash.cpp
      src/ripple/beast/insight/impl/Collector.cpp
      src/ripple/beast/insight/impl/Group.cpp
      src/ripple/beast/insight/impl/Groups.cpp
      src/ripple/beast/insight/impl/Hook.cpp
      src/ripple/beast/insight/impl/Metric.cpp
      src/ripple/beast/insight/impl/NullCollector.cpp
      src/ripple/beast/insight/impl/StatsDCollector.cpp
      src/ripple/beast/net/impl/IPAddressConversion.cpp
      src/ripple/beast/net/impl/IPAddressV4.cpp
      src/ripple/beast/net/impl/IPAddressV6.cpp
      src/ripple/beast/net/impl/IPEndpoint.cpp
      src/ripple/beast/utility/src/beast_Debug.cpp
      src/ripple/beast/utility/src/beast_Journal.cpp
      src/ripple/beast/utility/src/beast_PropertyStream.cpp
  >
  ${rb_headers})

target_include_directories (ripple_beast PUBLIC src/ripple)
# express dependency of this lib on boost headers (asio and optional)
target_link_libraries (ripple_beast PUBLIC Boost::boost)
add_library (ripple::beast ALIAS ripple_beast)
list (APPEND RIPPLED_LIBRARIES ripple::beast)

######################################################################
### import protobuf (lib and compiler) and create a lib            ###
### from our proto message definitions.                            ###
######################################################################

if (static)
  set (Protobuf_USE_STATIC_LIBS ON)
  endif()
find_package (Protobuf REQUIRED)
protobuf_generate_cpp (PROTO_SRCS PROTO_HDRS
  src/ripple/proto/ripple.proto)
add_library (ripple_protos STATIC
  ${PROTO_SRCS}
  ${PROTO_HDRS})
target_include_directories (ripple_protos
    PUBLIC ${CMAKE_BINARY_DIR})
if(TARGET protobuf::libprotobuf)
  target_link_libraries (ripple_protos protobuf::libprotobuf)
else ()
  target_link_libraries (ripple_protos ${Protobuf_LIBRARIES})
endif ()
target_compile_options (ripple_protos
  PUBLIC
    $<$<BOOL:${is_xcode}>:--system-header-prefix="google/protobuf">
    $<$<BOOL:${MSVC}>:
      -wd4251
      -wd4146
    >)
add_library (ripple::protos ALIAS ripple_protos)
list (APPEND RIPPLED_LIBRARIES ripple::protos)

#### <<<<>>>>

############################################################

# Unity sources

prepend(ripple_unity_srcs
src/ripple/unity/
app_consensus.cpp
app_ledger.cpp
app_ledger_impl.cpp
app_main1.cpp
app_main2.cpp
app_misc.cpp
app_misc_impl.cpp
app_paths.cpp
app_tx.cpp
conditions.cpp
consensus.cpp
core.cpp
basics.cpp
crypto.cpp
ledger.cpp
net.cpp
nodestore.cpp
overlay1.cpp
overlay2.cpp
peerfinder.cpp
json.cpp
protocol.cpp
resource.cpp
rpcx1.cpp
rpcx2.cpp
shamap.cpp
server.cpp)

prepend(test_unity_srcs
src/test/unity/
app_test_unity1.cpp
app_test_unity2.cpp
basics_test_unity.cpp
beast_test_unity1.cpp
beast_test_unity2.cpp
conditions_test_unity.cpp
consensus_test_unity.cpp
core_test_unity.cpp
json_test_unity.cpp
ledger_test_unity.cpp
nodestore_test_unity.cpp
overlay_test_unity.cpp
peerfinder_test_unity.cpp
protocol_test_unity.cpp
resource_test_unity.cpp
rpc_test_unity.cpp
server_test_unity.cpp
server_status_test_unity.cpp
shamap_test_unity.cpp
jtx_unity1.cpp
jtx_unity2.cpp
csf_unity.cpp)

list(APPEND rippled_src_unity ${beast_unity_srcs} ${ripple_unity_srcs} ${test_unity_srcs})

############################################################

# Non-unity sources
file(GLOB_RECURSE core_srcs src/ripple/core/*.cpp)
list(APPEND rippled_src_nonunity ${core_srcs})

foreach(curdir
        app
        basics
        conditions
        crypto
        consensus
        json
        ledger
        legacy
        net
        nodestore
        overlay
        peerfinder
        protocol
        resource
        rpc
        server
        shamap)
    file(GLOB_RECURSE cursrcs src/ripple/${curdir}/*.cpp)
    list(APPEND rippled_src_nonunity "${cursrcs}")
endforeach()

# unit test sources
foreach(curdir
        app
        basics
        beast
        conditions
        consensus
        core
        csf
        json
        jtx
        ledger
        nodestore
        overlay
        peerfinder
        protocol
        resource
        rpc
        server
        shamap
        unit_test)
    file(GLOB_RECURSE cursrcs src/test/${curdir}/*.cpp)
    list(APPEND test_srcs "${cursrcs}")
endforeach()

list(APPEND rippled_src_nonunity "${test_srcs}")

if(is_multiconfig)
    # Rippled headers. Only needed for IDEs.
    file(GLOB_RECURSE rippled_headers src/*.h src/*.hpp *.md)
    list(APPEND rippled_headers Builds/CMake/CMakeFuncs.cmake)
    foreach(curdir
            proto)
        file(GLOB_RECURSE cursrcs src/ripple/${curdir}/*.cpp)
        list(APPEND rippled_headers "${cursrcs}")
    endforeach()
    list(APPEND rippled_src_nonunity "${rippled_headers}")

    set_property(
        SOURCE ${rippled_headers}
        APPEND
        PROPERTY HEADER_FILE_ONLY
        true)
        # Doesn't work
        # $<OR:$<CONFIG:Debug>,$<CONFIG:Release>>)
endif()

if (is_multiconfig)
  # Documentation sources. Only needed for IDEs.
  prepend(doc_srcs
    docs/
    source.dox)

  file(GLOB_RECURSE other_docs docs/*.md)
  list(APPEND doc_srcs "${other_docs}")
  set_property(
    SOURCE ${doc_srcs}
    APPEND
    PROPERTY HEADER_FILE_ONLY
    true)
endif()

############################################################

if (is_gcc)
  set(no_init_w -Wno-maybe-uninitialized)
else()
  unset(no_init_w)
endif()

list(APPEND rippled_src_unity "${rippled_src_all}")
list(APPEND rippled_src_nonunity "${rippled_src_all}")

############################################################

if (WIN32 OR is_xcode)
  group_sources(src)
  group_sources(docs)
  group_sources(Builds)
endif()

if(unity)
  add_executable(rippled ${rippled_src_unity})
  target_link_libraries(rippled)
  add_executable(rippled_classic EXCLUDE_FROM_ALL ${rippled_src_nonunity})
  target_link_libraries(rippled_classic)
  set(other_target rippled_classic)
else()
  add_executable(rippled ${rippled_src_nonunity})
  target_link_libraries(rippled)
  add_executable(rippled_unity EXCLUDE_FROM_ALL ${rippled_src_unity})
  target_link_libraries(rippled_unity)
  set(other_target rippled_unity)
endif()
list(APPEND targets "rippled")
list(APPEND targets ${other_target})
# Not the same as EXCLUDE_FROM_ALL. Prevents Visual Studio from building the
# other_target when the user builds the solution (default when pressing <F7>)
set_property(TARGET ${other_target} PROPERTY EXCLUDE_FROM_DEFAULT_BUILD true)

find_package(Doxygen)
if(TARGET Doxygen::doxygen)
  # read the source config and make a modified one
  # that points the output files to our build directory
  FILE(READ "${CMAKE_SOURCE_DIR}/docs/source.dox" dox_content)
  string(REGEX REPLACE "[\t ]*OUTPUT_DIRECTORY[\t ]*=(.*)"
    "OUTPUT_DIRECTORY=${CMAKE_BINARY_DIR}\n\\1"
    new_config "${dox_content}")
  FILE(WRITE "${CMAKE_BINARY_DIR}/source.dox" "${new_config}")
  add_custom_target(docs
    COMMAND "${DOXYGEN_EXECUTABLE}" "${CMAKE_BINARY_DIR}/source.dox"
    BYPRODUCTS "${CMAKE_BINARY_DIR}/html_doc/index.html"
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/docs"
    SOURCES "${doc_srcs}"
  )
endif()

set_startup_project(rippled)

foreach(target IN LISTS targets)
    target_link_libraries(${target}
        ${RIPPLED_LIBRARIES}
        ${RIPPLED_NIH_LIBRARIES}
        ${OPENSSL_LIBRARIES}
        ${SANITIZER_LIBRARIES})

    link_common_libraries(${target})
endforeach()

if (NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
  message(WARNING "Rippled requires a 64 bit target architecture.\n"
    "The most likely cause of this warning is trying to build rippled with a 32-bit OS.")
endif()
