list(APPEND INCLUDE_DIRS ${CASS_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

#------------------------------
# Targets
#------------------------------

macro(create_symlink source_file symlink_name)
  add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/${symlink_name}
    COMMAND ${CMAKE_COMMAND} -E create_symlink ${source_file} ${CMAKE_BINARY_DIR}/${symlink_name}
  )
  add_custom_target(${symlink_name}_symlink ALL DEPENDS ${CMAKE_BINARY_DIR}/${symlink_name})
endmacro()

macro(create_copy source_file dest_name)
  add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/${dest_name}
    DEPENDS ${source_file}
    COMMAND ${CMAKE_COMMAND} -E copy ${source_file} ${CMAKE_BINARY_DIR}/${dest_name}
  )
  add_custom_target(${dest_name}_copy ALL DEPENDS ${CMAKE_BINARY_DIR}/${dest_name})
endmacro()

if(DEFINED CMAKE_Rust_FLAGS)
  set(CMAKE_Rust_FLAGS "${CMAKE_Rust_FLAGS} --cfg cpp_rust_unstable")
else()
  set(CMAKE_Rust_FLAGS "--cfg cpp_rust_unstable")
endif()

if(APPLE)
  set(INSTALL_NAME_SHARED "libscylla-cpp-driver.${PROJECT_VERSION_STRING}.dylib")
  set(INSTALL_NAME_SHARED_SYMLINK_VERSION "libscylla-cpp-driver.${PROJECT_VERSION_MAJOR}.dylib")
  set(INSTALL_NAME_SHARED_SYMLINK_NO_VERSION "libscylla-cpp-driver.dylib")
  set(CMAKE_Rust_FLAGS_SONAME "-Clink-arg=-Wl,-install_name,${CMAKE_INSTALL_LIBDIR}/libscylla-cpp-driver.${PROJECT_VERSION_MAJOR}.dylib -Clink-arg=-Wl,-current_version,${PROJECT_VERSION_STRING} -Clink-arg=-Wl,-compatibility_version,${PROJECT_VERSION_MAJOR}")
else()
  set(INSTALL_NAME_SHARED "libscylla-cpp-driver.so.${PROJECT_VERSION_STRING}")
  set(INSTALL_NAME_SHARED_SYMLINK_VERSION "libscylla-cpp-driver.so.${PROJECT_VERSION_MAJOR}")
  set(INSTALL_NAME_SHARED_SYMLINK_NO_VERSION "libscylla-cpp-driver.so")
  set(CMAKE_Rust_FLAGS_SONAME "-Clink-arg=-Wl,-soname=libscylla-cpp-driver.so.${PROJECT_VERSION_MAJOR}")
endif()
set(INSTALL_NAME_STATIC "libscylla-cpp-driver_static.a")
if(DEFINED CMAKE_Rust_FLAGS)
  set(CMAKE_Rust_FLAGS "${CMAKE_Rust_FLAGS} ${CMAKE_Rust_FLAGS_SONAME}")
else()
  set(CMAKE_Rust_FLAGS "${CMAKE_Rust_FLAGS_SONAME}")
endif()
cargo_build(NAME scylla_cpp_driver)
create_copy($<TARGET_FILE:scylla_cpp_driver_shared> ${INSTALL_NAME_SHARED})
add_library(scylla-cpp-driver SHARED IMPORTED GLOBAL)
add_dependencies(scylla-cpp-driver scylla-cpp-driver_target)
set_target_properties(scylla-cpp-driver PROPERTIES IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/${INSTALL_NAME_SHARED})
create_copy($<TARGET_FILE:scylla_cpp_driver> ${INSTALL_NAME_STATIC})
add_library(scylla-cpp-driver_static STATIC IMPORTED GLOBAL)
add_dependencies(scylla-cpp-driver_static scylla-cpp-driver_static_target)
set_target_properties(scylla-cpp-driver_static PROPERTIES IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/${INSTALL_NAME_STATIC})
create_symlink(${INSTALL_NAME_SHARED} ${INSTALL_NAME_SHARED_SYMLINK_VERSION})
create_symlink(${INSTALL_NAME_SHARED} ${INSTALL_NAME_SHARED_SYMLINK_NO_VERSION})

#-------------------------------------
# Installation
#-------------------------------------

# Determine if the library directory needs to be determined
if(NOT DEFINED CMAKE_INSTALL_LIBDIR)
  if ("${CMAKE_SYSTEM_NAME}" MATCHES "Linux" AND ("${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr" OR
    "${CMAKE_INSTALL_PREFIX}" STREQUAL "/usr/local"))
    if(EXISTS "/etc/debian_version")
      set (CMAKE_INSTALL_LIBDIR "lib/${CMAKE_LIBRARY_ARCHITECTURE}")
    elseif(EXISTS "/etc/redhat-release" OR EXISTS "/etc/fedora-release" OR
        EXISTS "/etc/slackware-version" OR EXISTS "/etc/gentoo-release")
      if(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set (CMAKE_INSTALL_LIBDIR "lib64")
      else()
        set (CMAKE_INSTALL_LIBDIR "lib")
      endif()
    else()
      set (CMAKE_INSTALL_LIBDIR "lib")
    endif()
  else()
    set (CMAKE_INSTALL_LIBDIR "lib")
  endif()
endif()

# Create a binary directory executable and DLLs (windows only)
set(INSTALL_DLL_EXE_DIR "bin") # Determine the header install dir
if (CASS_INSTALL_HEADER_IN_SUBDIR)
  if (CASS_INSTALL_HEADER_SUBDIR_NAME)
    # User-specified include sub-dir
    set(INSTALL_HEADER_DIR "include/${CASS_INSTALL_HEADER_SUBDIR_NAME}")
  else()
    # Default subdir location is 'include/cassandra'
    set(INSTALL_HEADER_DIR "include/cassandra")
  endif()
else()
  # Default header install location is 'include'
  set(INSTALL_HEADER_DIR "include")
endif()

if(CASS_INSTALL_PKG_CONFIG)
  find_package(PkgConfig)
  if(PKG_CONFIG_FOUND)
    set(prefix ${CMAKE_INSTALL_PREFIX})
    set(exec_prefix ${CMAKE_INSTALL_PREFIX})
    set(libdir ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR})
    set(includedir ${CMAKE_INSTALL_PREFIX}/${INSTALL_HEADER_DIR})
    set(version ${PROJECT_VERSION_STRING})
  endif()
endif()

# Determine if the header should be installed
if(CASS_INSTALL_HEADER)
  file(GLOB CASS_API_HEADER_FILES ${CASS_INCLUDE_DIR}/*.h)
  install(FILES ${CASS_API_HEADER_FILES} DESTINATION ${INSTALL_HEADER_DIR})
endif()

# Install the dynamic/shared library
if(CASS_BUILD_SHARED)
    install(FILES $<TARGET_FILE:scylla-cpp-driver>
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ)
  install(FILES ${CMAKE_BINARY_DIR}/${INSTALL_NAME_SHARED_SYMLINK_VERSION}
    DESTINATION ${CMAKE_INSTALL_LIBDIR})
  install(FILES ${CMAKE_BINARY_DIR}/${INSTALL_NAME_SHARED_SYMLINK_NO_VERSION}
    DESTINATION ${CMAKE_INSTALL_LIBDIR})
  if(CASS_INSTALL_PKG_CONFIG)
    if(PKG_CONFIG_FOUND)
      configure_file("${CMAKE_CURRENT_SOURCE_DIR}/scylla-cpp-driver.pc.in" "scylla-cpp-driver.pc" @ONLY)
      install(FILES "${CMAKE_CURRENT_BINARY_DIR}/scylla-cpp-driver.pc"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
    endif()
  endif()
endif()

if(CASS_BUILD_STATIC)
    install(FILES $<TARGET_FILE:scylla-cpp-driver_static>
    DESTINATION ${CMAKE_INSTALL_LIBDIR})
  if(CASS_INSTALL_PKG_CONFIG)
    if(PKG_CONFIG_FOUND)
        configure_file("${CMAKE_CURRENT_SOURCE_DIR}/scylla-cpp-driver_static.pc.in" "scylla-cpp-driver_static.pc" @ONLY)
      install(FILES "${CMAKE_CURRENT_BINARY_DIR}/scylla-cpp-driver_static.pc"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig")
    endif()
  endif()
endif()

#-------------------
# Uninstall target
#-------------------

configure_file(
  "${CASS_ROOT_DIR}/cmake_uninstall.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
  IMMEDIATE @ONLY)

add_custom_target(uninstall
  COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
