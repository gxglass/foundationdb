fdb_find_sources(FDBSERVER_SRCS)
list(FILTER FDBSERVER_SRCS EXCLUDE REGEX "^workloads/")
include(AddFdbServerLinkTest)

function(configure_fdbserver_common_includes target_name)
  target_include_directories(${target_name} PRIVATE
    ${CMAKE_SOURCE_DIR}/bindings/c
    ${CMAKE_BINARY_DIR}/bindings/c)
endfunction()

function(configure_fdbserver_target_includes target_name)
  configure_fdbserver_common_includes(${target_name})
  target_include_directories(${target_name} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
  )
endfunction()

if(WITH_ROCKSDB)
  find_package(LZ4 REQUIRED)
  include(CompileRocksDB)
  if(WITH_LIBURING)
    find_package(uring REQUIRED)
  endif()
endif()

add_subdirectory(core)
add_subdirectory(kvstore)
add_subdirectory(logsystem)
add_subdirectory(mocks3)
add_subdirectory(clustercontroller)
add_subdirectory(backupworker)
add_subdirectory(commitproxy)
add_subdirectory(coordinator)
add_subdirectory(datadistributor)
add_subdirectory(consistencyscan)
add_subdirectory(grvproxy)
add_subdirectory(logrouter)
add_subdirectory(ratekeeper)
add_subdirectory(resolver)
add_subdirectory(sequencer)
add_subdirectory(storageserver)
add_subdirectory(tester)
add_subdirectory(tlog)
add_subdirectory(worker)
add_subdirectory(workloads)

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/workloads)

add_flow_target(EXECUTABLE NAME fdbserver SRCS ${FDBSERVER_SRCS})

configure_fdbserver_target_includes(fdbserver)
target_link_libraries(fdbserver PRIVATE
  fdbserver_mocks3
  "$<LINK_LIBRARY:WHOLE_ARCHIVE,fdbserver_workloads>"
  fdbserver_worker
  fdbserver_backupworker
  fdbserver_clustercontroller
  fdbserver_commitproxy
  fdbserver_consistencyscan
  fdbserver_coordinator
  fdbserver_datadistributor
  fdbserver_grvproxy
  fdbserver_kvstore
  fdbserver_logrouter
  fdbserver_logsystem
  fdbserver_ratekeeper
  fdbserver_resolver
  fdbserver_sequencer
  fdbserver_storageserver
  fdbserver_tester
  fdbserver_tlog
  fdbserver_core)
if (WITH_ROCKSDB)
  add_dependencies(fdbserver rocksdb)
  if(WITH_LIBURING)
    target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR} ${uring_INCLUDE_DIR})
    target_link_libraries(fdbserver PRIVATE fdbclient sqlite ${ROCKSDB_LIBRARIES} ${uring_LIBRARIES} ${LZ4_LIBRARY})
    target_compile_definitions(fdbserver PRIVATE BOOST_ASIO_HAS_IO_URING=1 BOOST_ASIO_DISABLE_EPOLL=1)
  else()
    target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(fdbserver PRIVATE fdbclient sqlite ${ROCKSDB_LIBRARIES} ${LZ4_LIBRARY})
  endif()
else()
  target_link_libraries(fdbserver PRIVATE fdbclient sqlite)
endif()

if(USE_JEMALLOC)
  target_include_directories(fdbserver INTERFACE $<TARGET_PROPERTY:jemalloc::jemalloc, INTERFACE_INCLUDE_DIRECTORIES>)
  target_link_libraries(fdbserver PRIVATE jemalloc::jemalloc)
endif()

target_link_libraries(fdbserver PRIVATE toml11::toml11 rapidjson)
if(WITH_ROCKSDB)
  target_compile_definitions(fdbserver PRIVATE WITH_ROCKSDB)
endif()

if(GPERFTOOLS_FOUND)
  target_link_libraries(fdbserver PRIVATE gperftools)
endif()

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbserver DESTINATION sbin COMPONENT server)
  else()
    add_custom_target(prepare_fdbserver_install ALL DEPENDS strip_only_fdbserver)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbserver DESTINATION sbin COMPONENT server)
  endif()
endif()

target_link_libraries(fdbserver PUBLIC fdbctl)
