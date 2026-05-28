add_library(quill_mlir STATIC
  QuillCAPI.cpp
  QuillDialect.cpp
  QuillPasses.cpp
)

add_dependencies(quill_mlir MLIRQuillOpsIncGen)

target_include_directories(quill_mlir
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_BINARY_DIR}/../include
)

install(TARGETS quill_mlir
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
  RUNTIME DESTINATION bin
)
