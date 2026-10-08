# Copyright (c) 2026 Percona LLC and/or its affiliates.
# Licensed under the GNU General Public License, version 2.0.

FUNCTION(MYSQL_CHECK_ICU_REGEX_ALLOCATIONS)
  IF(CMAKE_CROSSCOMPILING AND NOT CMAKE_CROSSCOMPILING_EMULATOR)
    MESSAGE(FATAL_ERROR
      "Audit regex requires ICU allocation-failure fixes. Use WITH_ICU=bundled "
      "or set CMAKE_CROSSCOMPILING_EMULATOR to verify external ICU.")
  ENDIF()
  GET_TARGET_PROPERTY(_icu_libs ext::icu INTERFACE_LINK_LIBRARIES)
  GET_TARGET_PROPERTY(_icu_includes ext::icu INTERFACE_INCLUDE_DIRECTORIES)
  IF(NOT _icu_includes)
    SET(_icu_includes "")
  ENDIF()
  SET(_probe "${CMAKE_BINARY_DIR}/CMakeFiles/icu_regex_allocation_check${CMAKE_EXECUTABLE_SUFFIX}")
  SET(_definitions)
  IF(WIN32)
    LIST(APPEND _definitions -DU_STATIC_IMPLEMENTATION)
  ENDIF()
  TRY_COMPILE(_compiled "${CMAKE_BINARY_DIR}/CMakeFiles/icu_regex_check"
    SOURCES
      "${CMAKE_SOURCE_DIR}/cmake/icu_regex_allocation_check.cc"
      "${CMAKE_SOURCE_DIR}/components/audit_log_filter/audit_regex.cc"
    CMAKE_FLAGS
      "-DCMAKE_CXX_STANDARD=17"
      "-DCMAKE_CXX_COMPILER_LAUNCHER:STRING=${CMAKE_CXX_COMPILER_LAUNCHER}"
      "-DINCLUDE_DIRECTORIES:STRING=${CMAKE_SOURCE_DIR};${_icu_includes}"
    COMPILE_DEFINITIONS ${_definitions}
    LINK_LIBRARIES ${_icu_libs}
    COPY_FILE "${_probe}"
    OUTPUT_VARIABLE _output)
  IF(NOT _compiled)
    MESSAGE(FATAL_ERROR "Cannot build ICU allocation check: ${_output}")
  ENDIF()
  FOREACH(_operation compile match)
    SET(_complete FALSE)
    FOREACH(_allocation RANGE 0 255)
      EXECUTE_PROCESS(
        COMMAND ${CMAKE_CROSSCOMPILING_EMULATOR} "${_probe}" ${_operation} ${_allocation}
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
        RESULT_VARIABLE _result OUTPUT_QUIET ERROR_QUIET TIMEOUT 10)
      IF("${_result}" STREQUAL "77")
        SET(_complete TRUE)
        BREAK()
      ELSEIF(NOT "${_result}" STREQUAL "0")
        MESSAGE(FATAL_ERROR
          "External ICU failed the audit regex ${_operation} allocation check "
          "at allocation ${_allocation} (${_result}). Use WITH_ICU=bundled "
          "or an ICU package with the RegexPattern, RegexCompile and UText "
          "allocation-failure fixes carried in extra/icu.")
      ENDIF()
    ENDFOREACH()
    IF(NOT _complete)
      MESSAGE(FATAL_ERROR "ICU allocation check did not complete; use WITH_ICU=bundled")
    ENDIF()
  ENDFOREACH()
ENDFUNCTION()
