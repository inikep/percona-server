# Copyright (c) 2026 Percona LLC and/or its affiliates. All rights reserved.
# Licensed under the GNU General Public License, version 2.
INCLUDE_DIRECTORIES(SYSTEM ${GMOCK_INCLUDE_DIRS})
SET(AUDIT_SRC ${CMAKE_SOURCE_DIR}/components/audit_log_filter)
MYSQL_ADD_EXECUTABLE(audit_regex-t audit_regex-t.cc ${AUDIT_SRC}/audit_regex.cc
  ADD_TEST audit_regex LINK_LIBRARIES ext::icu gunit_small)
MYSQL_ADD_EXECUTABLE(audit_field_regex-t audit_field_regex-t.cc
  ${AUDIT_SRC}/audit_regex.cc
  ${AUDIT_SRC}/audit_record.cc
  ${AUDIT_SRC}/event_field_condition/field_regex.cc
  ${AUDIT_SRC}/event_field_condition/and.cc
  ${AUDIT_SRC}/event_field_condition/or.cc
  ${AUDIT_SRC}/event_field_condition/not.cc
  ${AUDIT_SRC}/event_field_condition/bool.cc
  ADD_TEST audit_field_regex LINK_LIBRARIES ext::icu gunit_small)
TARGET_COMPILE_DEFINITIONS(audit_field_regex-t PRIVATE LOG_COMPONENT_TAG="audit_log_filter")

IF(WITH_ICU STREQUAL "bundled")
  # The standalone binary must also find ICU data when run without ctest.
  TARGET_COMPILE_DEFINITIONS(audit_regex-t PRIVATE
    AUDIT_REGEX_TEST_ICU_DATA_DIR="${CMAKE_SOURCE_DIR}/extra/icu")
ENDIF()
