# Copyright (c) 2026 Percona LLC and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

MYSQL_ADD_EXECUTABLE(audit_regex-t
  audit_regex-t.cc
  ${CMAKE_SOURCE_DIR}/components/audit_log_filter/audit_regex.cc
  ADD_TEST audit_regex
  LINK_LIBRARIES ext::icu gunit_small
)

MYSQL_ADD_EXECUTABLE(audit_field_regex-t
  audit_field_regex-t.cc
  ${CMAKE_SOURCE_DIR}/components/audit_log_filter/audit_regex.cc
  ${CMAKE_SOURCE_DIR}/components/audit_log_filter/audit_record.cc
  ${CMAKE_SOURCE_DIR}/components/audit_log_filter/event_field_condition/field_regex.cc
  ${CMAKE_SOURCE_DIR}/components/audit_log_filter/event_field_condition/and.cc
  ${CMAKE_SOURCE_DIR}/components/audit_log_filter/event_field_condition/or.cc
  ${CMAKE_SOURCE_DIR}/components/audit_log_filter/event_field_condition/not.cc
  ${CMAKE_SOURCE_DIR}/components/audit_log_filter/event_field_condition/bool.cc
  ADD_TEST audit_field_regex
  LINK_LIBRARIES ext::icu gunit_small
)
