/* Copyright (c) 2026 Percona LLC and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include "components/audit_log_filter/event_field_condition/field_regex.h"

#include "components/audit_log_filter/audit_error_log.h"

namespace audit_log_filter::event_field_condition {

void default_regex_warning_emitter(const RegexRuntimeDiagnostic &diagnostic,
                                   void *) noexcept {
  LogComponentErr(WARNING_LEVEL, ER_AUDIT_FILTER_REGEX_MATCH_FAILURE,
                  diagnostic.filter_name, diagnostic.pattern_preview,
                  diagnostic.field_name, diagnostic.category,
                  diagnostic.status_name);
}

}  // namespace audit_log_filter::event_field_condition
