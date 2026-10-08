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

#ifndef AUDIT_LOG_FILTER_EVENT_FIELD_CONDITION_FIELD_REGEX_H_INCLUDED
#define AUDIT_LOG_FILTER_EVENT_FIELD_CONDITION_FIELD_REGEX_H_INCLUDED

#include "components/audit_log_filter/audit_regex.h"
#include "components/audit_log_filter/event_field_condition/base.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

namespace audit_log_filter::event_field_condition {

inline constexpr std::size_t kDiagnosticTextMaxBytes = 96;
inline constexpr std::chrono::seconds kRegexWarningInterval{60};

std::string escape_diagnostic_text(std::string_view input);

void inc_regex_match_errors() noexcept;
[[nodiscard]] uint64_t regex_match_error_count() noexcept;

const char *regex_runtime_category_name(RegexFailureCategory category) noexcept;

class RegexWarningLimiter {
 public:
  [[nodiscard]] bool try_acquire(
      std::chrono::steady_clock::time_point now) const noexcept;

 private:
  static constexpr int64_t kNeverWarned = INT64_MIN;
  mutable std::atomic<int64_t> m_last_ns{kNeverWarned};
};

struct RegexRuntimeDiagnostic {
  const char *filter_name;
  const char *pattern_preview;
  const char *field_name;
  const char *category;
  const char *status_name;
};

using RegexWarningEmitter = void (*)(const RegexRuntimeDiagnostic &diagnostic,
                                     void *context) noexcept;

void default_regex_warning_emitter(const RegexRuntimeDiagnostic &diagnostic,
                                   void *context) noexcept;

void note_regex_match_error(const RegexWarningLimiter &limiter,
                            std::chrono::steady_clock::time_point now,
                            const RegexRuntimeDiagnostic &diagnostic,
                            RegexWarningEmitter emit, void *context) noexcept;

class EventFieldConditionRegex : public EventFieldConditionBase {
 public:
  EventFieldConditionRegex(
      std::string field_name, AuditRegex pattern, std::string escaped_filter,
      std::string escaped_field, std::string pattern_preview,
      RegexWarningEmitter emit = &default_regex_warning_emitter,
      void *emit_context = nullptr);

  [[nodiscard]] bool check_applies(
      const AuditRecordFieldsList &fields) const noexcept override;

 private:
  void report(const RegexFailure &failure) const noexcept;

  std::string m_field_name;
  AuditRegex m_pattern;
  std::string m_escaped_filter;
  std::string m_escaped_field;
  std::string m_pattern_preview;
  RegexWarningLimiter m_limiter;
  RegexWarningEmitter m_emit;
  void *m_emit_context;
};

}  // namespace audit_log_filter::event_field_condition

#endif  // AUDIT_LOG_FILTER_EVENT_FIELD_CONDITION_FIELD_REGEX_H_INCLUDED
