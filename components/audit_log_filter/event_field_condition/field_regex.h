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
#include <limits>
#include <string>
#include <string_view>

namespace audit_log_filter::event_field_condition {
namespace regex_detail {
// Parse-time only: at most 96 bytes, including a visible truncation marker.
std::string diagnostic_text(std::string_view text);

class WarningLimiter {
 public:
  using Clock = std::chrono::steady_clock;
  bool try_acquire(Clock::time_point now) noexcept;

 private:
  using Tick = Clock::duration::rep;
  static constexpr Tick kNever = std::numeric_limits<Tick>::min();
  std::atomic<Tick> m_last{kNever};
};
}  // namespace regex_detail

class EventFieldConditionRegex : public EventFieldConditionBase {
 public:
  // All allocating preparation is done by the guarded parser.
  EventFieldConditionRegex(std::string name,
                           std::unique_ptr<AuditRegex> pattern,
                           std::string filter_preview,
                           std::string field_preview,
                           std::string pattern_preview) noexcept;
  bool check_applies(
      const AuditRecordFieldsList &fields) const noexcept override;
  ConditionResult check_result(
      const AuditRecordFieldsList &fields) const noexcept override;

 private:
  std::string m_name;
  std::unique_ptr<AuditRegex> m_pattern;
  std::string m_filter_preview;
  std::string m_field_preview;
  std::string m_pattern_preview;
  mutable regex_detail::WarningLimiter m_warning_limiter;
};
}  // namespace audit_log_filter::event_field_condition
#endif  // AUDIT_LOG_FILTER_EVENT_FIELD_CONDITION_FIELD_REGEX_H_INCLUDED
