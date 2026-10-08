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
#include "components/audit_log_filter/sys_vars.h"
#include "my_dbug.h"

#include <type_traits>

namespace audit_log_filter::event_field_condition {
namespace regex_detail {
std::string diagnostic_text(std::string_view text) {
  constexpr size_t bound = 96;
  constexpr char hex[] = "0123456789ABCDEF";
  std::string result;
  result.reserve(bound);
  size_t truncation_boundary = 0;
  for (size_t pos = 0; pos < text.size();) {
    const auto ch = static_cast<unsigned char>(text[pos]);
    char escaped[6] = {'\\', 'u', '0', '0', hex[ch >> 4], hex[ch & 15]};
    std::string_view part;
    size_t consumed = 1;
    if (ch < 0x20 || ch == 0x7f || ch == '\'') {
      part = {escaped, sizeof(escaped)};
    } else if (ch == '\\') {
      part = "\\\\";
    } else {
      // Inputs are validated JSON/UTF-8. Keep complete code points together.
      if (ch >= 0xf0)
        consumed = 4;
      else if (ch >= 0xe0)
        consumed = 3;
      else if (ch >= 0xc0)
        consumed = 2;
      if (consumed > text.size() - pos) consumed = 1;
      part = text.substr(pos, consumed);
    }
    if (result.size() + part.size() > bound) {
      result.resize(truncation_boundary);
      result += "...";
      break;
    }
    result.append(part);
    if (result.size() <= bound - 3) truncation_boundary = result.size();
    pos += consumed;
  }
  return result;
}

bool WarningLimiter::try_acquire(Clock::time_point now) noexcept {
  const Tick current = now.time_since_epoch().count();
  const Tick interval =
      std::chrono::duration_cast<Clock::duration>(std::chrono::seconds(60))
          .count();
  Tick last = m_last.load(std::memory_order_relaxed);
  do {
    using UnsignedTick = std::make_unsigned_t<Tick>;
    if (last != kNever &&
        (current < last ||
         static_cast<UnsignedTick>(current) - static_cast<UnsignedTick>(last) <
             static_cast<UnsignedTick>(interval)))
      return false;
  } while (
      !m_last.compare_exchange_weak(last, current, std::memory_order_relaxed));
  return true;
}
}  // namespace regex_detail

EventFieldConditionRegex::EventFieldConditionRegex(
    std::string name, std::unique_ptr<AuditRegex> pattern,
    std::string filter_preview, std::string field_preview,
    std::string pattern_preview) noexcept
    : m_name(std::move(name)),
      m_pattern(std::move(pattern)),
      m_filter_preview(std::move(filter_preview)),
      m_field_preview(std::move(field_preview)),
      m_pattern_preview(std::move(pattern_preview)) {}

bool EventFieldConditionRegex::check_applies(
    const AuditRecordFieldsList &fields) const noexcept {
  const auto field = fields.find(m_name);
  if (field == fields.end()) return false;
  const auto *subject = std::get_if<std::string>(&field->second);
  if (subject == nullptr) return false;

  AuditRegex::Error error;
  AuditRegex::Result result;
  bool inject_error = false;
  DBUG_EXECUTE_IF("audit_log_filter_regex_runtime_error",
                  { inject_error = true; });
  if (inject_error) {
    error = AuditRegex::allocation_error();
    result = AuditRegex::Result::Error;
  } else {
    result = m_pattern->match(*subject, error);
  }
  if (result != AuditRegex::Result::Error)
    return result == AuditRegex::Result::Match;

  SysVars::inc_regex_match_errors();
  if (m_warning_limiter.try_acquire(
          regex_detail::WarningLimiter::Clock::now())) {
    // These arguments are precomputed or static. Logging follows the existing
    // component service contract (LogEvent's destructor is noexcept).
    LogComponentErr(WARNING_LEVEL, ER_AUDIT_FILTER_REGEX_MATCH_FAILURE,
                    m_filter_preview.c_str(), m_pattern_preview.c_str(),
                    m_field_preview.c_str(), error.category_name(),
                    error.status_name());
  }
  return false;
}
}  // namespace audit_log_filter::event_field_condition
