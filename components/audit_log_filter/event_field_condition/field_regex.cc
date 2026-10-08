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

#include "my_dbug.h"

#include <unicode/utypes.h>

#include <atomic>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

namespace audit_log_filter::event_field_condition {
namespace {

std::atomic<uint64_t> g_regex_match_errors{0};

std::size_t utf8_unit_length(std::string_view input, std::size_t offset) {
  if (offset >= input.size()) {
    return 0;
  }
  const auto lead = static_cast<unsigned char>(input[offset]);
  std::size_t need = 1;
  if (lead < 0x80) {
    need = 1;
  } else if ((lead & 0xE0) == 0xC0) {
    need = 2;
  } else if ((lead & 0xF0) == 0xE0) {
    need = 3;
  } else if ((lead & 0xF8) == 0xF0) {
    need = 4;
  } else {
    return 0;
  }
  if (offset + need > input.size()) {
    return 0;
  }
  for (std::size_t i = 1; i < need; ++i) {
    if ((static_cast<unsigned char>(input[offset + i]) & 0xC0) != 0x80) {
      return 0;
    }
  }
  uint32_t code_point = 0;
  if (need == 1) {
    return 1;
  }
  if (need == 2) {
    code_point = (lead & 0x1FU) << 6U;
    code_point |= static_cast<unsigned char>(input[offset + 1]) & 0x3FU;
    if (code_point < 0x80U) {
      return 0;
    }
  } else if (need == 3) {
    code_point = (lead & 0x0FU) << 12U;
    code_point |= (static_cast<unsigned char>(input[offset + 1]) & 0x3FU) << 6U;
    code_point |= static_cast<unsigned char>(input[offset + 2]) & 0x3FU;
    if (code_point < 0x800U ||
        (code_point >= 0xD800U && code_point <= 0xDFFFU)) {
      return 0;
    }
  } else {
    code_point = (lead & 0x07U) << 18U;
    code_point |= (static_cast<unsigned char>(input[offset + 1]) & 0x3FU)
                  << 12U;
    code_point |= (static_cast<unsigned char>(input[offset + 2]) & 0x3FU) << 6U;
    code_point |= static_cast<unsigned char>(input[offset + 3]) & 0x3FU;
    if (code_point < 0x10000U || code_point > 0x10FFFFU) {
      return 0;
    }
  }
  return need;
}

void append_hex_escape(std::string *out, unsigned char byte) {
  char buffer[8];
  std::snprintf(buffer, sizeof(buffer), "\\u%04X", byte);
  out->append(buffer);
}

void append_invalid_byte(std::string *out, unsigned char byte) {
  char buffer[8];
  std::snprintf(buffer, sizeof(buffer), "\\x%02X", byte);
  out->append(buffer);
}

}  // namespace

std::string escape_diagnostic_text(std::string_view input) {
  std::string escaped;
  escaped.reserve(input.size());
  std::vector<std::size_t> boundaries;
  boundaries.push_back(0);

  for (std::size_t i = 0; i < input.size();) {
    const auto byte = static_cast<unsigned char>(input[i]);
    const auto start = escaped.size();
    if (byte == '\\') {
      escaped.append("\\\\");
      i += 1;
    } else if (byte == '\'') {
      escaped.append("\\u0027");
      i += 1;
    } else if (byte <= 0x1FU || byte == 0x7FU) {
      append_hex_escape(&escaped, byte);
      i += 1;
    } else {
      const auto unit = utf8_unit_length(input, i);
      if (unit == 0) {
        append_invalid_byte(&escaped, byte);
        i += 1;
      } else {
        escaped.append(input.data() + i, unit);
        i += unit;
      }
    }
    if (escaped.size() == start) {
      break;
    }
    boundaries.push_back(escaped.size());
  }

  if (escaped.size() <= kDiagnosticTextMaxBytes) {
    return escaped;
  }

  constexpr std::size_t kEllipsis = 3;
  const std::size_t budget = kDiagnosticTextMaxBytes - kEllipsis;
  std::size_t keep = 0;
  for (const auto boundary : boundaries) {
    if (boundary <= budget) {
      keep = boundary;
    } else {
      break;
    }
  }
  escaped.resize(keep);
  escaped.append("...");
  return escaped;
}

void inc_regex_match_errors() noexcept {
  g_regex_match_errors.fetch_add(1, std::memory_order_relaxed);
}

uint64_t regex_match_error_count() noexcept {
  return g_regex_match_errors.load(std::memory_order_relaxed);
}

const char *regex_runtime_category_name(
    RegexFailureCategory category) noexcept {
  switch (category) {
    case RegexFailureCategory::Timeout:
      return "timeout";
    case RegexFailureCategory::Stack:
      return "stack";
    case RegexFailureCategory::Allocation:
      return "allocation";
    default:
      return "engine";
  }
}

bool RegexWarningLimiter::try_acquire(
    std::chrono::steady_clock::time_point now) const noexcept {
  using namespace std::chrono;
  const auto now_ns =
      duration_cast<nanoseconds>(now.time_since_epoch()).count();
  auto observed = m_last_ns.load(std::memory_order_relaxed);
  for (;;) {
    if (observed != kNeverWarned) {
      if (now_ns < observed) {
        return false;
      }
      if (nanoseconds(now_ns - observed) < kRegexWarningInterval) {
        return false;
      }
    }
    if (m_last_ns.compare_exchange_weak(observed, now_ns,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
      return true;
    }
  }
}

void note_regex_match_error(const RegexWarningLimiter &limiter,
                            std::chrono::steady_clock::time_point now,
                            const RegexRuntimeDiagnostic &diagnostic,
                            RegexWarningEmitter emit, void *context) noexcept {
  inc_regex_match_errors();
  if (!limiter.try_acquire(now)) {
    return;
  }
  if (emit == nullptr) {
    return;
  }
  try {
    emit(diagnostic, context);
  } catch (...) {
  }
}

EventFieldConditionRegex::EventFieldConditionRegex(
    std::string field_name, AuditRegex pattern, std::string escaped_filter,
    std::string escaped_field, std::string pattern_preview,
    RegexWarningEmitter emit, void *emit_context)
    : m_field_name{std::move(field_name)},
      m_pattern{std::move(pattern)},
      m_escaped_filter{std::move(escaped_filter)},
      m_escaped_field{std::move(escaped_field)},
      m_pattern_preview{std::move(pattern_preview)},
      m_emit{emit},
      m_emit_context{emit_context} {}

void EventFieldConditionRegex::report(
    const RegexFailure &failure) const noexcept {
  RegexRuntimeDiagnostic diagnostic{
      m_escaped_filter.c_str(), m_pattern_preview.c_str(),
      m_escaped_field.c_str(), regex_runtime_category_name(failure.category),
      icu_error_name(failure.status)};
  note_regex_match_error(m_limiter, std::chrono::steady_clock::now(),
                         diagnostic, m_emit, m_emit_context);
}

bool EventFieldConditionRegex::check_applies(
    const AuditRecordFieldsList &fields) const noexcept {
  try {
    const auto field = fields.find(m_field_name);
    if (field == fields.cend()) {
      return false;
    }
    const auto *text = std::get_if<std::string>(&field->second);
    if (text == nullptr) {
      return false;
    }

    bool injected = false;
    DBUG_EXECUTE_IF("audit_log_filter_regex_runtime_error", injected = true;);

    AuditRegex::MatchOutcome outcome;
    if (injected) {
      outcome.result = RegexMatchResult::Error;
      outcome.failure.category = RegexFailureCategory::Allocation;
      outcome.failure.status = static_cast<int32_t>(U_MEMORY_ALLOCATION_ERROR);
    } else {
      outcome = m_pattern.find(*text);
    }

    if (outcome.result == RegexMatchResult::Error) {
      report(outcome.failure);
      return false;
    }
    return outcome.result == RegexMatchResult::Match;
  } catch (const std::bad_alloc &) {
    RegexFailure failure;
    failure.category = RegexFailureCategory::Allocation;
    failure.status = static_cast<int32_t>(U_MEMORY_ALLOCATION_ERROR);
    report(failure);
    return false;
  } catch (...) {
    RegexFailure failure;
    failure.category = RegexFailureCategory::Engine;
    failure.status = static_cast<int32_t>(U_REGEX_INTERNAL_ERROR);
    report(failure);
    return false;
  }
}

}  // namespace audit_log_filter::event_field_condition
