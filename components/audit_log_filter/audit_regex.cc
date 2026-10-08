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

#include "components/audit_log_filter/audit_regex.h"

#include <unicode/regex.h>
#include <unicode/unistr.h>
#include <unicode/ustring.h>
#include <unicode/utext.h>
#include <unicode/utypes.h>

#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <utility>

namespace audit_log_filter {
namespace {

RegexFailure make_failure(RegexFailureCategory category, UErrorCode status,
                          const UParseError *parse_error, bool with_position) {
  RegexFailure failure;
  failure.category = category;
  failure.status = static_cast<int32_t>(status);
  if (with_position && parse_error != nullptr && parse_error->offset >= 0) {
    failure.has_position = true;
    failure.line = parse_error->line;
    failure.offset = parse_error->offset;
  }
  return failure;
}

bool is_syntax_status(UErrorCode status) {
  switch (status) {
    case U_REGEX_RULE_SYNTAX:
    case U_REGEX_BAD_ESCAPE_SEQUENCE:
    case U_REGEX_PROPERTY_SYNTAX:
    case U_REGEX_MISMATCHED_PAREN:
    case U_REGEX_NUMBER_TOO_BIG:
    case U_REGEX_BAD_INTERVAL:
    case U_REGEX_MAX_LT_MIN:
    case U_REGEX_INVALID_BACK_REF:
    case U_REGEX_INVALID_FLAG:
    case U_REGEX_LOOK_BEHIND_LIMIT:
    case U_REGEX_SET_CONTAINS_STRING:
    case U_REGEX_OCTAL_TOO_BIG:
    case U_REGEX_MISSING_CLOSE_BRACKET:
    case U_REGEX_INVALID_RANGE:
    case U_REGEX_INVALID_CAPTURE_GROUP_NAME:
      return true;
    default:
      return false;
  }
}

bool is_encoding_status(UErrorCode status) {
  return status == U_INVALID_CHAR_FOUND || status == U_TRUNCATED_CHAR_FOUND ||
         status == U_ILLEGAL_CHAR_FOUND;
}

RegexFailure map_compile_status(UErrorCode status,
                                const UParseError &parse_error,
                                bool null_pattern) {
  if ((null_pattern && U_SUCCESS(status)) ||
      status == U_MEMORY_ALLOCATION_ERROR) {
    return make_failure(RegexFailureCategory::Allocation,
                        U_MEMORY_ALLOCATION_ERROR, nullptr, false);
  }
  if (is_syntax_status(status)) {
    return make_failure(RegexFailureCategory::Syntax, status, &parse_error,
                        true);
  }
  return make_failure(RegexFailureCategory::Engine, status, &parse_error, true);
}

RegexFailure map_conversion_status(UErrorCode status) {
  if (status == U_MEMORY_ALLOCATION_ERROR) {
    return make_failure(RegexFailureCategory::Allocation, status, nullptr,
                        false);
  }
  if (is_encoding_status(status)) {
    return make_failure(RegexFailureCategory::Encoding, status, nullptr, false);
  }
  return make_failure(RegexFailureCategory::Engine, status, nullptr, false);
}

RegexFailure map_runtime_status(UErrorCode status) {
  if (status == U_REGEX_TIME_OUT) {
    return make_failure(RegexFailureCategory::Timeout, status, nullptr, false);
  }
  if (status == U_REGEX_STACK_OVERFLOW) {
    return make_failure(RegexFailureCategory::Stack, status, nullptr, false);
  }
  if (status == U_MEMORY_ALLOCATION_ERROR) {
    return make_failure(RegexFailureCategory::Allocation, status, nullptr,
                        false);
  }
  return make_failure(RegexFailureCategory::Engine, status, nullptr, false);
}

struct UTextClose {
  void operator()(UText *text) const noexcept {
    if (text != nullptr) {
      utext_close(text);
    }
  }
};

using UTextPtr = std::unique_ptr<UText, UTextClose>;

}  // namespace

struct AuditRegex::Impl {
  std::unique_ptr<icu::RegexPattern> pattern;
  RegexLimits limits{};
  RegexFailure failure{};
  bool valid{false};

  void fail(RegexFailure error) {
    pattern.reset();
    failure = error;
    valid = false;
  }

  void compile_owned(const icu::UnicodeString &owned) {
    UParseError parse_error;
    std::memset(&parse_error, 0, sizeof(parse_error));
    parse_error.line = 0;
    parse_error.offset = -1;

    UErrorCode status = U_ZERO_ERROR;
    std::unique_ptr<icu::RegexPattern> compiled(
        icu::RegexPattern::compile(owned, 0, parse_error, status));
    if (U_FAILURE(status) || compiled == nullptr) {
      fail(map_compile_status(status, parse_error, compiled == nullptr));
      return;
    }
    pattern = std::move(compiled);
    failure = {};
    valid = true;
  }

  void compile_pattern(std::string_view pattern_text) {
    if (pattern_text.size() >
            static_cast<std::size_t>(std::numeric_limits<int32_t>::max()) ||
        pattern_text.size() > kRegexMaxPatternBytes) {
      fail(make_failure(RegexFailureCategory::Size, U_ILLEGAL_ARGUMENT_ERROR,
                        nullptr, false));
      return;
    }

    if (pattern_text.empty()) {
      icu::UnicodeString empty;
      if (empty.isBogus()) {
        fail(make_failure(RegexFailureCategory::Allocation,
                          U_MEMORY_ALLOCATION_ERROR, nullptr, false));
        return;
      }
      compile_owned(empty);
      return;
    }

    const auto src_len = static_cast<int32_t>(pattern_text.size());
    UErrorCode status = U_ZERO_ERROR;
    int32_t dest_len = 0;
    u_strFromUTF8(nullptr, 0, &dest_len, pattern_text.data(), src_len, &status);
    if (status != U_BUFFER_OVERFLOW_ERROR) {
      fail(map_conversion_status(status));
      return;
    }
    status = U_ZERO_ERROR;
    if (dest_len < 0 || dest_len == std::numeric_limits<int32_t>::max()) {
      fail(make_failure(RegexFailureCategory::Allocation,
                        U_MEMORY_ALLOCATION_ERROR, nullptr, false));
      return;
    }

    std::unique_ptr<UChar[]> converted(
        new UChar[static_cast<std::size_t>(dest_len) + 1]);
    int32_t written = 0;
    u_strFromUTF8(converted.get(), dest_len + 1, &written, pattern_text.data(),
                  src_len, &status);
    if (status != U_ZERO_ERROR) {
      fail(map_conversion_status(status));
      return;
    }

    icu::UnicodeString owned(converted.get(), written);
    if (owned.isBogus()) {
      fail(make_failure(RegexFailureCategory::Allocation,
                        U_MEMORY_ALLOCATION_ERROR, nullptr, false));
      return;
    }
    compile_owned(owned);
  }
};

const char *icu_error_name(int32_t status) noexcept {
  return u_errorName(static_cast<UErrorCode>(status));
}

AuditRegex::AuditRegex(std::string_view pattern)
    : AuditRegex(pattern, RegexLimits{}) {}

AuditRegex::AuditRegex(std::string_view pattern, RegexLimits limits)
    : m_impl(std::make_unique<Impl>()) {
  m_impl->limits = limits;
  try {
    m_impl->compile_pattern(pattern);
  } catch (const std::bad_alloc &) {
    m_impl->fail(make_failure(RegexFailureCategory::Allocation,
                              U_MEMORY_ALLOCATION_ERROR, nullptr, false));
  }
}

AuditRegex::~AuditRegex() = default;

AuditRegex::AuditRegex(AuditRegex &&other) noexcept = default;

AuditRegex &AuditRegex::operator=(AuditRegex &&other) noexcept = default;

bool AuditRegex::valid() const noexcept {
  return m_impl != nullptr && m_impl->valid;
}

const RegexFailure &AuditRegex::failure() const noexcept {
  static const RegexFailure kEmpty{};
  return m_impl == nullptr ? kEmpty : m_impl->failure;
}

AuditRegex::MatchOutcome AuditRegex::find(std::string_view subject) const {
  MatchOutcome outcome;
  if (m_impl == nullptr || !m_impl->valid || m_impl->pattern == nullptr) {
    outcome.result = RegexMatchResult::Error;
    outcome.failure = m_impl == nullptr
                          ? make_failure(RegexFailureCategory::Engine,
                                         U_INVALID_STATE_ERROR, nullptr, false)
                          : m_impl->failure;
    if (outcome.failure.category == RegexFailureCategory::None) {
      outcome.failure = make_failure(RegexFailureCategory::Engine,
                                     U_INVALID_STATE_ERROR, nullptr, false);
    }
    return outcome;
  }

  try {
    const char *bytes = subject.data();
    int64_t length = static_cast<int64_t>(subject.size());
    if (bytes == nullptr) {
      bytes = "";
      length = 0;
    }
    if (subject.size() >
        static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
      outcome.result = RegexMatchResult::Error;
      outcome.failure = make_failure(RegexFailureCategory::Engine,
                                     U_ILLEGAL_ARGUMENT_ERROR, nullptr, false);
      return outcome;
    }

    UErrorCode status = U_ZERO_ERROR;
    UTextPtr text(utext_openUTF8(nullptr, bytes, length, &status));
    if (U_FAILURE(status) || text == nullptr) {
      outcome.result = RegexMatchResult::Error;
      outcome.failure = map_runtime_status(
          U_FAILURE(status) ? status : U_MEMORY_ALLOCATION_ERROR);
      if (text == nullptr && U_SUCCESS(status)) {
        outcome.failure =
            make_failure(RegexFailureCategory::Allocation,
                         U_MEMORY_ALLOCATION_ERROR, nullptr, false);
      }
      return outcome;
    }

    status = U_ZERO_ERROR;
    std::unique_ptr<icu::RegexMatcher> matcher(
        m_impl->pattern->matcher(status));
    if (U_FAILURE(status) || matcher == nullptr) {
      outcome.result = RegexMatchResult::Error;
      outcome.failure = map_runtime_status(
          matcher == nullptr && U_SUCCESS(status) ? U_MEMORY_ALLOCATION_ERROR
                                                  : status);
      if (matcher == nullptr && U_SUCCESS(status)) {
        outcome.failure =
            make_failure(RegexFailureCategory::Allocation,
                         U_MEMORY_ALLOCATION_ERROR, nullptr, false);
      }
      return outcome;
    }

    matcher->reset(text.get());
    status = U_ZERO_ERROR;
    matcher->setTimeLimit(m_impl->limits.time_limit, status);
    if (U_FAILURE(status)) {
      outcome.result = RegexMatchResult::Error;
      outcome.failure = map_runtime_status(status);
      return outcome;
    }
    matcher->setStackLimit(m_impl->limits.stack_limit, status);
    if (U_FAILURE(status)) {
      outcome.result = RegexMatchResult::Error;
      outcome.failure = map_runtime_status(status);
      return outcome;
    }
    // setStackLimit() clears the current input. Restore the subject so the
    // following find() observes the caller's text and any deferred reset error.
    matcher->reset(text.get());

    status = U_ZERO_ERROR;
    const UBool found = matcher->find(status);
    if (U_FAILURE(status)) {
      outcome.result = RegexMatchResult::Error;
      outcome.failure = map_runtime_status(status);
      return outcome;
    }
    outcome.result =
        found ? RegexMatchResult::Match : RegexMatchResult::NoMatch;
    return outcome;
  } catch (const std::bad_alloc &) {
    outcome.result = RegexMatchResult::Error;
    outcome.failure = make_failure(RegexFailureCategory::Allocation,
                                   U_MEMORY_ALLOCATION_ERROR, nullptr, false);
    return outcome;
  } catch (...) {
    outcome.result = RegexMatchResult::Error;
    outcome.failure = make_failure(RegexFailureCategory::Engine,
                                   U_REGEX_INTERNAL_ERROR, nullptr, false);
    return outcome;
  }
}

}  // namespace audit_log_filter
