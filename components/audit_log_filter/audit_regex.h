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

#ifndef AUDIT_LOG_FILTER_AUDIT_REGEX_H_INCLUDED
#define AUDIT_LOG_FILTER_AUDIT_REGEX_H_INCLUDED

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace audit_log_filter {

inline constexpr std::size_t kRegexMaxPatternBytes = 16384;
inline constexpr int32_t kRegexDefaultTimeLimit = 32;
inline constexpr int32_t kRegexDefaultStackLimit = 8000000;

enum class RegexMatchResult { Match, NoMatch, Error };

enum class RegexFailureCategory {
  None,
  Size,
  Encoding,
  Allocation,
  Syntax,
  Timeout,
  Stack,
  Engine
};

struct RegexFailure {
  RegexFailureCategory category{RegexFailureCategory::None};
  int32_t status{0};
  int32_t line{0};
  int32_t offset{-1};
  bool has_position{false};
};

struct RegexLimits {
  int32_t time_limit{kRegexDefaultTimeLimit};
  int32_t stack_limit{kRegexDefaultStackLimit};
};

const char *icu_error_name(int32_t status) noexcept;

/**
  Owning ICU pattern. Matchers are created per evaluation and are not shared.
  ICU headers stay in the implementation.
*/
class AuditRegex {
 public:
  struct MatchOutcome {
    RegexMatchResult result{RegexMatchResult::Error};
    RegexFailure failure{};
  };

  explicit AuditRegex(std::string_view pattern);
  AuditRegex(std::string_view pattern, RegexLimits limits);
  ~AuditRegex();

  AuditRegex(AuditRegex &&other) noexcept;
  AuditRegex &operator=(AuditRegex &&other) noexcept;
  AuditRegex(const AuditRegex &) = delete;
  AuditRegex &operator=(const AuditRegex &) = delete;

  [[nodiscard]] bool valid() const noexcept;
  [[nodiscard]] const RegexFailure &failure() const noexcept;
  [[nodiscard]] MatchOutcome find(std::string_view subject) const;

 private:
  struct Impl;
  std::unique_ptr<Impl> m_impl;
};

}  // namespace audit_log_filter

#endif  // AUDIT_LOG_FILTER_AUDIT_REGEX_H_INCLUDED
