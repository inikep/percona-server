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

#include <gtest/gtest.h>

#include "components/audit_log_filter/audit_regex.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace audit_log_filter {
namespace {

AuditRegex::MatchOutcome match_temporary_pattern(std::string pattern,
                                                 std::string_view subject) {
  AuditRegex compiled{std::move(pattern)};
  return compiled.find(subject);
}

TEST(AuditRegex, EmptyPatternCompilesAndMatchesEverywhere) {
  AuditRegex compiled{""};
  EXPECT_TRUE(compiled.valid());
  EXPECT_EQ(RegexMatchResult::Match, compiled.find("").result);
  EXPECT_EQ(RegexMatchResult::Match, compiled.find("orders").result);
}

TEST(AuditRegex, SizeBoundary) {
  const std::string at_limit(kRegexMaxPatternBytes, 'a');
  AuditRegex allowed{at_limit};
  EXPECT_TRUE(allowed.valid());
  EXPECT_EQ(RegexMatchResult::Match, allowed.find(at_limit).result);

  const std::string over(kRegexMaxPatternBytes + 1, 'a');
  AuditRegex rejected{over};
  EXPECT_FALSE(rejected.valid());
  EXPECT_EQ(RegexFailureCategory::Size, rejected.failure().category);
  EXPECT_FALSE(rejected.failure().has_position);
}

TEST(AuditRegex, RejectsMalformedUtf8Pattern) {
  const std::string bad("\xFF", 1);
  AuditRegex compiled{bad};
  EXPECT_FALSE(compiled.valid());
  EXPECT_EQ(RegexFailureCategory::Encoding, compiled.failure().category);
  EXPECT_FALSE(compiled.failure().has_position);
}

TEST(AuditRegex, CompilePositionsAreIcuCharacterPositions) {
  auto expect_pos = [](std::string pattern, int32_t line, int32_t offset,
                       const char *status_name) {
    AuditRegex compiled{std::move(pattern)};
    ASSERT_FALSE(compiled.valid());
    EXPECT_EQ(RegexFailureCategory::Syntax, compiled.failure().category);
    EXPECT_TRUE(compiled.failure().has_position);
    EXPECT_EQ(line, compiled.failure().line);
    EXPECT_EQ(offset, compiled.failure().offset);
    EXPECT_STREQ(status_name, icu_error_name(compiled.failure().status));
  };

  expect_pos("a(", 1, 2, "U_REGEX_MISMATCHED_PAREN");
  expect_pos("😀(", 1, 2, "U_REGEX_MISMATCHED_PAREN");
  expect_pos(std::string("a\n") + "😀(", 2, 2, "U_REGEX_MISMATCHED_PAREN");
  expect_pos("[z-a]", 1, 4, "U_REGEX_INVALID_RANGE");
  expect_pos("a{2,1}", 1, 6, "U_REGEX_MAX_LT_MIN");
  expect_pos("^(orders", 1, 8, "U_REGEX_MISMATCHED_PAREN");
}

TEST(AuditRegex, OwnedPatternSurvivesTemporaryInput) {
  const auto outcome = match_temporary_pattern("orders[0-9]+", "new_orders1");
  EXPECT_EQ(RegexMatchResult::Match, outcome.result);
}

TEST(AuditRegex, EmbeddedNulAndExplicitLength) {
  const std::string pattern("a\0b", 3);
  AuditRegex compiled{pattern};
  ASSERT_TRUE(compiled.valid());
  const std::string subject("xa\0by", 5);
  EXPECT_EQ(RegexMatchResult::Match, compiled.find(subject).result);
  EXPECT_EQ(RegexMatchResult::NoMatch, compiled.find("ab").result);
  EXPECT_EQ(RegexMatchResult::NoMatch,
            compiled.find(std::string_view("a", 1)).result);

  char raw[] = {'a', 'b', 'c', 'X'};
  AuditRegex prefix{"ab"};
  EXPECT_EQ(RegexMatchResult::Match,
            prefix.find(std::string_view(raw, 2)).result);
  EXPECT_EQ(RegexMatchResult::NoMatch,
            prefix.find(std::string_view(raw + 1, 2)).result);
}

TEST(AuditRegex, EmptySubjectAndAnchors) {
  AuditRegex empty_pat{"^$"};
  EXPECT_EQ(RegexMatchResult::Match, empty_pat.find("").result);
  EXPECT_EQ(RegexMatchResult::Match, empty_pat.find(std::string_view{}).result);
  EXPECT_EQ(RegexMatchResult::NoMatch, empty_pat.find("orders1").result);

  AuditRegex absolute{"\\A\\z"};
  EXPECT_EQ(RegexMatchResult::Match, absolute.find("").result);
  EXPECT_EQ(RegexMatchResult::NoMatch, absolute.find("orders1").result);

  AuditRegex search{"orders"};
  EXPECT_EQ(RegexMatchResult::Match, search.find("new_orders1").result);
  EXPECT_EQ(RegexMatchResult::Match, search.find("orders_archive").result);

  AuditRegex anchored{"\\Aorders[0-9]+\\z"};
  EXPECT_EQ(RegexMatchResult::Match, anchored.find("orders1").result);
  EXPECT_EQ(RegexMatchResult::NoMatch, anchored.find("orders_archive").result);
  EXPECT_EQ(RegexMatchResult::NoMatch, anchored.find("new_orders1").result);
}

TEST(AuditRegex, CaseFlagsUnicodeAndReplacementDecoding) {
  AuditRegex sensitive{"^ORDERS[0-9]+$"};
  EXPECT_EQ(RegexMatchResult::NoMatch, sensitive.find("orders1").result);
  AuditRegex insensitive{"(?i)^ORDERS[0-9]+$"};
  EXPECT_EQ(RegexMatchResult::Match, insensitive.find("orders1").result);

  AuditRegex unicode{"^zam.wienia[0-9]+$"};
  EXPECT_EQ(RegexMatchResult::Match, unicode.find("zamówienia1").result);
  EXPECT_EQ(RegexMatchResult::NoMatch, unicode.find("orders1").result);

  const std::string replacement("\xEF\xBF\xBD", 3);
  AuditRegex fffd{replacement};
  EXPECT_EQ(RegexMatchResult::Match, fffd.find(std::string("\xFF", 1)).result);
  EXPECT_EQ(RegexMatchResult::NoMatch, fffd.find("cafe").result);

  AuditRegex literal_cafe{"café"};
  EXPECT_EQ(RegexMatchResult::NoMatch,
            literal_cafe.find(std::string("caf\xE9", 4)).result);
  EXPECT_EQ(RegexMatchResult::Match, literal_cafe.find("café").result);
}

TEST(AuditRegex, TimeoutThenSuccessfulMatch) {
  std::string subject = "SELECT '";
  subject.append(40, 'a');
  subject.push_back('\'');
  ASSERT_EQ(49U, subject.size());

  AuditRegex compiled{"(a+)+$", RegexLimits{32, 8000000}};
  ASSERT_TRUE(compiled.valid());
  const auto timed_out = compiled.find(subject);
  EXPECT_EQ(RegexMatchResult::Error, timed_out.result);
  EXPECT_EQ(RegexFailureCategory::Timeout, timed_out.failure.category);
  EXPECT_STREQ("U_REGEX_TIME_OUT", icu_error_name(timed_out.failure.status));
  EXPECT_FALSE(timed_out.failure.has_position);

  const auto recovered = compiled.find("SELECT 1 AS aaaa");
  EXPECT_EQ(RegexMatchResult::Match, recovered.result);
}

TEST(AuditRegex, StackExhaustionThenSuccessfulMatch) {
  AuditRegex compiled{"^(a|b)*$", RegexLimits{0, 4096}};
  ASSERT_TRUE(compiled.valid());
  const auto exhausted = compiled.find(std::string(100000, 'a'));
  EXPECT_EQ(RegexMatchResult::Error, exhausted.result);
  EXPECT_EQ(RegexFailureCategory::Stack, exhausted.failure.category);
  EXPECT_STREQ("U_REGEX_STACK_OVERFLOW",
               icu_error_name(exhausted.failure.status));

  EXPECT_EQ(RegexMatchResult::Match, compiled.find("a").result);

  AuditRegex production{"^(a|b)*$"};
  EXPECT_EQ(RegexMatchResult::Match, production.find("a").result);
  EXPECT_EQ(RegexMatchResult::Match,
            production.find(std::string(32, 'a')).result);
}

TEST(AuditRegex, SharedPatternConcurrentLocalMatchers) {
  AuditRegex compiled{"orders[0-9]+"};
  ASSERT_TRUE(compiled.valid());
  constexpr int kThreads = 4;
  std::mutex mutex;
  std::condition_variable ready_cv;
  int arrived = 0;
  bool go = false;
  std::vector<RegexMatchResult> results(kThreads, RegexMatchResult::Error);
  const char *subjects[] = {"orders1", "new_orders2", "history", "customer"};
  const RegexMatchResult expected[] = {
      RegexMatchResult::Match, RegexMatchResult::Match,
      RegexMatchResult::NoMatch, RegexMatchResult::NoMatch};

  std::vector<std::thread> threads;
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i] {
      std::unique_lock<std::mutex> lock(mutex);
      ++arrived;
      ready_cv.notify_all();
      ready_cv.wait(lock, [&] { return go; });
      lock.unlock();
      results[i] = compiled.find(subjects[i]).result;
    });
  }
  {
    std::unique_lock<std::mutex> lock(mutex);
    ready_cv.wait(lock, [&] { return arrived == kThreads; });
    go = true;
  }
  ready_cv.notify_all();
  for (auto &thread : threads) {
    thread.join();
  }
  for (int i = 0; i < kThreads; ++i) {
    EXPECT_EQ(expected[i], results[i]);
  }
}

}  // namespace
}  // namespace audit_log_filter
