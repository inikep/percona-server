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

#include "components/audit_log_filter/audit_error_log.h"
#include "components/audit_log_filter/event_field_condition/and.h"
#include "components/audit_log_filter/event_field_condition/bool.h"
#include "components/audit_log_filter/event_field_condition/field_regex.h"
#include "components/audit_log_filter/event_field_condition/not.h"
#include "components/audit_log_filter/event_field_condition/or.h"
#include "components/audit_log_filter/sys_vars.h"
#include "my_dbug.h"
#include "mysql/components/services/defs/event_tracking_general_defs.h"
#include "mysql/components/services/defs/event_tracking_table_access_defs.h"

#include <barrier>
#include <thread>

namespace {
std::atomic<unsigned> errors{0};
std::atomic<unsigned> warnings{0};
}  // namespace
// Only the service endpoints are faked. Counter policy, escaping, limiter and
// conditional reporting all execute the production condition implementation.
void audit_log_filter::SysVars::inc_regex_match_errors() noexcept { ++errors; }

bool log_item_set_cstring(log_item_data *, char const *) { return false; }
bool log_item_set_int(log_item_data *, longlong) { return false; }
bool log_item_set_lexstring(log_item_data *, char const *, size_t) {
  return false;
}
void log_line_exit(log_line *) {}
log_line *log_line_init() { return nullptr; }
log_item_data *log_line_item_set(log_line *, enum_log_item_type) {
  return nullptr;
}
log_item_data *log_line_item_set_with_key(log_line *, log_item_type,
                                          const char *, uint32) {
  return nullptr;
}
log_item_type_mask log_line_item_types_seen(log_line *, log_item_type_mask) {
  return 0;
}
int log_line_submit(log_line *) { return 0; }
const char *error_message_for_error_log(int code) {
  if (code == ER_AUDIT_FILTER_REGEX_MATCH_FAILURE) ++warnings;
  return nullptr;
}

namespace audit_log_filter::event_field_condition {
namespace {
std::shared_ptr<EventFieldConditionRegex> condition(
    const char *pattern, std::string field = "query") {
  AuditRegex::Error error;
  auto compiled = AuditRegex::compile(pattern, error);
  EXPECT_NE(nullptr, compiled) << error.status_name();
  return std::make_shared<EventFieldConditionRegex>(
      field, std::move(compiled), "filter",
      regex_detail::diagnostic_text(field),
      regex_detail::diagnostic_text(pattern));
}

TEST(AuditFieldRegex, DiagnosticText) {
  using regex_detail::diagnostic_text;
  EXPECT_EQ("a\\u0000\\u000A\\u007F\\u0027\\\\\"é",
            diagnostic_text(std::string_view("a\0\n\x7f'\\\"é", 9)));
  EXPECT_EQ(std::string(96, 'a'), diagnostic_text(std::string(96, 'a')));
  EXPECT_EQ(std::string(93, 'a') + "...",
            diagnostic_text(std::string(97, 'a')));
  EXPECT_EQ(std::string(92, 'a') + "...",
            diagnostic_text(std::string(92, 'a') + "😀xx"));
  EXPECT_EQ(std::string(90, 'a') + "...",
            diagnostic_text(std::string(90, 'a') + "\nxx"));
}

TEST(AuditFieldRegex, DiagnosticMalformedUtf8) {
  using regex_detail::diagnostic_text;
  EXPECT_EQ("\\xC0\\u0000\\u000A\\u0027",
            diagnostic_text(std::string_view("\xc0\0\n'", 4)));
  EXPECT_EQ("\\xE2\\u000A\\x80", diagnostic_text("\xe2\n\x80"));
  EXPECT_EQ("\\xE0\\x80\\x80", diagnostic_text("\xe0\x80\x80"));
  EXPECT_EQ("\\xED\\xA0\\x80", diagnostic_text("\xed\xa0\x80"));
  EXPECT_EQ("\\xF4\\x90\\x80\\x80", diagnostic_text("\xf4\x90\x80\x80"));
  EXPECT_EQ("\\xF0\\x9F\\x98", diagnostic_text("\xf0\x9f\x98"));
  EXPECT_EQ("\\xFF\\\\", diagnostic_text("\xff\\"));
  EXPECT_EQ("é😀", diagnostic_text("é😀"));
  EXPECT_EQ(std::string(89, 'a') + "\\xC0...",
            diagnostic_text(std::string(89, 'a') + "\xc0\n"));
}

TEST(AuditFieldRegex, LimiterPrecisionAndIndependence) {
  using Limiter = regex_detail::WarningLimiter;
  using namespace std::chrono;
  const auto time = [](int64_t ms) {
    return Limiter::Clock::time_point(milliseconds(ms));
  };
  Limiter limiter, second;
  EXPECT_TRUE(limiter.try_acquire(time(100990)));
  EXPECT_FALSE(limiter.try_acquire(time(159990)));
  EXPECT_FALSE(limiter.try_acquire(time(160010)));
  EXPECT_TRUE(second.try_acquire(time(160010)));
  EXPECT_TRUE(limiter.try_acquire(time(160990)));
  EXPECT_FALSE(limiter.try_acquire(time(160990)));
}

TEST(AuditFieldRegex, LimiterContention) {
  regex_detail::WarningLimiter limiter;
  std::barrier start(16);
  std::atomic<int> acquired{0};
  std::vector<std::thread> workers;
  for (int n = 0; n < 16; ++n)
    workers.emplace_back([&] {
      start.arrive_and_wait();
      if (limiter.try_acquire(
              regex_detail::WarningLimiter::Clock::time_point{}))
        ++acquired;
    });
  for (auto &worker : workers) worker.join();
  EXPECT_EQ(1, acquired);
}

TEST(AuditFieldRegex, FieldsAndReportingPolicy) {
  errors = 0;
  warnings = 0;
  auto regex = condition("(a+)+$");
  const AuditRecordFieldsList pathological{
      {"query", "SELECT '" + std::string(40, 'a') + "'"}};
  EXPECT_FALSE(regex->check_applies({}));
  EXPECT_FALSE(regex->check_applies({{"query", int64_t{1}}}));
  EXPECT_FALSE(regex->check_applies({{"query", uint64_t{1}}}));
  EXPECT_FALSE(regex->check_applies({{"query", "bbb"}}));
  EXPECT_EQ(0U, errors);
  for (int i = 0; i < 3; ++i) EXPECT_FALSE(regex->check_applies(pathological));
  EXPECT_EQ(3U, errors);
  EXPECT_EQ(1U, warnings);
  EXPECT_TRUE(regex->check_applies({{"query", "SELECT 1 AS aaaa"}}));
  EXPECT_FALSE(regex->check_applies(pathological));
  EXPECT_EQ(4U, errors);
  EXPECT_EQ(1U, warnings);
  EXPECT_FALSE(condition("(a+)+$")->check_applies(pathological));
  EXPECT_EQ(5U, errors);
  EXPECT_EQ(2U, warnings);
  EventFieldConditionAnd conjunction(
      {std::make_shared<EventFieldConditionBool>(false), regex});
  EventFieldConditionOr disjunction(
      {std::make_shared<EventFieldConditionBool>(true), regex});
  EXPECT_FALSE(conjunction.check_applies(pathological));
  EXPECT_TRUE(disjunction.check_applies(pathological));
  EXPECT_EQ(5U, errors);
  EXPECT_EQ(ConditionResult::Error,
            EventFieldConditionNot(regex).check_result(pathological));
  EXPECT_EQ(6U, errors);
  EXPECT_EQ(2U, warnings);
  EventFieldConditionAnd reached_and(
      {std::make_shared<EventFieldConditionBool>(true), regex});
  EventFieldConditionOr reached_or(
      {std::make_shared<EventFieldConditionBool>(false), regex});
  EventFieldConditionAnd error_before_false(
      {regex, std::make_shared<EventFieldConditionBool>(false)});
  EventFieldConditionNot double_negated(
      std::make_shared<EventFieldConditionNot>(regex));
  EXPECT_EQ(ConditionResult::Error, reached_and.check_result(pathological));
  EXPECT_EQ(ConditionResult::Error, reached_or.check_result(pathological));
  EXPECT_EQ(ConditionResult::NoMatch,
            error_before_false.check_result(pathological));
  EXPECT_EQ(ConditionResult::Error, double_negated.check_result(pathological));
  EXPECT_EQ(10U, errors);
  EXPECT_EQ(2U, warnings);
}

TEST(AuditFieldRegex, ThreeValuedBooleanComposition) {
  using R = ConditionResult;
  const auto false_condition = std::make_shared<EventFieldConditionBool>(false);
  const auto true_condition = std::make_shared<EventFieldConditionBool>(true);
  const std::shared_ptr<EventFieldConditionBase> operands[] = {
      false_condition, true_condition, condition("(a+)+$")};
  const AuditRecordFieldsList fields{
      {"query", "SELECT '" + std::string(40, 'a') + "'"}};
  // Rows and columns: NoMatch, Match, Error. Decisive Boolean values win in
  // either operand order; only otherwise unresolved expressions stay Error.
  const R conjunction[][3] = {{R::NoMatch, R::NoMatch, R::NoMatch},
                              {R::NoMatch, R::Match, R::Error},
                              {R::NoMatch, R::Error, R::Error}};
  const R disjunction[][3] = {{R::NoMatch, R::Match, R::Error},
                              {R::Match, R::Match, R::Match},
                              {R::Error, R::Match, R::Error}};
  const auto negate = [](R value) {
    if (value == R::Error) return R::Error;
    return value == R::Match ? R::NoMatch : R::Match;
  };
  for (int left = 0; left < 3; ++left) {
    for (int right = 0; right < 3; ++right) {
      SCOPED_TRACE(std::to_string(left) + "," + std::to_string(right));
      const std::vector<std::shared_ptr<EventFieldConditionBase>> pair{
          operands[left], operands[right]};
      const auto both = std::make_shared<EventFieldConditionAnd>(pair);
      const auto either = std::make_shared<EventFieldConditionOr>(pair);
      EXPECT_EQ(conjunction[left][right], both->check_result(fields));
      EXPECT_EQ(disjunction[left][right], either->check_result(fields));
      EXPECT_EQ(negate(conjunction[left][right]),
                EventFieldConditionNot(both).check_result(fields));
      EXPECT_EQ(negate(disjunction[left][right]),
                EventFieldConditionNot(either).check_result(fields));
      // Nesting preserves decisive results across the other operator.
      EXPECT_EQ(
          conjunction[left][right],
          EventFieldConditionOr({both, false_condition}).check_result(fields));
      EXPECT_EQ(disjunction[left][right],
                EventFieldConditionAnd({either, true_condition})
                    .check_result(fields));
    }
  }
  EXPECT_EQ(R::Match, EventFieldConditionAnd({}).check_result(fields));
  EXPECT_EQ(R::NoMatch, EventFieldConditionOr({}).check_result(fields));
}

TEST(AuditFieldRegex, RetainedConditionAcrossPublicationChange) {
  auto published = condition("^orders[0-9]+$");
  std::weak_ptr<EventFieldConditionRegex> lifetime = published;
  std::barrier ready(5), resume(5);
  std::vector<std::thread> workers;
  for (int i = 0; i < 4; ++i) {
    workers.emplace_back([&, retained = published, i] {
      ready.arrive_and_wait();
      resume.arrive_and_wait();
      const AuditRecordFieldsList fields{
          {"query", "orders" + std::to_string(i)}};
      for (int j = 0; j < 100; ++j)
        EXPECT_TRUE(retained->check_applies(fields));
    });
  }
  ready.arrive_and_wait();
  published.reset();
  EXPECT_FALSE(lifetime.expired());
  resume.arrive_and_wait();
  for (auto &worker : workers) worker.join();
  EXPECT_TRUE(lifetime.expired());
}

TEST(AuditFieldRegex, RealQueryFieldMaps) {
  mysql_event_tracking_general_data general{};
  mysql_event_tracking_table_access_data table{};
  AuditRecordGeneral g{};
  g.event = &general;
  AuditRecordTableAccess t{};
  t.event = &table;
  for (auto raw : std::vector<std::optional<std::string>>{
           std::nullopt, std::string{}, std::string("a\0b", 3),
           std::string("caf\xe9")}) {
    g.extended_info.query = t.extended_info.query = raw;
    g.extended_info.query_charset = t.extended_info.query_charset = "latin1";
    g.extended_info.query_output = t.extended_info.query_output =
        QueryOutput{"different UTF-8", ""};
    const auto gf = get_audit_record_fields(g), tf = get_audit_record_fields(t);
    EXPECT_EQ(raw.value_or(""),
              std::get<std::string>(gf.at("general_query.str")));
    EXPECT_EQ(raw.value_or(""), std::get<std::string>(tf.at("query.str")));
    EXPECT_EQ(raw ? raw->size() : 0U,
              std::get<uint64_t>(gf.at("general_query.length")));
    EXPECT_EQ(raw ? raw->size() : 0U,
              std::get<uint64_t>(tf.at("query.length")));
    const bool empty = !raw || raw->empty();
    EXPECT_EQ(empty,
              condition("\\A\\z", "general_query.str")->check_applies(gf));
    EXPECT_EQ(empty, condition("^$", "query.str")->check_applies(tf));
    if (raw && raw->size() == 3) {
      EXPECT_TRUE(condition("a\\x{00}b", "query.str")->check_applies(tf));
    }
    if (raw && raw->size() == 4) {
      EXPECT_TRUE(condition("caf\\x{FFFD}", "query.str")->check_applies(tf));
    }
  }
}
}  // namespace
}  // namespace audit_log_filter::event_field_condition
