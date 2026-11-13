//! tests/triggers_tests.l — the 5-field cron translation and the trigger
//! constructors of `Jobs.Quartz.Triggers`.  Every translated expression is
//! also checked against Quartz's own parser.
@test_module
package Jobs.Quartz.TriggersTests

import Std.Core
import Std.Collections
import Std.Testing
import Std.String as Str
import Jobs.Quartz.Triggers

func translated(expr: in String): String {
  match Triggers.unixCronToQuartz(expr) {
    case Err(e) -> "Err(" + e + ")"
    case Ok(exprs) -> {
      for q in exprs {
        match Triggers.quartzCron(q, Triggers.MisfirePolicy.Smart) {
          case Ok(_) -> ()
          case Err(e) -> return "Quartz rejects '" + q + "': " + e
        }
      }
      Str.join(" | ", exprs)
    }
  }
}

func expectCron(unix: in String, quartz: in String): Unit {
  assertEqual(translated(unix), quartz, unix)
}

test "every-day expressions put ? in day-of-week" {
  expectCron("* * * * *", "0 * * * * ?")
  expectCron("0 3 * * *", "0 0 3 * * ?")
  expectCron("0 0,12 * * *", "0 0 0,12 * * ?")
}

test "steps, ranges and lists are written out" {
  expectCron("*/5 * * * *", "0 0,5,10,15,20,25,30,35,40,45,50,55 * * * ?")
  expectCron("10-20/5 */8 * * *", "0 10,15,20 0,8,16 * * ?")
  expectCron("50/5 * * * *", "0 50,55 * * * ?")
  expectCron("5,1-3,5 * * 1-12/3 *", "0 1,2,3,5 * * 1,4,7,10 ?")
}

test "a day-of-month expression keeps the day and puts ? in day-of-week" {
  expectCron("30 6 1 * *", "0 30 6 1 * ?")
  expectCron("0 0 */10 * *", "0 0 0 1,11,21,31 * ?")
}

test "day-of-week is renumbered from Unix 0-7 to Quartz 1-7" {
  expectCron("0 3 * * 1-5", "0 0 3 ? * 2,3,4,5,6")
  expectCron("0 3 * * 0", "0 0 3 ? * 1")
  expectCron("0 3 * * 7", "0 0 3 ? * 1")
  expectCron("0 3 * * 6,0", "0 0 3 ? * 1,7")
  expectCron("0 3 * * 5-7", "0 0 3 ? * 1,6,7")
  expectCron("0 3 * * 0-7", "0 0 3 ? * 1,2,3,4,5,6,7")
  expectCron("0 3 * * */2", "0 0 3 ? * 1,3,5,7")
}

test "day-of-month and day-of-week together become two expressions" {
  expectCron("0 9 13 * 5", "0 0 9 13 * ? | 0 0 9 ? * 6")
  expectCron("15 9 1,15 6 1-2", "0 15 9 1,15 6 ? | 0 15 9 ? 6 2,3")
}

test "invalid 5-field expressions are rejected with the validator's message" {
  for expr in ["not a cron", "* * * *", "61 * * * *", "* 24 * * *", "* * 0 * *", "* * * 13 *", "*/0 * * * *", "* * * * 8"] {
    match Triggers.unixCronToQuartz(expr) {
      case Err(e) -> assertTrue(Str.startsWith(e, "cron: "), "error for '" + expr + "': " + e)
      case Ok(_) -> assertTrue(false, "'" + expr + "' must be rejected")
    }
  }
}

test "quartzCron accepts native expressions and rejects malformed ones" {
  for expr in ["* * * * * ?", "0 0/5 14 * * ?", "0 15 10 ? * MON-FRI", "0 15 10 L * ?", "0 15 10 ? * 6#3", "0 0 12 * * ? 2099"] {
    match Triggers.quartzCron(expr, Triggers.MisfirePolicy.Skip) {
      case Ok(spec) -> {
        assertEqual(spec.cron, expr, "the expression is kept verbatim")
        assertTrue(Triggers.isRecurring(spec), "a cron trigger recurs")
      }
      case Err(e) -> assertTrue(false, e)
    }
  }
  for expr in ["", "* * * * *", "0 0 12 * * *", "0 0 25 * * ?", "bogus"] {
    match Triggers.quartzCron(expr, Triggers.MisfirePolicy.Smart) {
      case Ok(_) -> assertTrue(false, "'" + expr + "' must be rejected")
      case Err(e) -> assertTrue(Str.contains(e, "invalid Quartz cron expression"), e)
    }
  }
}

test "once and interval validate their arguments" {
  match Triggers.once(-1i64) {
    case Ok(_) -> assertTrue(false, "a negative delay must be rejected")
    case Err(_) -> ()
  }
  match Triggers.once(250i64) {
    case Ok(spec) -> assertFalse(Triggers.isRecurring(spec), "a one-shot trigger does not recur")
    case Err(e) -> assertTrue(false, e)
  }
  match Triggers.interval(0i64, 0i64, 3, Triggers.MisfirePolicy.Smart) {
    case Ok(_) -> assertTrue(false, "a zero interval must be rejected")
    case Err(_) -> ()
  }
  match Triggers.interval(-5i64, 10i64, 3, Triggers.MisfirePolicy.Smart) {
    case Ok(_) -> assertTrue(false, "a negative delay must be rejected")
    case Err(_) -> ()
  }
  match Triggers.interval(0i64, 10i64, 0, Triggers.MisfirePolicy.Smart) {
    case Ok(spec) -> assertFalse(Triggers.isRecurring(spec), "repeatCount 0 fires once")
    case Err(e) -> assertTrue(false, e)
  }
  match Triggers.interval(0i64, 10i64, -1, Triggers.MisfirePolicy.Smart) {
    case Ok(spec) -> assertTrue(Triggers.isRecurring(spec), "a negative repeatCount repeats forever")
    case Err(e) -> assertTrue(false, e)
  }
}
