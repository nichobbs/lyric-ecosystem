//! Jobs.Quartz.Triggers — Quartz-only scheduling features.
//!
//! `Jobs.JobScheduler` knows two things: run once now, and run on a 5-field
//! Unix cron.  Quartz can do more, and those extras live here, apart from the
//! interface implementation in `Jobs.Quartz`:
//!
//!   - native Quartz cron expressions (6 or 7 fields, seconds resolution,
//!     `L`, `W`, `#`, names),
//!   - fixed-interval triggers with an optional repeat count,
//!   - one-shot triggers with a start delay,
//!   - a misfire policy for the recurring kinds.
//!
//! A `TriggerSpec` is plain data; hand it to `Jobs.Quartz.scheduleTrigger`.
//!
//! `unixCronToQuartz` is the translation `Jobs.Quartz` uses for
//! `JobScheduler.schedule`; it is public so an application can see exactly
//! what a 5-field expression becomes.
@runtime_checked
@io
package Jobs.Quartz.Triggers

import Std.Core
import Std.Collections
import Std.Parse
import Std.String as Str
import Jobs
import Jobs.Quartz.Kernel.Jvm as Kernel

/// What Quartz does with a firing it could not make on time (scheduler in
/// standby or shut down, no free worker thread) once the lateness exceeds
/// `QuartzConfig.misfireThresholdMs`.
pub enum MisfirePolicy {
  /// Quartz's own choice for the trigger type.
  case Smart
  /// Fire once as soon as possible, then carry on with the schedule.
  case FireNow
  /// Drop the missed firings and wait for the next scheduled time.
  case Skip
  /// Fire every missed firing, back to back, then carry on.
  case IgnoreMisfires
}

pub enum TriggerKind {
  case Once
  case Cron
  case Interval
}

/// A Quartz trigger description.  Build one with `once`, `quartzCron` or
/// `interval`; the constructors validate their arguments.
pub record TriggerSpec {
  kind: TriggerKind
  /// Quartz cron expression (`Cron` only).
  cron: String
  /// Delay before the first firing, in milliseconds (`Once` and `Interval`).
  delayMs: Long
  /// Time between firings, in milliseconds (`Interval` only).
  intervalMs: Long
  /// Firings after the first; negative means forever (`Interval` only).
  repeatCount: Int
  misfire: MisfirePolicy
}

/// Fire once, `delayMs` from now.
pub func once(delayMs: in Long): Result[TriggerSpec, String] {
  if delayMs < 0i64 {
    return Err("once: delayMs must be >= 0, got " + delayMs.toString())
  }
  Ok(
    TriggerSpec(
      kind = TriggerKind.Once,
      cron = "",
      delayMs = delayMs,
      intervalMs = 0i64,
      repeatCount = 0,
      misfire = MisfirePolicy.Smart
    )
  )
}

/// Fire on a native Quartz cron expression:
/// `seconds minutes hours day-of-month month day-of-week [year]`, where
/// exactly one of day-of-month and day-of-week is `?` and day-of-week counts
/// 1 = Sunday to 7 = Saturday.  `Err` when Quartz rejects the expression.
pub func quartzCron(expr: in String, misfire: in MisfirePolicy): Result[TriggerSpec, String] {
  if not Kernel.validQuartzCron(expr) {
    return Err("invalid Quartz cron expression '" + expr + "'")
  }
  Ok(
    TriggerSpec(
      kind = TriggerKind.Cron,
      cron = expr,
      delayMs = 0i64,
      intervalMs = 0i64,
      repeatCount = 0,
      misfire = misfire
    )
  )
}

/// Fire after `delayMs`, then every `intervalMs`, `repeatCount` more times
/// (so `repeatCount + 1` firings in total); a negative `repeatCount` repeats
/// forever.
pub func interval(
  delayMs: in Long, intervalMs: in Long, repeatCount: in Int, misfire: in MisfirePolicy
): Result[TriggerSpec, String] {
  if delayMs < 0i64 {
    return Err("interval: delayMs must be >= 0, got " + delayMs.toString())
  }
  if intervalMs < 1i64 {
    return Err("interval: intervalMs must be >= 1, got " + intervalMs.toString())
  }
  Ok(
    TriggerSpec(
      kind = TriggerKind.Interval,
      cron = "",
      delayMs = delayMs,
      intervalMs = intervalMs,
      repeatCount = repeatCount,
      misfire = misfire
    )
  )
}

/// Whether more than one firing can come from `spec`.
pub func isRecurring(spec: in TriggerSpec): Bool {
  match spec.kind {
    case TriggerKind.Once -> false
    case TriggerKind.Cron -> true
    case TriggerKind.Interval -> spec.repeatCount != 0
  }
}

/// The kernel's code for `policy`.
pub func misfireCode(policy: in MisfirePolicy): Int {
  match policy {
    case MisfirePolicy.Smart -> Kernel.misfireSmart
    case MisfirePolicy.FireNow -> Kernel.misfireFireNow
    case MisfirePolicy.Skip -> Kernel.misfireSkip
    case MisfirePolicy.IgnoreMisfires -> Kernel.misfireIgnore
  }
}

// ─── 5-field Unix cron to Quartz cron ─────────────────────────────────────────
// The values one list item (`*`, `a`, `a-b`, each with an optional `/n`)
// selects within [lo, hi].  `a/n` means `a-hi/n`.  `item` has already passed
// `Jobs.parseCron`, so the numbers parse and are in range.
func markItem(item: in String, lo: in Int, hi: in Int, marks: in List[Bool]): Unit {
  val slash = Str.split(item, "/")
  var step = 1
  if slash.length == 2 {
    match parseOptInt(slash[1]) {
      case Some(n) -> {
        step = n
      }
      case None -> ()
    }
  }
  var first = lo
  var last = hi
  if slash[0] != "*" {
    val dash = Str.split(slash[0], "-")
    match parseOptInt(dash[0]) {
      case Some(n) -> {
        first = n
      }
      case None -> ()
    }
    if dash.length == 2 {
      match parseOptInt(dash[1]) {
        case Some(n) -> {
          last = n
        }
        case None -> ()
      }
    } else if slash.length == 1 {
      last = first
    }
  }
  var v = first
  while v <= last {
    marks[v - lo] = true
    v = v + step
  }
}

func markField(field: in String, lo: in Int, hi: in Int): List[Bool] {
  val marks: List[Bool] = newList()
  var i = lo
  while i <= hi {
    marks.add(false)
    i = i + 1
  }
  for item in Str.split(field, ",") {
    markItem(item, lo, hi, marks)
  }
  marks
}

func joinMarked(marks: in List[Bool], lo: in Int): String {
  val parts: List[String] = newList()
  var i = 0
  while i < marks.count {
    if marks[i] {
      parts.add((i + lo).toString())
    }
    i = i + 1
  }
  Str.joinList(",", parts)
}

// A minute, hour, day-of-month or month field as an explicit value list, so
// its meaning does not depend on how Quartz reads steps and ranges.
func expandField(field: in String, lo: in Int, hi: in Int): String {
  if field == "*" {
    return "*"
  }
  joinMarked(markField(field, lo, hi), lo)
}

// Unix day-of-week is 0-7 with 0 and 7 both Sunday; Quartz is 1 (Sunday) to
// 7 (Saturday).
func expandDayOfWeek(field: in String): String {
  val unix = markField(field, 0, 7)
  val quartz: List[Bool] = newList()
  var d = 0
  while d < 7 {
    quartz.add(unix[d] or (d == 0 and unix[7]))
    d = d + 1
  }
  joinMarked(quartz, 1)
}

/// Translate a standard 5-field Unix cron expression (validated by
/// `Jobs.parseCron`) to Quartz cron expressions that fire at second 0 of the
/// same minutes.
///
/// Quartz requires exactly one of day-of-month and day-of-week to be `?`:
///
///   - both `*`                : every day (`* ... ?`),
///   - only day-of-month given : `dom ... ?`,
///   - only day-of-week given  : `? ... dow`,
///   - both given              : POSIX fires when either matches, which one
///     Quartz expression cannot say, so two expressions are returned (one per
///     field) and `Jobs.Quartz` runs the job once for a minute both select.
///
/// A field counts as given unless it is exactly `*`.  Lists, ranges and steps
/// are written out as explicit value lists; `a/n` is read as `a-max/n`.
pub func unixCronToQuartz(expr: in String): Result[slice[String], String] {
  match Jobs.parseCron(expr) {
    case Err(e) -> Err(e)
    case Ok(cron) -> {
      val f = Str.split(Jobs.cronExprToString(cron), " ")
      val head = "0 " + expandField(f[0], 0, 59) + " " + expandField(f[1], 0, 23) + " "
      val month = " " + expandField(f[3], 1, 12) + " "
      val domGiven = f[2] != "*"
      val dowGiven = f[4] != "*"
      val byDom = head + expandField(f[2], 1, 31) + month + "?"
      val byDow = head + "?" + month + expandDayOfWeek(f[4])
      // Typed local: a slice literal written straight into `Ok(...)` is built
      // as a list on the JVM.
      val exprs: slice[String] = if domGiven and dowGiven { [byDom, byDow] } else if dowGiven { [byDow] } else { [byDom] }
      Ok(exprs)
    }
  }
}
