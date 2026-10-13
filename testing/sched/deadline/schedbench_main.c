/****************************************************************************
 * apps/testing/sched/deadline/schedbench_main.c
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * One image, two policies. The compute kernel is a fixed iteration
 * count. CLOCK_MONOTONIC around that kernel is an isolated-cost
 * estimate during calibration, and a release-to-completion wall time
 * during the run. It is not per-job CPU time.
 *
 ****************************************************************************/

/****************************************************************************
 * Included Files
 ****************************************************************************/

#include <nuttx/config.h>
#include <nuttx/compiler.h>

#include <errno.h>
#include <sched.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <nuttx/clock.h>
#include <nuttx/sched.h>
#include <nuttx/wdog.h>

/****************************************************************************
 * Pre-processor Definitions
 ****************************************************************************/

#define BENCH_TASKS      3
#define BENCH_MAX_JOBS   960
#define BENCH_LOW_PRIO   50
#define BENCH_CTRL_PRIO  40
#define BENCH_OBS_PRIO   80

/* step9 and targeted stay the historical fixtures. stress90 is a
 * separate 200/300/450 ms set and is not a replacement for them.
 */

#define SCEN_STEP9       0
#define SCEN_TARGETED    1
#define SCEN_STRESS90    2
#define SCEN_ISOLATE     3

/****************************************************************************
 * Private Types
 ****************************************************************************/

struct job_rec_s
{
  uint64_t nominal_ns;
  uint64_t actual_ns;
  uint64_t start_ns;
  uint64_t completion_ns;
  uint32_t checksum;
  int16_t  prio_start;
  int16_t  prio_end;
  uint8_t  valid;
};

/****************************************************************************
 * Private Data
 ****************************************************************************/

static unsigned int g_period_ms[BENCH_TASKS];
static unsigned int g_budget_ms[BENCH_TASKS];
static uint64_t g_budget_ns[BENCH_TASKS];
static int g_diag_q105;
static int g_high_prio[BENCH_TASKS];
static unsigned int g_hp_ms;
static unsigned int g_max_hp;
static int g_scenario;
static uint64_t g_post_ns[BENCH_TASKS][BENCH_MAX_JOBS];
static volatile uint8_t g_id_hits[BENCH_TASKS];
static int g_prio_before[BENCH_TASKS];
static uint32_t g_ledger_armed;
static int g_rb_policy[BENCH_TASKS];
static int g_rb_high[BENCH_TASKS];
static int g_rb_low[BENCH_TASKS];
static unsigned int g_rb_budget_ms[BENCH_TASKS];
static unsigned int g_rb_period_ms[BENCH_TASKS];
static uint64_t g_rb_runtime[BENCH_TASKS];
static uint64_t g_rb_deadline_ns[BENCH_TASKS];
static uint64_t g_rb_period_ns[BENCH_TASKS];

static const char g_task_name[BENCH_TASKS] =
{
  'A', 'B', 'C'
};

static volatile uint32_t g_sink;
static sem_t g_park;
static sem_t g_rel[BENCH_TASKS];
static volatile uint16_t g_done[BENCH_TASKS];
static volatile uint64_t g_t0;
static unsigned int g_iters[BENCH_TASKS];
static unsigned int g_hp;
static int g_deadline_mode;
static uint64_t g_cbs_abs[BENCH_TASKS];
static struct job_rec_s g_jobs[BENCH_TASKS][BENCH_MAX_JOBS];
static volatile int g_obs;
static volatile int g_over_seen;
static volatile uint64_t g_over_start;
static volatile uint64_t g_over_end;
static volatile uint32_t g_over_sum;
static volatile uint32_t g_over_chunks;
static volatile uint64_t g_over_until;
static volatile int g_over_done;
static int g_isolate_over;

struct bench_release_s
{
  struct wdog_s wdog;
  unsigned int planned;
  unsigned int period_ms;
  volatile unsigned int next_job;
  volatile unsigned int arm_fail;
  volatile unsigned int late_arm;
  volatile unsigned int overflow;
};

static struct bench_release_s g_release[BENCH_TASKS];
static volatile int g_release_stop;
static int64_t g_late_samples[BENCH_MAX_JOBS];

/****************************************************************************
 * Private Functions
 ****************************************************************************/

static uint64_t mono_ns(void)
{
  struct timespec ts;

  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    {
      return 0;
    }

  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint32_t work_kernel(uint32_t iters) noinline_function;

static uint32_t work_kernel(uint32_t iters)
{
  uint32_t x = 0x00a5a5u;
  uint32_t i;

  for (i = 0; i < iters; i++)
    {
      x ^= x << 13;
      x += 0x9e3779b9u;
      x ^= x >> 17;
      x *= 0x85ebca6bu;
      x ^= x << 5;
    }

  g_sink = x;
  return x;
}

static const char *scenario_name(void)
{
  if (g_scenario == SCEN_STRESS90)
    {
      return "stress90";
    }

  if (g_scenario == SCEN_ISOLATE)
    {
      return g_isolate_over ? "isolate-over" : "isolate-control";
    }

  if (g_scenario == SCEN_TARGETED)
    {
      return "targeted";
    }

  return "step9";
}

static void select_scenario(int scenario)
{
  static const unsigned int step9_period[BENCH_TASKS] =
  {
    200, 240, 300
  };

  static const unsigned int step9_budget[BENCH_TASKS] =
  {
    40, 80, 110
  };

  static const unsigned int targeted_period[BENCH_TASKS] =
  {
    40, 60, 90
  };

  static const unsigned int targeted_budget[BENCH_TASKS] =
  {
    10, 20, 30
  };

  /* Preregistered stress90: Q = 50/100/164 ms, T = D = 200/300/450 ms.
   * Reserved 853/900. Admission ceil is 947779 ppm.
   */

  static const unsigned int stress_period[BENCH_TASKS] =
  {
    200, 300, 450
  };

  static const unsigned int stress_budget[BENCH_TASKS] =
  {
    50, 100, 164
  };

  /* Preregistered isolation fixture. A/B are protected. C is the
   * overrunner and is admitted only when g_isolate_over is set.
   * Q/P charges: 350000 + 333334 + 111112 = 794446 ppm.
   */

  static const unsigned int isolate_period[BENCH_TASKS] =
  {
    40, 60, 90
  };

  static const unsigned int isolate_budget[BENCH_TASKS] =
  {
    14, 20, 10
  };

  static const int high_prio[BENCH_TASKS] =
  {
    220, 210, 200
  };

  int id;

  g_scenario = scenario;
  if (scenario == SCEN_STRESS90)
    {
      g_hp_ms = 1800u;
      g_max_hp = 10u;
    }
  else if (scenario == SCEN_TARGETED || scenario == SCEN_ISOLATE)
    {
      g_hp_ms = 360u;
      g_max_hp = scenario == SCEN_ISOLATE ? 5u : 100u;
    }
  else
    {
      g_hp_ms = 1200u;
      g_max_hp = 32u;
    }

  for (id = 0; id < BENCH_TASKS; id++)
    {
      if (scenario == SCEN_STRESS90)
        {
          g_period_ms[id] = stress_period[id];
          g_budget_ms[id] = stress_budget[id];
        }
      else if (scenario == SCEN_TARGETED)
        {
          g_period_ms[id] = targeted_period[id];
          g_budget_ms[id] = targeted_budget[id];
        }
      else if (scenario == SCEN_ISOLATE)
        {
          g_period_ms[id] = isolate_period[id];
          g_budget_ms[id] = isolate_budget[id];
        }
      else
        {
          g_period_ms[id] = step9_period[id];
          g_budget_ms[id] = step9_budget[id];
        }

      g_budget_ns[id] = (uint64_t)g_budget_ms[id] * 1000000ull;
      g_high_prio[id] = high_prio[id];
    }

  /* Diagnostic only. The default targeted reservation stays 10/20/30 ms.
   * Sporadic enforcement is in 1 ms ticks and rounds this request up.
   */

  if (g_diag_q105 && scenario == SCEN_TARGETED)
    {
      static const uint64_t q105_ns[BENCH_TASKS] =
      {
        10500000ull, 20500000ull, 30500000ull
      };

      for (id = 0; id < BENCH_TASKS; id++)
        {
          g_budget_ns[id] = q105_ns[id];
        }
    }
}

static unsigned int jobs_for(int id, unsigned int hp)
{
  return (unsigned int)(((uint64_t)hp * g_hp_ms) / g_period_ms[id]);
}

static unsigned int ts_ms(FAR const struct timespec *ts)
{
  return (unsigned int)ts->tv_sec * 1000u +
         (unsigned int)(ts->tv_nsec / 1000000L);
}

static int current_prio(void)
{
  struct sched_param param;

  memset(&param, 0, sizeof(param));
  if (sched_getparam(0, &param) != 0)
    {
      return -1;
    }

  return param.sched_priority;
}

static int worker_main(int argc, FAR char *argv[])
{
  int id;
  unsigned int job;
  unsigned int planned;
  struct job_rec_s *rec;

  if (argc < 2)
    {
      return 1;
    }

  /* task_create() prepends the task name, so the id is the last argument. */

  id = atoi(argv[argc - 1]);
  if (id < 0 || id >= BENCH_TASKS)
    {
      return 1;
    }

  g_id_hits[id]++;
  sem_post(&g_park);
  planned = jobs_for(id, g_hp);
  for (job = 0; job < planned; job++)
    {
      uint64_t nominal;
      uint64_t actual;
      uint64_t start;
      uint64_t done;
      int p0;
      int p1;

      sem_wait(&g_rel[id]);
      actual = mono_ns();
      p0 = current_prio();
      start = mono_ns();
      rec = &g_jobs[id][job];
      nominal = g_t0 + (uint64_t)job * g_period_ms[id] * 1000000ull;
      rec->nominal_ns = nominal;
      rec->actual_ns = actual;
      rec->start_ns = start;
      rec->checksum = work_kernel(g_iters[id]);
      done = mono_ns();
      p1 = current_prio();
      rec->completion_ns = done;
      rec->prio_start = (int16_t)p0;
      rec->prio_end = (int16_t)p1;
      rec->valid = 1;
#ifdef CONFIG_SCHED_DEADLINE_TRACE
      /* After the completion timestamp. Stops the ring on the first
       * job whose wall completion is past its job deadline.
       */

      if (g_deadline_mode &&
          done > nominal + (uint64_t)g_period_ms[id] * 1000000ull)
        {
          sched_deadline_trace_freeze();
        }
#endif
      if (job == 0 && g_deadline_mode)
        {
          struct sched_deadline_view_s view;

          memset(&view, 0, sizeof(view));
          if (sched_deadline_view(0, &view) == 0)
            {
              g_cbs_abs[id] = view.abs_deadline;
            }
        }

      g_done[id] = (uint16_t)(job + 1);
    }

  return 0;
}

static int over_worker(int argc, FAR char *argv[])
{
  (void)argc;
  (void)argv;
  sem_post(&g_park);
  sem_wait(&g_rel[0]);
  g_over_start = mono_ns();
  g_over_sum = work_kernel(g_iters[0]);
  g_over_end = mono_ns();
  g_over_seen = g_obs;
  g_done[0] = 1;
  return 0;
}

static int over_observer(int argc, FAR char *argv[])
{
  (void)argc;
  (void)argv;
  sem_post(&g_park);
  sem_wait(&g_rel[1]);
  g_obs = 1;
  return 0;
}

/* One offered kernel is about 60 ms if it ran without throttling.
 * CBS may stop it inside work_kernel. The loop condition is checked
 * only between kernels, so the last chunk is bounded by one kernel.
 */

static int isolate_over_main(int argc, FAR char *argv[])
{
  uint32_t checksum = 0;
  uint32_t chunks = 0;

  (void)argc;
  (void)argv;
  g_id_hits[2]++;
  sem_post(&g_park);
  sem_wait(&g_rel[2]);
  while (mono_ns() < g_over_until)
    {
      struct sched_deadline_view_s view;

      memset(&view, 0, sizeof(view));
      if (sched_deadline_view(0, &view) == 0)
        {
          printf("isolate: chunk=%lu ts=%llu remaining=%llu abs=%llu "
                 "overrun=%llu sched=%u\n",
                 (unsigned long)chunks, (unsigned long long)mono_ns(),
                 (unsigned long long)view.remaining_ns,
                 (unsigned long long)view.abs_deadline,
                 (unsigned long long)view.overrun_ns,
                 (unsigned)view.sched);
        }

      checksum ^= work_kernel(g_iters[2]);
      chunks++;
    }

  g_over_sum = checksum;
  g_over_chunks = chunks;
  g_over_end = mono_ns();
  g_over_done = 1;
  printf("isolate: over_exit chunks=%lu checksum=%lu ts=%llu\n",
         (unsigned long)chunks, (unsigned long)checksum,
         (unsigned long long)g_over_end);
  return 0;
}

static int set_fifo(pid_t pid, int priority)
{
  struct sched_param param;

  memset(&param, 0, sizeof(param));
  param.sched_priority = priority;
  if (sched_setscheduler(pid, SCHED_FIFO, &param) != 0)
    {
      return errno;
    }

  return 0;
}

static void ms_timespec(FAR struct timespec *ts, unsigned int ms)
{
  ts->tv_sec = (time_t)(ms / 1000u);
  ts->tv_nsec = (long)(ms % 1000u) * 1000000L;
}

static void ns_timespec(FAR struct timespec *ts, uint64_t ns)
{
  ts->tv_sec = (time_t)(ns / 1000000000ull);
  ts->tv_nsec = (long)(ns % 1000000000ull);
}

static uint64_t ts_ns(FAR const struct timespec *ts)
{
  return (uint64_t)ts->tv_sec * 1000000000ull + (uint64_t)ts->tv_nsec;
}

static int apply_sporadic(pid_t pid, int id, int quiet)
{
  struct sched_param param;
  struct sched_param got;
  int policy;
  unsigned int budget_ms;
  unsigned int period_ms;

  memset(&param, 0, sizeof(param));
  param.sched_priority = g_high_prio[id];
  param.sched_ss_low_priority = BENCH_LOW_PRIO;
  param.sched_ss_max_repl = 3;
  ns_timespec(&param.sched_ss_init_budget, g_budget_ns[id]);
  ms_timespec(&param.sched_ss_repl_period, g_period_ms[id]);
  if (sched_setscheduler(pid, SCHED_SPORADIC, &param) != 0)
    {
      printf("schedbench: sporadic %c errno=%d\n", g_task_name[id], errno);
      return -1;
    }

  policy = sched_getscheduler(pid);
  memset(&got, 0, sizeof(got));
  if (policy != SCHED_SPORADIC || sched_getparam(pid, &got) != 0)
    {
      printf("schedbench: sporadic readback %c policy=%d errno=%d\n",
             g_task_name[id], policy, errno);
      return -1;
    }

  budget_ms = ts_ms(&got.sched_ss_init_budget);
  period_ms = ts_ms(&got.sched_ss_repl_period);
  {
    uint64_t got_ns = ts_ns(&got.sched_ss_init_budget);
    uint64_t want_ns = g_budget_ns[id];
    uint64_t tick_ns = 1000000ull;
    uint64_t rounded_ns = ((want_ns + tick_ns - 1ull) / tick_ns) * tick_ns;

    /* The kernel stores this budget in ticks and rounds up. A 10.5 ms
     * request becomes 11 ms. Record the enforced value.
     */

    if (got_ns != want_ns && got_ns != rounded_ns)
      {
        printf("schedbench: sporadic budget %c requested_ns=%llu enforced_ns=%llu\n",
               g_task_name[id], (unsigned long long)want_ns,
               (unsigned long long)got_ns);
        return -1;
      }

    if (got_ns != want_ns)
      {
        printf("schedbench: sporadic quantized %c requested_ns=%llu enforced_ns=%llu\n",
               g_task_name[id], (unsigned long long)want_ns,
               (unsigned long long)got_ns);
      }

    g_budget_ns[id] = got_ns;
  }

  if (got.sched_ss_low_priority != BENCH_LOW_PRIO ||
      got.sched_ss_max_repl != 3 ||
      got.sched_priority != g_high_prio[id] ||
      period_ms != g_period_ms[id])
    {
      printf("schedbench: sporadic param %c prio=%d low=%d repl=%d budget_ms=%u period_ms=%u\n",
             g_task_name[id], got.sched_priority,
             got.sched_ss_low_priority, got.sched_ss_max_repl,
             budget_ms, period_ms);
      return -1;
    }

  g_rb_policy[id] = policy;
  g_rb_high[id] = got.sched_priority;
  g_rb_low[id] = got.sched_ss_low_priority;
  g_rb_budget_ms[id] = budget_ms;
  g_rb_period_ms[id] = period_ms;
  if (!quiet)
    {
      printf("schedbench: readback %c policy=%d high=%d low=%d budget_ms=%u period_ms=%u\n",
             g_task_name[id], policy, got.sched_priority,
             got.sched_ss_low_priority, budget_ms, period_ms);
    }

  return 0;
}

static int apply_deadline(pid_t pid, int id, int quiet)
{
  struct sched_attr attr;
  struct sched_attr got;
  int policy;
  uint64_t runtime;

  runtime = g_budget_ns[id];
  memset(&attr, 0, sizeof(attr));
  attr.size = SCHED_ATTR_SIZE_VER0;
  attr.sched_policy = SCHED_DEADLINE;
  attr.sched_runtime = runtime;
  attr.sched_deadline = (uint64_t)g_period_ms[id] * 1000000ull;
  attr.sched_period = attr.sched_deadline;
  if (sched_setattr(pid, &attr, 0) != 0)
    {
      printf("schedbench: deadline %c errno=%d admitted=%lu\n",
             g_task_name[id], errno,
             (unsigned long)sched_deadline_admitted());
      return -1;
    }

  policy = sched_getscheduler(pid);
  memset(&got, 0, sizeof(got));
  got.size = SCHED_ATTR_SIZE_VER0;
  if (policy != SCHED_DEADLINE ||
      sched_getattr(pid, &got, SCHED_ATTR_SIZE_VER0, 0) != 0 ||
      got.sched_runtime != attr.sched_runtime ||
      got.sched_deadline != attr.sched_deadline ||
      got.sched_period != attr.sched_period)
    {
      printf("schedbench: deadline readback %c policy=%d errno=%d\n",
             g_task_name[id], policy, errno);
      return -1;
    }

  g_rb_policy[id] = policy;
  g_rb_high[id] = 0;
  g_rb_low[id] = 0;
  g_rb_budget_ms[id] = g_budget_ms[id];
  g_rb_period_ms[id] = g_period_ms[id];
  g_rb_runtime[id] = got.sched_runtime;
  g_rb_deadline_ns[id] = got.sched_deadline;
  g_rb_period_ns[id] = got.sched_period;
  if (!quiet)
    {
      printf("schedbench: readback %c policy=%d runtime=%llu deadline=%llu period=%llu\n",
             g_task_name[id], policy,
             (unsigned long long)got.sched_runtime,
             (unsigned long long)got.sched_deadline,
             (unsigned long long)got.sched_period);
    }

  return 0;
}

static int arm_release(FAR struct bench_release_s *rel);

static void release_fire(wdparm_t arg)
{
  FAR struct bench_release_s *rel;
  unsigned int id;
  unsigned int job;

  rel = (FAR struct bench_release_s *)(uintptr_t)arg;
  if (g_release_stop || rel == NULL)
    {
      return;
    }

  id = (unsigned int)(rel - g_release);
  if (id >= BENCH_TASKS)
    {
      return;
    }

  job = rel->next_job;
  if (job >= rel->planned || job >= BENCH_MAX_JOBS || g_post_ns[id][job] != 0)
    {
      rel->overflow++;
      return;
    }

  g_post_ns[id][job] = mono_ns();
  if (sem_post(&g_rel[id]) != 0)
    {
      rel->arm_fail++;
      return;
    }

  rel->next_job = job + 1;
  if (rel->next_job < rel->planned && arm_release(rel) != 0)
    {
      rel->arm_fail++;
    }
}

static int arm_release(FAR struct bench_release_s *rel)
{
  uint64_t nominal;
  uint64_t now;
  struct timespec when;
  clock_t ticks;

  nominal = g_t0 + (uint64_t)rel->next_job * (uint64_t)rel->period_ms *
            1000000ull;
  now = mono_ns();
  if (now != 0 && nominal <= now)
    {
      /* The absolute target is already due. Post this job on the next
       * tick instead of dropping it. wd_start(0) is current tick + 1.
       */

      rel->late_arm++;
      return wd_start(&rel->wdog, 0, release_fire,
                      (wdparm_t)(uintptr_t)rel);
    }

  when.tv_sec = (time_t)(nominal / 1000000000ull);
  when.tv_nsec = (long)(nominal % 1000000000ull);
  ticks = clock_time2ticks(&when);
  return wd_start_abstick(&rel->wdog, ticks, release_fire,
                          (wdparm_t)(uintptr_t)rel);
}

static void release_quiesce(void)
{
  int id;

  g_release_stop = 1;
  for (id = 0; id < BENCH_TASKS; id++)
    {
      wd_cancel(&g_release[id].wdog);
    }
}

static int cmp_i64(FAR const void *a, FAR const void *b)
{
  int64_t da = *(FAR const int64_t *)a;
  int64_t db = *(FAR const int64_t *)b;

  if (da < db)
    {
      return -1;
    }

  if (da > db)
    {
      return 1;
    }

  return 0;
}

static void report_release_lateness(int id, unsigned int planned)
{
  unsigned int job;
  unsigned int n = 0;
  unsigned int p50;
  unsigned int p95;

  for (job = 0; job < planned && job < BENCH_MAX_JOBS; job++)
    {
      uint64_t nominal;

      if (g_post_ns[id][job] == 0)
        {
          continue;
        }

      nominal = g_t0 + (uint64_t)job * (uint64_t)g_period_ms[id] * 1000000ull;
      g_late_samples[n++] = (int64_t)g_post_ns[id][job] - (int64_t)nominal;
    }

  if (n == 0)
    {
      printf("schedbench: release_late %c n=0\n", g_task_name[id]);
      return;
    }

  qsort(g_late_samples, n, sizeof(g_late_samples[0]), cmp_i64);
  p50 = (n - 1u) / 2u;
  p95 = (unsigned int)(((n * 95u) + 99u) / 100u);
  if (p95 > 0)
    {
      p95--;
    }

  printf("schedbench: release_late %c n=%u max=%lld p50=%lld p95=%lld late_arm=%u\n",
         g_task_name[id], n,
         (long long)g_late_samples[n - 1u],
         (long long)g_late_samples[p50],
         (long long)g_late_samples[p95],
         g_release[id].late_arm);
}

static int spawn_parked(FAR pid_t *pid, FAR main_t entry,
                        FAR char *name, FAR char *arg1, int priority)
{
  FAR char *argv[3];

  /* The kernel copies name into argv[0]. Pass only the task argument. */

  argv[0] = arg1;
  argv[1] = NULL;
  *pid = task_create(name, priority, 8192, entry, argv);
  if (*pid < 0)
    {
      printf("schedbench: task_create %s errno=%d\n", name, errno);
      return -1;
    }

  if (sem_wait(&g_park) != 0)
    {
      printf("schedbench: park %s errno=%d\n", name, errno);
      return -1;
    }

  return 0;
}

static void print_timing_limit(void)
{
  struct timespec res;

  memset(&res, 0, sizeof(res));
  clock_getres(CLOCK_MONOTONIC, &res);
  printf("schedbench: clock=CLOCK_MONOTONIC res_ns=%ld\n",
         (long)res.tv_nsec + (long)res.tv_sec * 1000000000L);
  printf("schedbench: cputime=unavailable CONFIG_SCHED_CRITMONITOR is off\n");
  printf("schedbench: isolated calibration is an execution-cost estimate, not per-job CPU time\n");
  printf("schedbench: response time is nominal-release to completion wall time\n");
}

static uint64_t sample_work(uint32_t iters, FAR uint32_t *checksum)
{
  uint64_t t0;
  uint64_t t1;

  t0 = mono_ns();
  *checksum = work_kernel(iters);
  t1 = mono_ns();
  if (t1 < t0)
    {
      return 0;
    }

  return t1 - t0;
}

static int cmd_calibrate(int scenario)
{
  enum
  {
    NSAMP = 8,
    NCHECK = 21
  };

  int ncheck;

  static const uint64_t step9_ns[4] =
  {
    36000000ull, 72000000ull, 99000000ull, 70000000ull
  };

  /* 70% of the 10/20/30 ms reservations. The reservation itself is unchanged. */

  static const uint64_t targeted_ns[3] =
  {
    7000000ull, 14000000ull, 21000000ull
  };

  /* Isolated targets for stress90: 47/95/156 ms, about 94–95% of Q. */

  static const uint64_t stress90_ns[3] =
  {
    47000000ull, 95000000ull, 156000000ull
  };

  static const uint64_t isolate_ns[3] =
  {
    9000000ull, 14000000ull, 60000000ull
  };

  static const char *step9_label[4] =
  {
    "A", "B", "C", "over"
  };

  static const char *targeted_label[3] =
  {
    "T1", "T2", "T3"
  };

  static const char *stress90_label[3] =
  {
    "T1", "T2", "T3"
  };

  static const char *isolate_label[3] =
  {
    "A", "B", "C"
  };

  FAR const uint64_t *target_ns;
  FAR const char * const *label;
  int ntargets;

  uint64_t samples[NSAMP];
  uint64_t median;
  uint64_t probe_ns;
  uint32_t probe_iters;
  uint32_t checksum;
  uint32_t iters[4];
  int i;
  int s;
  int saved;

  if (scenario == SCEN_STRESS90)
    {
      target_ns = stress90_ns;
      label = stress90_label;
      ntargets = 3;
      ncheck = NCHECK;
    }
  else if (scenario == SCEN_ISOLATE)
    {
      target_ns = isolate_ns;
      label = isolate_label;
      ntargets = 3;
      ncheck = NCHECK;
    }
  else if (scenario == SCEN_TARGETED)
    {
      target_ns = targeted_ns;
      label = targeted_label;
      ntargets = 3;
      ncheck = NCHECK;
    }
  else
    {
      target_ns = step9_ns;
      label = step9_label;
      ntargets = 4;
      ncheck = 5;
    }

  printf("schedbench: scenario=%s\n",
         scenario == SCEN_STRESS90 ? "stress90" :
         scenario == SCEN_ISOLATE ? "isolate" :
         scenario == SCEN_TARGETED ? "targeted" : "step9");
  print_timing_limit();
  saved = current_prio();
  if (set_fifo(0, 240) != 0)
    {
      printf("schedbench: calibrate priority errno=%d\n", errno);
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  probe_iters = 200000u;
  probe_ns = 0;
  while (probe_ns < 100000000ull)
    {
      if (probe_iters > 1000000000u)
        {
          printf("schedbench: probe did not reach 100ms\n");
          set_fifo(0, saved > 0 ? saved : 100);
          printf("SCHEDBENCH_FAIL\n");
          return 1;
        }

      probe_iters *= 2u;
      probe_ns = sample_work(probe_iters, &checksum);
    }

  probe_iters = (uint32_t)(((uint64_t)probe_iters * 500000000ull) / probe_ns);
  if (probe_iters < 1000u)
    {
      probe_iters = 1000u;
    }

  for (s = 0; s < NSAMP; s++)
    {
      samples[s] = sample_work(probe_iters, &checksum);
    }

  for (i = 0; i < NSAMP; i++)
    {
      for (s = i + 1; s < NSAMP; s++)
        {
          if (samples[s] < samples[i])
            {
              uint64_t swap = samples[i];

              samples[i] = samples[s];
              samples[s] = swap;
            }
        }
    }

  median = samples[NSAMP / 2];
  printf("SCHEDBENCH_PROBE iters=%lu median_ns=%llu min_ns=%llu max_ns=%llu checksum=%lu\n",
         (unsigned long)probe_iters, (unsigned long long)median,
         (unsigned long long)samples[0],
         (unsigned long long)samples[NSAMP - 1], (unsigned long)checksum);

  printf("SCHEDBENCH_MANIFEST scenario=%s samples=%d\n",
         scenario == SCEN_STRESS90 ? "stress90" :
         scenario == SCEN_ISOLATE ? "isolate" :
         scenario == SCEN_TARGETED ? "targeted" : "step9", ncheck);
  for (i = 0; i < ntargets; i++)
    {
      uint64_t verify[NCHECK];
      uint64_t sum;
      uint64_t p50;
      uint64_t p95;
      uint32_t cs;
      uint32_t cs0;
      int p95_index;

      iters[i] = (uint32_t)((target_ns[i] * (uint64_t)probe_iters) / median);
      if (iters[i] == 0)
        {
          iters[i] = 1;
        }

      sum = 0;
      cs0 = 0;
      for (s = 0; s < ncheck; s++)
        {
          verify[s] = sample_work(iters[i], &cs);
          if (s == 0)
            {
              cs0 = cs;
            }

          sum += verify[s];
        }

      {
        uint64_t vmin = verify[0];
        uint64_t vmax = verify[0];
        int a;
        int b;

        for (s = 1; s < ncheck; s++)
          {
            if (verify[s] < vmin)
              {
                vmin = verify[s];
              }

            if (verify[s] > vmax)
              {
                vmax = verify[s];
              }
          }

        for (a = 0; a < ncheck; a++)
          {
            for (b = a + 1; b < ncheck; b++)
              {
                if (verify[b] < verify[a])
                  {
                    uint64_t swap = verify[a];

                    verify[a] = verify[b];
                    verify[b] = swap;
                  }
              }
          }

        p50 = verify[(ncheck - 1) / 2];
        p95_index = (int)(((ncheck * 95) + 99) / 100) - 1;
        if (p95_index < 0)
          {
            p95_index = 0;
          }

        if (p95_index >= ncheck)
          {
            p95_index = ncheck - 1;
          }

        p95 = verify[p95_index];
        printf("SCHEDBENCH_TASK %s iters=%lu target_ns=%llu n=%d "
               "ns_min=%llu ns_p50=%llu ns_mean=%llu ns_p95=%llu "
               "ns_max=%llu checksum=%lu\n",
               label[i], (unsigned long)iters[i],
               (unsigned long long)target_ns[i], ncheck,
               (unsigned long long)vmin,
               (unsigned long long)p50,
               (unsigned long long)(sum / (uint64_t)ncheck),
               (unsigned long long)p95,
               (unsigned long long)vmax,
               (unsigned long)cs0);
      }
    }

  printf("schedbench: isolated samples are FIFO-240 wall time around work_kernel\n");
  set_fifo(0, saved > 0 ? saved : 100);
  printf("SCHEDBENCH_PASS\n");
  fflush(stdout);
  return 0;
}

static void print_job_row(unsigned int run_id, FAR const char *policy,
                          int id, unsigned int job, FAR const struct job_rec_s *rec,
                          int incomplete)
{
  uint64_t nominal;
  uint64_t abs_dl;
  uint64_t posted;
  const char *status;
  int missed;

  nominal = g_t0 + (uint64_t)job * g_period_ms[id] * 1000000ull;
  abs_dl = nominal + (uint64_t)g_period_ms[id] * 1000000ull;
  posted = g_post_ns[id][job];
  if (incomplete || rec == NULL || rec->valid == 0)
    {
      printf("%u,%s,%c,%u,%llu,NA,%llu,NA,NA,NA,NA,NA,NA,incomplete,%d,%d,%llu,%u,%u,NA,NA,NA,NA,NA,%llu,%lld,NA,NA,NA,0\n",
             run_id, policy, g_task_name[id], job,
             (unsigned long long)nominal,
             (unsigned long long)abs_dl,
             g_deadline_mode ? SCHED_DEADLINE : SCHED_SPORADIC,
             g_deadline_mode ? 0 : g_high_prio[id],
             (unsigned long long)g_budget_ns[id],
             g_period_ms[id] * 1000000u,
             g_period_ms[id] * 1000000u,
             (unsigned long long)posted,
             posted != 0 ? (long long)(posted - nominal) : 0ll);
      return;
    }

  missed = rec->completion_ns > abs_dl;
  status = missed ? "miss" : "ok";
  printf("%u,%s,%c,%u,%llu,%llu,%llu,%llu,%llu,%lld,%lld,%d,%lld,%s,%d,%d,%llu,%u,%u,NA,NA,NA,%s,%lu,%llu,%lld,%lld,%d,%d,0\n",
         run_id, policy, g_task_name[id], job,
         (unsigned long long)rec->nominal_ns,
         (unsigned long long)rec->actual_ns,
         (unsigned long long)abs_dl,
         (unsigned long long)rec->start_ns,
         (unsigned long long)rec->completion_ns,
         (long long)(rec->completion_ns - rec->nominal_ns),
         missed ? (long long)(rec->completion_ns - abs_dl) : 0ll,
         missed,
         (long long)(rec->actual_ns - rec->nominal_ns),
         status,
         g_deadline_mode ? SCHED_DEADLINE : SCHED_SPORADIC,
         g_deadline_mode ? 0 : g_high_prio[id],
         (unsigned long long)g_budget_ns[id],
         g_period_ms[id] * 1000000u,
         g_period_ms[id] * 1000000u,
         g_deadline_mode ? "NA" :
         (rec->prio_end >= 0 && rec->prio_end < g_high_prio[id]) ? "1" : "0",
         (unsigned long)rec->checksum,
         posted != 0 ? (unsigned long long)posted : 0ull,
         posted != 0 ? (long long)(posted - nominal) : 0ll,
         (posted != 0) ? (long long)(rec->actual_ns - posted) : 0ll,
         rec->prio_start, rec->prio_end);
}

static int cmd_compare(unsigned int run_id, int deadline_mode, int scenario,
                       unsigned int iters_a, unsigned int iters_b,
                       unsigned int iters_c, unsigned int hp)
{
  static char argn[BENCH_TASKS][2];
  static char name[BENCH_TASKS][8];
  pid_t pid[BENCH_TASKS];
  unsigned int planned[BENCH_TASKS];
  int id;
  int failed;
  uint32_t ledger;
  const char *policy;
  uint64_t window_end;
  int saved;

  select_scenario(scenario);
  if (g_diag_q105)
    {
      printf("schedbench: diagnostic=q105 requested_ns=%llu,%llu,%llu period_ms=%u,%u,%u\n",
             10500000ull, 20500000ull, 30500000ull,
             g_period_ms[0], g_period_ms[1], g_period_ms[2]);
    }

  if (hp == 0 || hp > g_max_hp)
    {
      printf("schedbench: hyperperiods %u out of range\n", hp);
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  for (id = 0; id < BENCH_TASKS; id++)
    {
      if (jobs_for(id, hp) > BENCH_MAX_JOBS)
        {
          printf("schedbench: job table overflow\n");
          printf("SCHEDBENCH_FAIL\n");
          return 1;
        }
    }

  ledger = sched_deadline_admitted();
  if (ledger != 0)
    {
      printf("schedbench: ledger not zero at start %lu\n",
             (unsigned long)ledger);
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  print_timing_limit();
  memset(g_jobs, 0, sizeof(g_jobs));
  memset(g_post_ns, 0, sizeof(g_post_ns));
  memset((void *)g_done, 0, sizeof(g_done));
  memset((void *)g_id_hits, 0, sizeof(g_id_hits));
  memset(g_cbs_abs, 0, sizeof(g_cbs_abs));
  memset(g_prio_before, 0, sizeof(g_prio_before));
  g_ledger_armed = 0;
  g_iters[0] = iters_a;
  g_iters[1] = iters_b;
  g_iters[2] = iters_c;
  g_hp = hp;
  g_deadline_mode = deadline_mode;
  g_t0 = 0;
#ifdef CONFIG_SCHED_DEADLINE_TRACE
  sched_deadline_trace_reset();
#endif
  policy = deadline_mode ? "deadline" : "sporadic";
  failed = 0;

  sem_init(&g_park, 0, 0);
  g_over_done = 0;
  g_over_chunks = 0;
  g_over_until = 0;
  for (id = 0; id < BENCH_TASKS; id++)
    {
      pid[id] = -1;
      sem_init(&g_rel[id], 0, 0);
      if (sem_setprotocol(&g_rel[id], SEM_PRIO_NONE) != 0)
        {
          printf("schedbench: sem protocol errno=%d\n", errno);
          printf("SCHEDBENCH_FAIL\n");
          return 1;
        }

      planned[id] = jobs_for(id, hp);
    }

  /* C is not a periodic job row. The overrunner, when enabled, has
   * its own loop and must not be waited on as 20 unfinished jobs.
   */

  if (scenario == SCEN_ISOLATE)
    {
      planned[2] = 0;
    }

  for (id = 0; id < BENCH_TASKS; id++)
    {
      if (scenario == SCEN_ISOLATE && id == 2)
        {
          continue;
        }

      argn[id][0] = (char)('0' + id);
      argn[id][1] = '\0';
      snprintf(name[id], sizeof(name[id]), "bench%c", g_task_name[id]);
      if (spawn_parked(&pid[id], worker_main, name[id], argn[id],
                       BENCH_LOW_PRIO) != 0)
        {
          printf("SCHEDBENCH_FAIL\n");
          return 1;
        }
    }

  if (scenario == SCEN_ISOLATE && g_isolate_over)
    {
      argn[2][0] = '2';
      argn[2][1] = '\0';
      snprintf(name[2], sizeof(name[2]), "benchC");
      if (spawn_parked(&pid[2], isolate_over_main, name[2], argn[2],
                       BENCH_LOW_PRIO) != 0)
        {
          printf("SCHEDBENCH_FAIL\n");
          return 1;
        }
    }

  for (id = 0; id < BENCH_TASKS; id++)
    {
      int rc;

      if (pid[id] < 0)
        {
          continue;
        }

      rc = deadline_mode ? apply_deadline(pid[id], id, scenario != SCEN_STEP9) :
                           apply_sporadic(pid[id], id, scenario != SCEN_STEP9);
      if (rc != 0)
        {
          failed = 1;
          break;
        }
    }

  if (failed)
    {
      for (id = 0; id < BENCH_TASKS; id++)
        {
          if (pid[id] >= 0)
            {
              task_delete(pid[id]);
            }
        }

      printf("schedbench: admitted_after_fail=%lu\n",
             (unsigned long)sched_deadline_admitted());
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  if (g_id_hits[0] != 1 || g_id_hits[1] != 1 ||
      (scenario != SCEN_ISOLATE && g_id_hits[2] != 1) ||
      (g_isolate_over && g_id_hits[2] != 1))
    {
      printf("schedbench: worker id hits %u %u %u\n",
             g_id_hits[0], g_id_hits[1], g_id_hits[2]);
      for (id = 0; id < BENCH_TASKS; id++)
        {
          if (pid[id] >= 0)
            {
              task_delete(pid[id]);
            }
        }

      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  if (deadline_mode)
    {
      g_ledger_armed = sched_deadline_admitted();
      if (g_ledger_armed == 0)
        {
          printf("schedbench: deadline admission ledger stayed 0\n");
          for (id = 0; id < BENCH_TASKS; id++)
            {
              if (pid[id] >= 0)
                {
                  task_delete(pid[id]);
                }
            }

          printf("SCHEDBENCH_FAIL\n");
          return 1;
        }
    }

  saved = current_prio();
  set_fifo(0, BENCH_CTRL_PRIO);

  /* Targeted sporadic budgets start at sched_setscheduler. A long delay
   * would let that watchdog expire before job 0. Step 9 keeps 200 ms.
   */

  for (id = 0; id < BENCH_TASKS; id++)
    {
      struct sched_param param;

      g_prio_before[id] = -1;
      memset(&param, 0, sizeof(param));
      if (sched_getparam(pid[id], &param) == 0)
        {
          g_prio_before[id] = param.sched_priority;
        }
    }

  /* Sporadic replenishment starts at sched_setscheduler. stress90's
   * smallest budget is 50 ms, so it uses the same 2 ms lead as targeted.
   */

  g_t0 = mono_ns() + (scenario == SCEN_STEP9 ? 200000000ull : 2000000ull);
  window_end = g_t0 + (uint64_t)hp * g_hp_ms * 1000000ull;
  g_over_until = window_end;
  g_release_stop = 0;
  memset(g_release, 0, sizeof(g_release));
  printf("schedbench: release=watchdog t0=%llu window_end=%llu\n",
         (unsigned long long)g_t0, (unsigned long long)window_end);
  for (id = 0; id < BENCH_TASKS; id++)
    {
      g_release[id].planned = planned[id];
      g_release[id].period_ms = g_period_ms[id];
      if (planned[id] == 0)
        {
          continue;
        }

      if (arm_release(&g_release[id]) != 0)
        {
          printf("schedbench: release arm %c failed\n", g_task_name[id]);
          release_quiesce();
          for (id = 0; id < BENCH_TASKS; id++)
            {
              if (pid[id] >= 0)
                {
                  task_delete(pid[id]);
                }
            }

          printf("SCHEDBENCH_FAIL\n");
          return 1;
        }
    }

  if (g_isolate_over && pid[2] >= 0)
    {
      sem_post(&g_rel[2]);
    }

  while ((g_release[0].next_job < planned[0] ||
          g_release[1].next_job < planned[1] ||
          g_release[2].next_job < planned[2]) &&
         mono_ns() < window_end + 3000000000ull)
    {
      usleep(1000);
    }

  /* No callback may run after this. The semaphores and post buffer
   * stay allocated, but they are not written again.
   */

  release_quiesce();
  for (id = 0; id < BENCH_TASKS; id++)
    {
      unsigned int job;
      unsigned int seen = 0;

      if (g_release[id].next_job != planned[id] ||
          g_release[id].overflow != 0 || g_release[id].arm_fail != 0)
        {
          printf("schedbench: release %c posted=%u planned=%u overflow=%u arm_fail=%u late_arm=%u\n",
                 g_task_name[id], g_release[id].next_job, planned[id],
                 g_release[id].overflow, g_release[id].arm_fail,
                 g_release[id].late_arm);
          failed = 1;
        }

      for (job = 0; job < planned[id]; job++)
        {
          if (g_post_ns[id][job] != 0)
            {
              seen++;
            }
        }

      if (seen != planned[id])
        {
          printf("schedbench: release %c unique_posts=%u planned=%u\n",
                 g_task_name[id], seen, planned[id]);
          failed = 1;
        }

      report_release_lateness(id, planned[id]);
    }

  while ((g_done[0] < planned[0] || g_done[1] < planned[1] ||
          g_done[2] < planned[2]) &&
         mono_ns() < window_end + 3000000000ull)
    {
      usleep(1000);
    }

  if (g_isolate_over && pid[2] >= 0)
    {
      while (g_over_done == 0 && mono_ns() < window_end + 3000000000ull)
        {
          usleep(1000);
        }

      if (g_over_done == 0)
        {
          task_delete(pid[2]);
          pid[2] = -1;
          failed = 1;
          printf("isolate: over_timeout\n");
        }
    }

  printf("SCHEDBENCH_CSV_BEGIN\n");
  printf("run_id,policy,task,job_index,nominal_release_ns,actual_release_ns,absolute_deadline_ns,job_start_ns,completion_ns,response_ns,tardiness_ns,deadline_missed,release_jitter_ns,completion_status,observed_policy,configured_priority,reservation_runtime_ns,reservation_deadline_ns,reservation_period_ns,preemptions,throttles,replenishments,sporadic_demotions,checksum,controller_post_ns,post_lateness_ns,dispatch_latency_ns,prio_start,prio_end,trace_overflow_flag\n");
  for (id = 0; id < BENCH_TASKS; id++)
    {
      unsigned int job;

      for (job = 0; job < planned[id]; job++)
        {
          if (job < g_done[id] && g_jobs[id][job].valid)
            {
              print_job_row(run_id, policy, id, job, &g_jobs[id][job], 0);
            }
          else
            {
              print_job_row(run_id, policy, id, job, NULL, 1);
              failed = 1;
            }
        }
    }

  printf("SCHEDBENCH_CSV_END\n");
  for (id = 0; id < BENCH_TASKS; id++)
    {
      if (g_done[id] < planned[id] && pid[id] >= 0)
        {
          task_delete(pid[id]);
        }

      if (deadline_mode)
        {
          printf("schedbench: cbs_abs %c %llu nominal0 %llu\n",
                 g_task_name[id],
                 (unsigned long long)g_cbs_abs[id],
                 (unsigned long long)g_t0);
        }
    }

  usleep(50000);
  ledger = sched_deadline_admitted();
  printf("schedbench: scenario=%s hp_ms=%u\n",
         scenario_name(), g_hp_ms);
  printf("schedbench: worker_ids=%u,%u,%u\n",
         (unsigned)g_id_hits[0], (unsigned)g_id_hits[1],
         (unsigned)g_id_hits[2]);
  printf("schedbench: prio_before_release %c=%d %c=%d %c=%d\n",
         g_task_name[0], g_prio_before[0],
         g_task_name[1], g_prio_before[1],
         g_task_name[2], g_prio_before[2]);
  printf("schedbench: ledger_armed_ppm=%lu\n",
         (unsigned long)g_ledger_armed);
  for (id = 0; id < BENCH_TASKS; id++)
    {
      if (deadline_mode)
        {
          printf("schedbench: readback %c policy=%d runtime=%llu deadline=%llu period=%llu\n",
                 g_task_name[id], g_rb_policy[id],
                 (unsigned long long)g_rb_runtime[id],
                 (unsigned long long)g_rb_deadline_ns[id],
                 (unsigned long long)g_rb_period_ns[id]);
        }
      else
        {
          printf("schedbench: readback %c policy=%d high=%d low=%d budget_ms=%u period_ms=%u\n",
                 g_task_name[id], g_rb_policy[id], g_rb_high[id],
                 g_rb_low[id], g_rb_budget_ms[id], g_rb_period_ms[id]);
        }
    }

  printf("schedbench: admitted_ppm=%lu\n", (unsigned long)ledger);
  if (deadline_mode && ledger != 0)
    {
      failed = 1;
    }

#ifdef CONFIG_SCHED_DEADLINE_TRACE
  {
    uint64_t win0 = 0;
    uint64_t win1 = 0;
    uint64_t lead = 0;
    uint64_t best = UINT64_MAX;
    unsigned int job;

    if (scenario == SCEN_ISOLATE)
      {
        win0 = g_t0;
        win1 = window_end;
      }
    else if (deadline_mode)
      {
        for (id = 0; id < BENCH_TASKS; id++)
          {
            for (job = 0; job < planned[id] && job < BENCH_MAX_JOBS; job++)
              {
                FAR const struct job_rec_s *rec = &g_jobs[id][job];
                uint64_t abs_dl;

                if (rec->valid == 0)
                  {
                    continue;
                  }

                abs_dl = rec->nominal_ns +
                         (uint64_t)g_period_ms[id] * 1000000ull;
                if (rec->completion_ns > abs_dl && rec->completion_ns < best)
                  {
                    best = rec->completion_ns;
                    lead = (uint64_t)g_period_ms[id] * 1000000ull;
                    win0 = rec->nominal_ns;
                    win1 = rec->completion_ns;
                  }
              }
          }
      }

    if (win0 > lead)
      {
        win0 -= lead;
      }

    sched_deadline_trace_report(win0, win1);
  }
#endif

  set_fifo(0, saved > 0 ? saved : 100);
  printf("SCHEDBENCH_%s\n", failed ? "FAIL" : "PASS");
  fflush(stdout);
  return failed ? 1 : 0;
}

static int cmd_overbudget(unsigned int iters)
{
  pid_t worker;
  pid_t observer;
  char arg0[] = "0";
  char arg1[] = "1";
  char name_w[] = "overdl";
  char name_o[] = "overob";
  struct sched_attr attr;
  int policy;
  uint32_t ledger;
  int failed = 0;
  uint64_t wall;

  ledger = sched_deadline_admitted();
  if (ledger != 0 || iters == 0)
    {
      printf("schedbench: overbudget start ledger=%lu iters=%u\n",
             (unsigned long)ledger, iters);
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  print_timing_limit();
  g_obs = 0;
  g_over_seen = 0;
  g_over_start = 0;
  g_over_end = 0;
  g_done[0] = 0;
  g_iters[0] = iters;
  sem_init(&g_park, 0, 0);
  sem_init(&g_rel[0], 0, 0);
  sem_init(&g_rel[1], 0, 0);
  if (spawn_parked(&worker, over_worker, name_w, arg0, BENCH_LOW_PRIO) != 0 ||
      spawn_parked(&observer, over_observer, name_o, arg1, BENCH_OBS_PRIO) != 0)
    {
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  memset(&attr, 0, sizeof(attr));
  attr.size = SCHED_ATTR_SIZE_VER0;
  attr.sched_policy = SCHED_DEADLINE;
  attr.sched_runtime = 40000000ull;
  attr.sched_deadline = 200000000ull;
  attr.sched_period = 200000000ull;
  if (sched_setattr(worker, &attr, 0) != 0)
    {
      printf("schedbench: overbudget setattr errno=%d\n", errno);
      task_delete(worker);
      task_delete(observer);
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  policy = sched_getscheduler(worker);
  printf("schedbench: overbudget policy=%d\n", policy);
  set_fifo(0, BENCH_CTRL_PRIO);
  if (policy != SCHED_DEADLINE)
    {
      failed = 1;
    }

  sched_lock();
  sem_post(&g_rel[1]);
  sem_post(&g_rel[0]);
  sched_unlock();

  {
    uint64_t limit = mono_ns() + 2000000000ull;

    while (g_done[0] == 0 && mono_ns() < limit)
      {
        usleep(1000);
      }
  }

  wall = (g_over_end >= g_over_start) ? (g_over_end - g_over_start) : 0;
  printf("schedbench: overbudget seen=%d checksum=%lu wall_ns=%llu obs=%d\n",
         g_over_seen, (unsigned long)g_over_sum, (unsigned long long)wall, g_obs);
  if (g_done[0] == 0)
    {
      task_delete(worker);
      failed = 1;
    }

  if (g_obs == 0)
    {
      task_delete(observer);
    }

  usleep(50000);
  ledger = sched_deadline_admitted();
  printf("schedbench: admitted_ppm=%lu\n", (unsigned long)ledger);

  /* The observer must have run before the compute kernel returned,
   * which is the throttle window, and the task must then have finished.
   */

  if (!g_over_seen || g_done[0] == 0 || policy != SCHED_DEADLINE ||
      ledger != 0 || wall < 40000000ull)
    {
      failed = 1;
    }

  printf("SCHEDBENCH_%s\n", failed ? "FAIL" : "PASS");
  fflush(stdout);
  return failed ? 1 : 0;
}

/****************************************************************************
 * Public Functions
 ****************************************************************************/

int main(int argc, FAR char *argv[])
{
  unsigned long iters_a;
  unsigned long iters_b;
  unsigned long iters_c;
  unsigned long hp;
  unsigned long run_id;

  if (argc < 2)
    {
      printf("usage: schedbench calibrate [targeted|stress90]\n");
      printf("       schedbench sporadic|deadline [targeted|stress90] <iters1> <iters2> <iters3> <hyperperiods> <run> [q105]\n");
      printf("       schedbench overbudget <iters>\n");
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  if (strcmp(argv[1], "calibrate") == 0)
    {
      if (argc == 3 && strcmp(argv[2], "targeted") == 0)
        {
          return cmd_calibrate(SCEN_TARGETED);
        }

      if (argc == 3 && strcmp(argv[2], "stress90") == 0)
        {
          return cmd_calibrate(SCEN_STRESS90);
        }

      if (argc == 3 && strcmp(argv[2], "isolate") == 0)
        {
          return cmd_calibrate(SCEN_ISOLATE);
        }

      if (argc == 2)
        {
          return cmd_calibrate(SCEN_STEP9);
        }

      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  if (strcmp(argv[1], "overbudget") == 0)
    {
      if (argc != 3)
        {
          printf("SCHEDBENCH_FAIL\n");
          return 1;
        }

      return cmd_overbudget((unsigned int)strtoul(argv[2], NULL, 10));
    }

  if (strcmp(argv[1], "sporadic") != 0 && strcmp(argv[1], "deadline") != 0)
    {
      printf("SCHEDBENCH_FAIL\n");
      return 1;
    }

  {
    int scenario = SCEN_STEP9;
    int base = 2;

    if (argc >= 3 && strcmp(argv[2], "targeted") == 0)
      {
        scenario = SCEN_TARGETED;
        base = 3;
      }
    else if (argc >= 3 && strcmp(argv[2], "stress90") == 0)
      {
        scenario = SCEN_STRESS90;
        base = 3;
      }
    else if (argc >= 3 && strcmp(argv[2], "isolate") == 0)
      {
        scenario = SCEN_ISOLATE;
        base = 3;
        g_isolate_over = 0;
        if (argc >= base + 6 && strcmp(argv[base + 5], "over") == 0)
          {
            g_isolate_over = 1;
          }
        else if (argc >= base + 6 && strcmp(argv[base + 5], "control") == 0)
          {
            g_isolate_over = 0;
          }
        else
          {
            printf("SCHEDBENCH_FAIL\n");
            return 1;
          }
      }

    g_diag_q105 = 0;
    if (argc == base + 6 && strcmp(argv[base + 5], "q105") == 0)
      {
        if (scenario != SCEN_TARGETED)
          {
            printf("SCHEDBENCH_FAIL\n");
            return 1;
          }

        g_diag_q105 = 1;
      }
    else if (scenario == SCEN_ISOLATE && argc == base + 6)
      {
      }
    else if (argc != base + 5)
      {
        printf("SCHEDBENCH_FAIL\n");
        return 1;
      }

    iters_a = strtoul(argv[base], NULL, 10);
    iters_b = strtoul(argv[base + 1], NULL, 10);
    iters_c = strtoul(argv[base + 2], NULL, 10);
    hp = strtoul(argv[base + 3], NULL, 10);
    run_id = strtoul(argv[base + 4], NULL, 10);
    if (iters_a == 0 || iters_b == 0 || iters_c == 0)
      {
        printf("SCHEDBENCH_FAIL\n");
        return 1;
      }

    return cmd_compare((unsigned int)run_id,
                       strcmp(argv[1], "deadline") == 0, scenario,
                       (unsigned int)iters_a, (unsigned int)iters_b,
                       (unsigned int)iters_c, (unsigned int)hp);
  }
}
