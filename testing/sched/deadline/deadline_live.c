/****************************************************************************
 * apps/testing/sched/deadline/deadline_live.c
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * Live SCHED_DEADLINE tasks: activation, EDF, block/wake, budget,
 * yield, and teardown. Timing sections do not print.
 *
 ****************************************************************************/

#include <nuttx/config.h>
#include <nuttx/sched.h>

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/****************************************************************************
 * Private Data
 ****************************************************************************/

static int g_phase;
static int g_obs;
static int g_order[4];
static int g_count;
static uint64_t g_seq[4];
static uint64_t g_abs[4];
static uint64_t g_remain[4];
static int g_fifo_ran;
static int g_saw;
static int g_child_policy;
static pid_t g_child_pid;
static sem_t g_ready;
static sem_t g_go;
static sem_t g_park;
static sem_t g_done;

#define MS(n)  ((uint64_t)(n) * 1000000ull)

/****************************************************************************
 * Private Functions
 ****************************************************************************/

static uint64_t mono_ns(void)
{
  struct timespec ts;

  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void burn_ns(uint64_t ns)
{
  uint64_t start = mono_ns();

  while (mono_ns() - start < ns)
    {
    }
}

static int wait_sem(FAR sem_t *sem, int sec)
{
  struct timespec ts;

  if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
    {
      return -1;
    }

  ts.tv_sec += sec;
  return sem_timedwait(sem, &ts);
}

static void fill_dl(FAR struct sched_attr *attr, uint64_t runtime,
                    uint64_t deadline, uint64_t period)
{
  memset(attr, 0, sizeof(*attr));
  attr->size           = sizeof(*attr);
  attr->sched_policy   = SCHED_DEADLINE;
  attr->sched_runtime  = runtime;
  attr->sched_deadline = deadline;
  attr->sched_period   = period;
}

static int spawn(FAR pthread_t *thread, int priority,
                 pthread_startroutine_t entry, FAR void *arg)
{
  pthread_attr_t attr;
  struct sched_param param;
  int rc;

  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, 8192);
  memset(&param, 0, sizeof(param));
  param.sched_priority = priority;
  pthread_attr_setschedpolicy(&attr, SCHED_FIFO);
  pthread_attr_setschedparam(&attr, &param);
  pthread_attr_setinheritsched(&attr, PTHREAD_EXPLICIT_SCHED);
  rc = pthread_create(thread, &attr, entry, arg);
  pthread_attr_destroy(&attr);
  return rc;
}

static int revert_self(void)
{
  struct sched_param param;
  int policy;

  policy = sched_getscheduler(0);
  if (policy != SCHED_DEADLINE)
    {
      return OK;
    }

  memset(&param, 0, sizeof(param));
  param.sched_priority = 100;
  return sched_setscheduler(0, SCHED_FIFO, &param);
}

static void note_case(int id)
{
  printf("livetest: ok %d\n", id);
}

static void *obs_after_phase(FAR void *arg)
{
  (void)arg;

  while (g_phase == 0)
    {
      usleep(1000);
    }

  g_obs = 1;
  return NULL;
}

static void *activate_worker(FAR void *arg)
{
  struct sched_attr attr;
  struct sched_attr got;
  struct sched_deadline_view_s view;

  (void)arg;
  fill_dl(&attr, MS(50), MS(100), MS(100));
  if (sched_setattr(0, &attr, 0) != 0 ||
      sched_getscheduler(0) != SCHED_DEADLINE)
    {
      g_saw = errno;
      sem_post(&g_done);
      return NULL;
    }

  memset(&got, 0, sizeof(got));
  if (sched_getattr(0, &got, sizeof(got), 0) != 0 ||
      got.sched_runtime != MS(50) ||
      got.sched_deadline != MS(100) ||
      got.sched_period != MS(100) ||
      sched_deadline_view(0, &view) != 0 ||
      view.runnable_seq == 0 ||
      view.sched != DL_SCHED_ELIGIBLE)
    {
      g_saw = EIO;
      revert_self();
      sem_post(&g_done);
      return NULL;
    }

  g_saw = 0;
  sem_post(&g_done);
  sem_wait(&g_park);
  revert_self();
  return NULL;
}

static int case_activate(void)
{
  pthread_t thread;
  int before;

  g_saw = -1;
  sem_init(&g_done, 0, 0);
  sem_init(&g_park, 0, 0);
  before = sched_getscheduler(0);
  if (spawn(&thread, 100, activate_worker, NULL) != 0)
    {
      return 1;
    }

  if (wait_sem(&g_done, 3) != 0 || g_saw != 0)
    {
      sem_post(&g_park);
      pthread_join(thread, NULL);
      return 1;
    }

  /* The worker is blocked in sem_wait, so this thread is running again. */

  if (sched_getscheduler(0) == SCHED_DEADLINE || before <= 0)
    {
      sem_post(&g_park);
      pthread_join(thread, NULL);
      return 1;
    }

  sem_post(&g_park);
  pthread_join(thread, NULL);
  sem_destroy(&g_done);
  sem_destroy(&g_park);
  if (sched_deadline_admitted() != 0)
    {
      return 1;
    }

  note_case(1);
  return 0;
}

static void *admit_holder(FAR void *arg)
{
  struct sched_attr attr;

  (void)arg;
  fill_dl(&attr, MS(90), MS(100), MS(100));
  g_saw = sched_setattr(0, &attr, 0) == 0 ? 0 : errno;
  sem_post(&g_ready);
  sem_wait(&g_park);
  revert_self();
  sem_post(&g_done);
  return NULL;
}

static void *admit_second(FAR void *arg)
{
  struct sched_attr attr;
  int policy;

  (void)arg;
  policy = sched_getscheduler(0);
  fill_dl(&attr, MS(6), MS(100), MS(100));
  if (sched_setattr(0, &attr, 0) == -1 && errno == EBUSY &&
      sched_getscheduler(0) == policy)
    {
      g_fifo_ran = 0;
    }
  else
    {
      g_fifo_ran = errno != 0 ? errno : EPERM;
      revert_self();
    }

  sem_post(&g_done);
  return NULL;
}

static int case_admit(void)
{
  pthread_t thread;
  pthread_t second;
  struct sched_attr attr;
  struct sched_attr got;
  struct sched_deadline_view_s view;
  uint32_t held;
  int rc;

  g_saw = -1;
  g_fifo_ran = -1;
  sem_init(&g_ready, 0, 0);
  sem_init(&g_park, 0, 0);
  sem_init(&g_done, 0, 0);
  if (spawn(&thread, 100, admit_holder, NULL) != 0)
    {
      return 2;
    }

  if (wait_sem(&g_ready, 3) != 0 || g_saw != 0)
    {
      sem_post(&g_park);
      pthread_join(thread, NULL);
      return 2;
    }

  held = sched_deadline_admitted();
  fill_dl(&attr, MS(96), MS(100), MS(100));
  rc = sched_setattr((pid_t)thread, &attr, 0);
  if (rc != -1 || errno != EBUSY || sched_deadline_admitted() != held)
    {
      printf("livetest: admit update rc=%d errno=%d held=%u now=%u\n",
             rc, errno, (unsigned int)held,
             (unsigned int)sched_deadline_admitted());
      sem_post(&g_park);
      wait_sem(&g_done, 3);
      pthread_join(thread, NULL);
      return 2;
    }

  memset(&got, 0, sizeof(got));
  if (sched_getattr((pid_t)thread, &got, sizeof(got), 0) != 0 ||
      got.sched_runtime != MS(90) ||
      sched_deadline_view((pid_t)thread, &view) != 0 ||
      view.runtime_ns != MS(90))
    {
      sem_post(&g_park);
      wait_sem(&g_done, 3);
      pthread_join(thread, NULL);
      return 2;
    }

  if (spawn(&second, 90, admit_second, NULL) != 0)
    {
      sem_post(&g_park);
      wait_sem(&g_done, 3);
      pthread_join(thread, NULL);
      return 2;
    }

  if (wait_sem(&g_done, 3) != 0 || g_fifo_ran != 0 ||
      sched_deadline_admitted() != held)
    {
      printf("livetest: admit second flag=%d held=%u now=%u\n",
             g_fifo_ran, (unsigned int)held,
             (unsigned int)sched_deadline_admitted());
      sem_post(&g_park);
      pthread_join(second, NULL);
      pthread_join(thread, NULL);
      return 2;
    }

  pthread_join(second, NULL);
  sem_post(&g_park);
  if (wait_sem(&g_done, 3) != 0)
    {
      pthread_join(thread, NULL);
      return 2;
    }

  pthread_join(thread, NULL);
  sem_destroy(&g_ready);
  sem_destroy(&g_park);
  sem_destroy(&g_done);
  if (sched_deadline_admitted() != 0)
    {
      return 2;
    }

  note_case(2);
  return 0;
}

struct edf_arg
{
  int id;
  sem_t go;
};

static void *edf_worker(FAR void *arg)
{
  struct edf_arg *spec = arg;
  struct sched_deadline_view_s view;
  int slot;

  sem_post(&g_ready);
  sem_wait(&spec->go);
  if (sched_deadline_view(0, &view) == 0)
    {
      g_seq[spec->id] = view.runnable_seq;
      g_abs[spec->id] = view.abs_deadline;
    }

  sched_lock();
  slot = g_count++;
  if (slot < 4)
    {
      g_order[slot] = spec->id;
    }

  sched_unlock();
  sem_post(&g_done);
  sem_wait(&g_park);
  revert_self();
  return NULL;
}

static void *fifo_observer(FAR void *arg)
{
  (void)arg;
  g_fifo_ran = 1;
  return NULL;
}

static int case_edf(void)
{
  pthread_t early;
  pthread_t late;
  pthread_t fifo;
  struct edf_arg early_arg;
  struct edf_arg late_arg;
  struct sched_attr attr;
  int i;

  early_arg.id = 1;
  late_arg.id = 2;
  sem_init(&early_arg.go, 0, 0);
  sem_init(&late_arg.go, 0, 0);
  sem_init(&g_ready, 0, 0);
  sem_init(&g_park, 0, 0);
  sem_init(&g_done, 0, 0);
  g_count = 0;
  g_fifo_ran = 0;
  g_seq[1] = 0;
  g_seq[2] = 0;
  if (spawn(&early, 80, edf_worker, &early_arg) != 0 ||
      spawn(&late, 80, edf_worker, &late_arg) != 0)
    {
      return 3;
    }

  if (wait_sem(&g_ready, 3) != 0 || wait_sem(&g_ready, 3) != 0)
    {
      return 3;
    }

  fill_dl(&attr, MS(20), MS(80), MS(80));
  if (sched_setattr((pid_t)early, &attr, 0) != 0)
    {
      return 3;
    }

  fill_dl(&attr, MS(20), MS(180), MS(180));
  if (sched_setattr((pid_t)late, &attr, 0) != 0)
    {
      return 3;
    }

  if (spawn(&fifo, 50, fifo_observer, NULL) != 0)
    {
      return 3;
    }

  /* Wake the later deadline first. Earliest deadline still runs first. */

  sched_lock();
  sem_post(&late_arg.go);
  sem_post(&early_arg.go);
  sched_unlock();

  if (wait_sem(&g_done, 3) != 0 || wait_sem(&g_done, 3) != 0)
    {
      return 3;
    }

  /* Parent outranks the FIFO observer, so it has had no turn yet. */

  if (g_count != 2 || g_order[0] != 1 || g_order[1] != 2 ||
      g_fifo_ran != 0 || g_abs[1] == 0 || g_abs[2] == 0 ||
      !(g_abs[1] < g_abs[2]))
    {
      sem_post(&g_park);
      sem_post(&g_park);
      pthread_join(early, NULL);
      pthread_join(late, NULL);
      pthread_join(fifo, NULL);
      return 3;
    }

  sem_post(&g_park);
  sem_post(&g_park);
  pthread_join(early, NULL);
  pthread_join(late, NULL);
  usleep(50000);
  pthread_join(fifo, NULL);
  if (g_fifo_ran != 1 || sched_deadline_admitted() != 0)
    {
      return 3;
    }

  for (i = 0; i < 4; i++)
    {
      g_order[i] = 0;
    }

  sem_destroy(&g_ready);
  sem_destroy(&early_arg.go);
  sem_destroy(&late_arg.go);
  sem_destroy(&g_park);
  sem_destroy(&g_done);
  note_case(3);
  return 0;
}

static int case_tie(void)
{
  pthread_t first;
  pthread_t second;
  struct edf_arg a;
  struct edf_arg b;
  struct sched_attr attr;
  int pass;

  a.id = 1;
  b.id = 2;
  for (pass = 0; pass < 2; pass++)
    {
      sem_init(&a.go, 0, 0);
      sem_init(&b.go, 0, 0);
      sem_init(&g_ready, 0, 0);
      sem_init(&g_park, 0, 0);
      sem_init(&g_done, 0, 0);
      g_count = 0;
      g_seq[1] = 0;
      g_seq[2] = 0;
      /* Above the parent (100) so each worker reaches sem_wait before
       * setattr. A still-runnable task would open its job at setattr.
       * With D=P that job is kept, so the later task would hold the
       * earlier deadline. Both jobs must open at the locked wake.
       */

      if (spawn(&first, 110, edf_worker, &a) != 0 ||
          spawn(&second, 110, edf_worker, &b) != 0 ||
          wait_sem(&g_ready, 3) != 0 || wait_sem(&g_ready, 3) != 0)
        {
          printf("livetest: tie pass=%d spawn/ready errno=%d\n",
                 pass, errno);
          return 4;
        }

      fill_dl(&attr, MS(20), MS(200), MS(200));
      if (sched_setattr((pid_t)first, &attr, 0) != 0 ||
          sched_setattr((pid_t)second, &attr, 0) != 0)
        {
          printf("livetest: tie pass=%d setattr errno=%d\n", pass, errno);
          return 4;
        }

      sched_lock();
      if (pass == 0)
        {
          sem_post(&a.go);
          sem_post(&b.go);
        }
      else
        {
          sem_post(&b.go);
          sem_post(&a.go);
        }

      sched_unlock();
      if (wait_sem(&g_done, 3) != 0 || wait_sem(&g_done, 3) != 0)
        {
          printf("livetest: tie pass=%d wait count=%d errno=%d\n",
                 pass, g_count, errno);
          return 4;
        }

      if (g_seq[1] == 0 || g_seq[2] == 0 || g_seq[1] == g_seq[2])
        {
          printf("livetest: tie pass=%d seq %llu %llu\n",
                 pass, (unsigned long long)g_seq[1],
                 (unsigned long long)g_seq[2]);
          sem_post(&g_park);
          sem_post(&g_park);
          pthread_join(first, NULL);
          pthread_join(second, NULL);
          return 4;
        }

      if (pass == 0 &&
          (g_order[0] != 1 || g_order[1] != 2 || !(g_seq[1] < g_seq[2])))
        {
          printf("livetest: tie pass=0 order %d %d seq %llu %llu abs %llu %llu\n",
                 g_order[0], g_order[1],
                 (unsigned long long)g_seq[1],
                 (unsigned long long)g_seq[2],
                 (unsigned long long)g_abs[1],
                 (unsigned long long)g_abs[2]);
          sem_post(&g_park);
          sem_post(&g_park);
          pthread_join(first, NULL);
          pthread_join(second, NULL);
          return 4;
        }

      if (pass == 1 &&
          (g_order[0] != 2 || g_order[1] != 1 || !(g_seq[2] < g_seq[1])))
        {
          printf("livetest: tie pass=1 order %d %d seq %llu %llu\n",
                 g_order[0], g_order[1],
                 (unsigned long long)g_seq[1],
                 (unsigned long long)g_seq[2]);
          sem_post(&g_park);
          sem_post(&g_park);
          pthread_join(first, NULL);
          pthread_join(second, NULL);
          return 4;
        }

      sem_post(&g_park);
      sem_post(&g_park);
      pthread_join(first, NULL);
      pthread_join(second, NULL);
      sem_destroy(&a.go);
      sem_destroy(&b.go);
      sem_destroy(&g_ready);
      sem_destroy(&g_park);
      sem_destroy(&g_done);
    }

  note_case(4);
  return 0;
}

static void *wait_worker(FAR void *arg)
{
  struct sched_deadline_view_s view;
  struct sched_attr attr;
  int step = (int)(uintptr_t)arg;

  fill_dl(&attr, MS(80), MS(200), MS(200));
  if (sched_setattr(0, &attr, 0) != 0)
    {
      g_saw = errno;
      sem_post(&g_ready);
      return NULL;
    }

  /* Spend enough of the budget that a short wait keeps the job.
   * Tickless has no periodic tick, so this burn may be charged at
   * the block edge rather than before the next sample.
   */

  if (sched_deadline_view(0, &view) != 0)
    {
      g_saw = EIO;
      sem_post(&g_ready);
      revert_self();
      return NULL;
    }

  if (step == 0)
    {
      g_remain[2] = view.remaining_ns;
    }

  burn_ns(MS(50));
  if (sched_deadline_view(0, &view) != 0)
    {
      g_saw = EIO;
      sem_post(&g_ready);
      revert_self();
      return NULL;
    }

  g_abs[step] = view.abs_deadline;
  g_remain[step] = view.remaining_ns;
  g_saw = 0;
  sem_post(&g_ready);
  sem_wait(&g_go);
  if (sched_deadline_view(0, &view) != 0)
    {
      g_saw = EIO;
    }
  else
    {
      g_abs[step + 1] = view.abs_deadline;
      g_remain[step + 1] = view.remaining_ns;
      g_seq[step] = view.sched;
    }

  sem_post(&g_done);
  revert_self();
  return NULL;
}

static int case_wait(void)
{
  pthread_t thread;
  uint64_t drop;

  g_saw = -1;
  sem_init(&g_ready, 0, 0);
  sem_init(&g_go, 0, 0);
  sem_init(&g_done, 0, 0);

  if (spawn(&thread, 80, wait_worker, (void *)0) != 0 ||
      wait_sem(&g_ready, 3) != 0 || g_saw != 0)
    {
      return 5;
    }

  usleep(20000);
  sem_post(&g_go);
  if (wait_sem(&g_done, 3) != 0)
    {
      return 5;
    }

  drop = g_remain[0] > g_remain[1] ? g_remain[0] - g_remain[1] : 0;
  {
    uint64_t burned = g_remain[2] > g_remain[0] ? g_remain[2] - g_remain[0] : 0;
    uint64_t total = g_remain[2] > g_remain[1] ? g_remain[2] - g_remain[1] : 0;

    /* Periodic ticks charge the burn before the block sample, so the
     * blocked window must stay small. Tickless charges that same burn
     * once, at the block. Either way the 20 ms sleep is not a second
     * charge, and the absolute deadline is unchanged.
     */

    if (g_abs[0] != g_abs[1] || g_seq[0] != DL_SCHED_ELIGIBLE ||
        total < MS(20) || total > MS(90) ||
        (burned >= MS(20) ? drop > MS(25) :
         (drop < MS(20) || drop > MS(65))))
      {
        printf("livetest: preserve abs %llu->%llu drop=%llu burned=%llu total=%llu sched=%llu\n",
               (unsigned long long)g_abs[0], (unsigned long long)g_abs[1],
               (unsigned long long)drop, (unsigned long long)burned,
               (unsigned long long)total, (unsigned long long)g_seq[0]);
        pthread_join(thread, NULL);
        return 5;
      }
  }

  sem_destroy(&g_ready);
  sem_destroy(&g_go);
  sem_destroy(&g_done);

  /* Second job: the deadline passes while the task is blocked. */

  g_saw = -1;
  sem_init(&g_ready, 0, 0);
  sem_init(&g_go, 0, 0);
  sem_init(&g_done, 0, 0);
  if (spawn(&thread, 80, wait_worker, (void *)0) != 0 ||
      wait_sem(&g_ready, 3) != 0 || g_saw != 0)
    {
      return 5;
    }

  usleep(500000);
  sem_post(&g_go);
  if (wait_sem(&g_done, 3) != 0)
    {
      pthread_join(thread, NULL);
      return 5;
    }

  pthread_join(thread, NULL);
  if (!(g_abs[1] > g_abs[0]) || g_remain[1] + MS(40) < MS(80) ||
      g_seq[0] != DL_SCHED_ELIGIBLE || sched_deadline_admitted() != 0)
    {
      printf("livetest: reset abs %llu->%llu remain=%llu sched=%llu\n",
             (unsigned long long)g_abs[0], (unsigned long long)g_abs[1],
             (unsigned long long)g_remain[1], (unsigned long long)g_seq[0]);
      return 5;
    }

  sem_destroy(&g_ready);
  sem_destroy(&g_go);
  sem_destroy(&g_done);
  note_case(5);
  return 0;
}

static void *burn_worker(FAR void *arg)
{
  struct sched_attr attr;
  uintptr_t kind = (uintptr_t)arg;

  if (kind == 1)
    {
      fill_dl(&attr, MS(80), MS(400), MS(400));
    }
  else if (kind == 2)
    {
      fill_dl(&attr, MS(40), MS(300), MS(300));
    }
  else
    {
      fill_dl(&attr, MS(200), MS(250), MS(250));
    }

  if (sched_setattr(0, &attr, 0) != 0)
    {
      g_saw = errno;
      sem_post(&g_done);
      return NULL;
    }

  g_phase = 1;
  if (kind == 2)
    {
      struct sched_deadline_view_s view;

      sched_lock();
      burn_ns(MS(80));
      if (sched_deadline_view(0, &view) == 0)
        {
          g_remain[0] = view.overrun_ns;
          g_seq[0] = view.flags;
        }

      g_saw = g_obs;
      sched_unlock();
    }
  else if (kind == 3)
    {
      sched_yield();
    }
  else
    {
      uint64_t start = mono_ns();

      while (g_obs == 0 && mono_ns() - start < 2000000000ull)
        {
        }
    }

  sem_post(&g_done);
  revert_self();
  return NULL;
}

static int case_deplete(void)
{
  pthread_t worker;
  pthread_t obs;

  g_phase = 0;
  g_obs = 0;
  g_saw = 0;
  sem_init(&g_done, 0, 0);
  if (spawn(&obs, 60, obs_after_phase, NULL) != 0 ||
      spawn(&worker, 80, burn_worker, (void *)1) != 0)
    {
      return 6;
    }

  if (wait_sem(&g_done, 3) != 0 || g_obs != 1)
    {
      return 6;
    }

  pthread_join(worker, NULL);
  pthread_join(obs, NULL);
  sem_destroy(&g_done);
  if (sched_deadline_admitted() != 0)
    {
      return 6;
    }

  note_case(6);
  return 0;
}

static void *switch_worker(FAR void *arg)
{
  struct sched_attr attr;
  struct sched_deadline_view_s view;

  (void)arg;
  fill_dl(&attr, MS(200), MS(500), MS(500));
  if (sched_setattr(0, &attr, 0) != 0 ||
      sched_deadline_view(0, &view) != 0)
    {
      g_saw = errno != 0 ? errno : EIO;
      sem_post(&g_ready);
      return NULL;
    }

  g_remain[0] = view.remaining_ns;
  burn_ns(MS(40));
  if (sched_deadline_view(0, &view) != 0)
    {
      g_saw = EIO;
      sem_post(&g_ready);
      revert_self();
      return NULL;
    }

  g_remain[1] = view.remaining_ns;
  g_saw = 0;
  sem_post(&g_ready);
  sem_wait(&g_go);
  if (sched_deadline_view(0, &view) != 0)
    {
      g_saw = EIO;
    }
  else
    {
      g_remain[2] = view.remaining_ns;
    }

  sem_post(&g_done);
  revert_self();
  return NULL;
}

static int case_switch_charge(void)
{
  pthread_t thread;
  uint64_t ran;
  uint64_t blocked;

  g_saw = -1;
  sem_init(&g_ready, 0, 0);
  sem_init(&g_go, 0, 0);
  sem_init(&g_done, 0, 0);
  if (spawn(&thread, 80, switch_worker, NULL) != 0 ||
      wait_sem(&g_ready, 3) != 0 || g_saw != 0)
    {
      printf("livetest: charge setup saw=%d\n", g_saw);
      return 7;
    }

  usleep(50000);
  sem_post(&g_go);
  if (wait_sem(&g_done, 3) != 0)
    {
      pthread_join(thread, NULL);
      return 7;
    }

  pthread_join(thread, NULL);
  ran = g_remain[0] > g_remain[1] ? g_remain[0] - g_remain[1] : 0;
  blocked = g_remain[1] > g_remain[2] ? g_remain[1] - g_remain[2] : 0;
  {
    uint64_t total = g_remain[0] > g_remain[2] ? g_remain[0] - g_remain[2] : 0;

    /* 40 ms of running is charged once. Periodic mode charges it before
     * the block sample. Tickless charges it at the block. The following
     * 50 ms sleep is not added on top.
     */

    if (sched_deadline_admitted() != 0 || total < MS(20) || total > MS(80) ||
        (ran >= MS(20) ? (ran > MS(80) || blocked > MS(25)) :
         (blocked < MS(20) || blocked > MS(80))))
      {
        printf("livetest: charge ran=%llu blocked=%llu total=%llu r0=%llu r1=%llu r2=%llu\n",
               (unsigned long long)ran, (unsigned long long)blocked,
               (unsigned long long)total,
               (unsigned long long)g_remain[0],
               (unsigned long long)g_remain[1],
               (unsigned long long)g_remain[2]);
        return 7;
      }
  }

  sem_destroy(&g_ready);
  sem_destroy(&g_go);
  sem_destroy(&g_done);
  note_case(7);
  return 0;
}

static int case_locked(void)
{
  pthread_t worker;
  pthread_t obs;

  g_phase = 0;
  g_obs = 0;
  g_saw = 1;
  g_remain[0] = 0;
  sem_init(&g_done, 0, 0);
  if (spawn(&obs, 60, obs_after_phase, NULL) != 0 ||
      spawn(&worker, 80, burn_worker, (void *)2) != 0)
    {
      return 8;
    }

  if (wait_sem(&g_done, 3) != 0 || g_saw != 0 || g_remain[0] == 0 ||
      (g_seq[0] & DEADLINE_FLAG_THROTTLE_PENDING) == 0 || g_obs != 1)
    {
      printf("livetest: lock obs_during=%d overrun=%llu flags=%llu obs=%d\n",
             g_saw, (unsigned long long)g_remain[0],
             (unsigned long long)g_seq[0], g_obs);
      pthread_join(worker, NULL);
      pthread_join(obs, NULL);
      return 8;
    }

  pthread_join(worker, NULL);
  pthread_join(obs, NULL);
  sem_destroy(&g_done);
  if (sched_deadline_admitted() != 0)
    {
      return 8;
    }

  note_case(8);
  return 0;
}

static void *yield_fifo(FAR void *arg)
{
  (void)arg;
  sem_post(&g_ready);
  sem_wait(&g_go);
  g_fifo_ran = 1;
  sched_yield();
  g_saw = g_obs;
  sem_post(&g_done);
  return NULL;
}

static void *yield_peer(FAR void *arg)
{
  (void)arg;
  sem_post(&g_ready);
  sem_wait(&g_park);
  g_obs = 1;
  sem_post(&g_done);
  return NULL;
}

static int case_yield(void)
{
  pthread_t worker;
  pthread_t obs;
  pthread_t peer;

  g_phase = 0;
  g_obs = 0;
  sem_init(&g_done, 0, 0);
  if (spawn(&obs, 60, obs_after_phase, NULL) != 0 ||
      spawn(&worker, 80, burn_worker, (void *)3) != 0)
    {
      return 9;
    }

  if (wait_sem(&g_done, 3) != 0 || g_obs != 1 ||
      sched_deadline_admitted() != 0)
    {
      pthread_join(worker, NULL);
      pthread_join(obs, NULL);
      return 9;
    }

  pthread_join(worker, NULL);
  pthread_join(obs, NULL);
  sem_destroy(&g_done);

  /* Same-priority FIFO yield still rotates. */

  g_phase = 0;
  g_obs = 0;
  g_fifo_ran = 0;
  g_saw = 0;
  sem_init(&g_ready, 0, 0);
  sem_init(&g_go, 0, 0);
  sem_init(&g_park, 0, 0);
  sem_init(&g_done, 0, 0);
  if (spawn(&worker, 70, yield_fifo, NULL) != 0 ||
      spawn(&peer, 70, yield_peer, NULL) != 0 ||
      wait_sem(&g_ready, 3) != 0 || wait_sem(&g_ready, 3) != 0)
    {
      return 9;
    }

  /* Release the yielder first. Its yield must let the peer run. */

  sched_lock();
  sem_post(&g_go);
  sem_post(&g_park);
  sched_unlock();

  if (wait_sem(&g_done, 3) != 0 || wait_sem(&g_done, 3) != 0 || g_saw != 1)
    {
      pthread_join(worker, NULL);
      pthread_join(peer, NULL);
      return 9;
    }

  pthread_join(worker, NULL);
  pthread_join(peer, NULL);
  sem_destroy(&g_ready);
  sem_destroy(&g_go);
  sem_destroy(&g_park);
  sem_destroy(&g_done);
  note_case(9);
  return 0;
}

static int exit_spin(int argc, FAR char *argv[])
{
  struct sched_attr attr;

  (void)argc;
  (void)argv;
  fill_dl(&attr, MS(40), MS(400), MS(400));
  if (sched_setattr(0, &attr, 0) != 0)
    {
      return 1;
    }

  g_phase = 1;
  while (g_obs == 0)
    {
    }

  return 0;
}

static int exit_block(int argc, FAR char *argv[])
{
  struct sched_attr attr;

  (void)argc;
  (void)argv;
  fill_dl(&attr, MS(40), MS(400), MS(400));
  if (sched_setattr(0, &attr, 0) != 0)
    {
      sem_post(&g_ready);
      return 1;
    }

  sem_post(&g_ready);
  sem_wait(&g_park);
  return 0;
}

static int exit_once(int argc, FAR char *argv[])
{
  struct sched_attr attr;

  (void)argc;
  (void)argv;
  fill_dl(&attr, MS(30), MS(100), MS(100));
  sched_setattr(0, &attr, 0);
  return 0;
}

static int wait_gone(pid_t pid)
{
  int i;

  for (i = 0; i < 400; i++)
    {
      if (sched_getscheduler(pid) < 0)
        {
          return 0;
        }

      usleep(5000);
    }

  return -1;
}

static int case_exit(void)
{
  pid_t pid;
  int i;
  struct sched_deadline_view_s view;

  for (i = 0; i < 3; i++)
    {
      pid = task_create("dl-exit", 100, 8192, exit_once, NULL);
      if (pid < 0 || wait_gone(pid) != 0 || sched_deadline_admitted() != 0)
        {
          return 10;
        }
    }

  sem_init(&g_ready, 0, 0);
  sem_init(&g_park, 0, 0);
  pid = task_create("dl-block", 100, 8192, exit_block, NULL);
  if (pid < 0 || wait_sem(&g_ready, 3) != 0)
    {
      return 10;
    }

  task_delete(pid);
  if (wait_gone(pid) != 0 || sched_deadline_admitted() != 0)
    {
      return 10;
    }

  g_phase = 0;
  g_obs = 0;
  pid = task_create("dl-spin", 100, 8192, exit_spin, NULL);
  if (pid < 0)
    {
      return 10;
    }

  for (i = 0; i < 400 && g_phase == 0; i++)
    {
      usleep(5000);
    }

  task_delete(pid);
  if (g_phase == 0 || wait_gone(pid) != 0 || sched_deadline_admitted() != 0)
    {
      return 10;
    }

  g_phase = 0;
  g_obs = 0;
  pid = task_create("dl-thr", 100, 8192, exit_spin, NULL);
  if (pid < 0)
    {
      return 10;
    }

  for (i = 0; i < 400; i++)
    {
      if (sched_deadline_view(pid, &view) == 0 &&
          view.sched == DL_SCHED_THROTTLED)
        {
          break;
        }

      usleep(5000);
    }

  if (i == 400)
    {
      task_delete(pid);
      return 10;
    }

  task_delete(pid);
  if (wait_gone(pid) != 0 || sched_deadline_admitted() != 0)
    {
      return 10;
    }

  sem_destroy(&g_ready);
  sem_destroy(&g_park);
  note_case(10);
  return 0;
}

static int child_record(int argc, FAR char *argv[])
{
  (void)argc;
  (void)argv;
  g_child_policy = sched_getscheduler(0);
  g_child_pid = getpid();
  sem_post(&g_ready);
  return 0;
}

static void *fork_worker(FAR void *arg)
{
  pthread_attr_t attr;
  pthread_t child;
  struct sched_attr dl;
  int rc;
  uint32_t held;

  (void)arg;
  fill_dl(&dl, MS(30), MS(200), MS(200));
  if (sched_setattr(0, &dl, 0) != 0)
    {
      g_saw = errno;
      sem_post(&g_done);
      return NULL;
    }

  held = sched_deadline_admitted();
  g_child_policy = -1;
  g_child_pid = 0;
  rc = task_create("dl-child", 90, 8192, child_record, NULL);
  if (rc < 0)
    {
      g_saw = errno;
      revert_self();
      sem_post(&g_done);
      return NULL;
    }

  if (sem_wait(&g_ready) != 0 || g_child_policy < 0)
    {
      g_saw = EIO;
      revert_self();
      sem_post(&g_done);
      return NULL;
    }

  if (g_child_policy == SCHED_DEADLINE || sched_deadline_admitted() != held)
    {
      g_saw = EBUSY;
      revert_self();
      sem_post(&g_done);
      return NULL;
    }

  pthread_attr_init(&attr);
  pthread_attr_setinheritsched(&attr, PTHREAD_INHERIT_SCHED);
  rc = pthread_create(&child, &attr, obs_after_phase, NULL);
  pthread_attr_destroy(&attr);
  if (rc != EAGAIN)
    {
      g_saw = rc == 0 ? EPERM : rc;
      if (rc == 0)
        {
          pthread_join(child, NULL);
        }

      revert_self();
      sem_post(&g_done);
      return NULL;
    }

#ifdef CONFIG_ARCH_HAVE_FORK
  rc = fork();
  if (rc == 0)
    {
      _exit(0);
    }

  if (rc > 0 || errno != EAGAIN)
    {
      g_saw = rc > 0 ? EPERM : errno;
      revert_self();
      sem_post(&g_done);
      return NULL;
    }
#else
  printf("livetest: fork not in this configuration\n");
#endif

#ifdef CONFIG_ARCH_HAVE_VFORK
  rc = vfork();
  if (rc == 0)
    {
      _exit(0);
    }

  if (rc > 0 || errno != EAGAIN)
    {
      g_saw = rc > 0 ? EPERM : errno;
      revert_self();
      sem_post(&g_done);
      return NULL;
    }
#else
  printf("livetest: vfork not in this configuration\n");
#endif

  g_saw = 0;
  revert_self();
  if (sched_deadline_admitted() != held - 300000 &&
      sched_deadline_admitted() != 0)
    {
      /* held included this task. After revert the ledger is the
       * child's nothing: the child did not reserve. Accept zero.
       */

      if (sched_deadline_admitted() != 0)
        {
          g_saw = EBUSY;
        }
    }

  sem_post(&g_done);
  return NULL;
}

static int case_fork(void)
{
  pthread_t thread;

  g_saw = -1;
  sem_init(&g_ready, 0, 0);
  sem_init(&g_done, 0, 0);
  if (spawn(&thread, 80, fork_worker, NULL) != 0 ||
      wait_sem(&g_done, 3) != 0 || g_saw != 0 ||
      sched_deadline_admitted() != 0)
    {
      printf("livetest: fork saw=%d admitted=%u\n",
             g_saw, (unsigned int)sched_deadline_admitted());
      pthread_join(thread, NULL);
      return 11;
    }

  pthread_join(thread, NULL);
  sem_destroy(&g_ready);
  sem_destroy(&g_done);
  note_case(11);
  return 0;
}

static int boundary_worker(int argc, FAR char *argv[])
{
  struct sched_attr attr;
  struct sched_deadline_view_s view;
  int i;

  (void)argc;
  (void)argv;

  /* Ack before any deadline conversion or burn. The parent is the NSH
   * task at priority 100. This worker is started above that, so the
   * ack is not delayed by the parent staying runnable.
   */

  g_saw = 1;
  sem_post(&g_ready);
  if (sem_wait(&g_go) != 0)
    {
      return 1;
    }

  fill_dl(&attr, MS(2), MS(200), MS(200));
  if (sched_setattr(0, &attr, 0) != 0)
    {
      g_saw = errno != 0 ? errno : EIO;
      g_obs = -1;
      sem_post(&g_done);
      return 1;
    }

  for (i = 0; i < 24; i++)
    {
      int spins;
      uint64_t held;
      uint64_t prev;

      /* Remaining is published only when the scheduler accounts, in
       * about 1 ms steps. Catch the tick that leaves a fraction of a
       * tick, then burn slightly more than that fraction before
       * blocking. The block account is what must cross zero. Sliding
       * the start each attempt changes where that fraction falls.
       */

      burn_ns((uint64_t)i * 80000ull);
      held = 0;
      prev = UINT64_MAX;
      for (spins = 0; spins < 400; spins++)
        {
          if (sched_deadline_view(0, &view) == 0 &&
              view.sched == DL_SCHED_ELIGIBLE &&
              view.remaining_ns > 0)
            {
              if (prev != UINT64_MAX &&
                  view.remaining_ns != prev &&
                  view.remaining_ns <= 600000ull)
                {
                  held = view.remaining_ns;
                  break;
                }

              prev = view.remaining_ns;
            }
          else
            {
              prev = UINT64_MAX;
            }

          burn_ns(40000ull);
        }

      if (held == 0)
        {
          continue;
        }

      burn_ns(held + 80000ull);
      if (sched_deadline_view(0, &view) != 0 ||
          view.sched != DL_SCHED_ELIGIBLE ||
          view.remaining_ns != held)
        {
          continue;
        }

      g_remain[0] = held;
      g_phase = 1;
      g_abs[0] = mono_ns();
      sem_wait(&g_park);
      g_abs[1] = mono_ns();
      if (g_phase == 2 || (g_abs[1] - g_abs[0]) < MS(20))
        {
          /* Either the block did not exhaust the budget, or the
           * semaphore was already posted so this wait did not block
           * across replenishment.
           */

          g_phase = 0;
          continue;
        }

      g_seq[0] = 1;
      g_obs = 1;
      sem_post(&g_done);
      revert_self();
      return 0;
    }

  g_obs = -1;
  sem_post(&g_done);
  revert_self();
  return 1;
}

static void boundary_cleanup(pid_t pid)
{
  if (pid >= 0 && sched_getscheduler(pid) >= 0)
    {
      task_delete(pid);
      wait_gone(pid);
    }

  sem_destroy(&g_ready);
  sem_destroy(&g_go);
  sem_destroy(&g_park);
  sem_destroy(&g_done);
}

static int drive_boundary(int post_after_replenish)
{
  pid_t pid;
  struct sched_deadline_view_s view;
  const char *name;
  int posted = 0;
  int saw_throttled = 0;
  int saw_eligible = 0;
  int i;
  int waitrc;
  int sched_at_post = -1;
  uint64_t dt_ms = 0;

  name = post_after_replenish ? "replenish-then-post" : "post-while-throttled";
  g_obs = 0;
  g_saw = 0;
  g_phase = 0;
  g_seq[0] = 0;
  g_remain[0] = 0;
  g_abs[0] = 0;
  g_abs[1] = 0;
  sem_init(&g_ready, 0, 0);
  sem_init(&g_go, 0, 0);
  sem_init(&g_park, 0, 0);
  sem_init(&g_done, 0, 0);

  /* 220 is above NSH (100) and the high-priority work queue (192).
   * task_create then runs this worker before the parent continues.
   * The parent blocks on g_ready and does not hold sched_lock.
   */

  pid = task_create("dl-edge", 220, 8192, boundary_worker, NULL);
  waitrc = pid < 0 ? -1 : wait_sem(&g_ready, 3);
  if (pid < 0 || waitrc != 0 || g_saw != 1)
    {
      printf("livetest: boundary order=%s FAIL handshake pid=%d saw=%d wait=%d errno=%d\n",
             name, (int)pid, g_saw, waitrc, errno);
      boundary_cleanup(pid);
      return -1;
    }

  sem_post(&g_go);
  memset(&view, 0, sizeof(view));
  for (i = 0; i < 15000 && g_obs == 0; i++)
    {
      if (posted && g_phase == 0 && g_obs == 0)
        {
          posted = 0;
          sched_at_post = -1;
          saw_throttled = 0;
          saw_eligible = 0;
        }

      if (sched_deadline_view(pid, &view) != 0)
        {
          usleep(1000);
          continue;
        }

      if (g_phase == 1 && view.sched == DL_SCHED_BLOCKED && !posted)
        {
          /* The blocking account did not exhaust the budget.
           * Release the worker so it can walk up to the edge again.
           */

          g_phase = 2;
          sem_post(&g_park);
        }

      if (g_phase == 1 && view.sched == DL_SCHED_THROTTLED)
        {
          saw_throttled = 1;
          if (!post_after_replenish && !posted)
            {
              sched_at_post = (int)view.sched;
              sem_post(&g_park);
              posted = 1;
            }
        }

      if (post_after_replenish && saw_throttled && !posted &&
          g_phase == 1 && view.sched == DL_SCHED_ELIGIBLE &&
          view.remaining_ns > 0)
        {
          saw_eligible = 1;
          sched_at_post = (int)view.sched;
          sem_post(&g_park);
          posted = 1;
        }

      usleep(1000);
    }

  if (g_abs[1] >= g_abs[0])
    {
      dt_ms = (g_abs[1] - g_abs[0]) / 1000000ull;
    }

  if (wait_sem(&g_done, 2) != 0 || g_obs != 1 || g_seq[0] != 1 ||
      !saw_throttled || sched_at_post < 0 || dt_ms < 20 ||
      (post_after_replenish && !saw_eligible) ||
      (!post_after_replenish && sched_at_post != DL_SCHED_THROTTLED) ||
      (post_after_replenish && sched_at_post != DL_SCHED_ELIGIBLE) ||
      wait_gone(pid) != 0 || sched_deadline_admitted() != 0)
    {
      printf("livetest: boundary order=%s FAIL throttled=%d eligible=%d edge=%llu remain=%llu dt_ms=%llu sched_at_post=%d obs=%d ledger=%lu\n",
             name, saw_throttled, saw_eligible,
             (unsigned long long)g_seq[0],
             (unsigned long long)g_remain[0],
             (unsigned long long)dt_ms, sched_at_post, g_obs,
             (unsigned long)sched_deadline_admitted());
      boundary_cleanup(pid);
      return -1;
    }

  printf("livetest: boundary order=%s PASS throttled=%d eligible=%d edge=%llu remain=%llu dt_ms=%llu sched_at_post=%d ledger=%lu\n",
         name, saw_throttled, saw_eligible,
         (unsigned long long)g_seq[0],
         (unsigned long long)g_remain[0],
         (unsigned long long)dt_ms, sched_at_post,
         (unsigned long)sched_deadline_admitted());
  boundary_cleanup(-1);
  return 0;
}

static int case_boundary(void)
{
  if (drive_boundary(0) != 0 || drive_boundary(1) != 0)
    {
      return 13;
    }

  if (sched_deadline_admitted() != 0)
    {
      printf("livetest: boundary ledger %lu\n",
             (unsigned long)sched_deadline_admitted());
      return 13;
    }

  note_case(13);
  return 0;
}

int deadline_boundary_test(void)
{
  return case_boundary();
}

static int case_legacy(void)
{
  struct sched_param param;
  int policy;
  int rc;

  if (sched_deadline_admitted() != 0)
    {
      return 12;
    }

  policy = sched_getscheduler(0);
  memset(&param, 0, sizeof(param));
  param.sched_priority = 100;
  rc = sched_setscheduler(0, SCHED_FIFO, &param);
  if (rc != 0 || sched_getscheduler(0) != SCHED_FIFO)
    {
      return 12;
    }

  if (sched_yield() != 0)
    {
      return 12;
    }

  memset(&param, 0, sizeof(param));
  param.sched_priority = 100;
  if (policy != SCHED_FIFO)
    {
      sched_setscheduler(0, policy, &param);
    }

  note_case(12);
  return 0;
}

/****************************************************************************
 * Public Functions
 ****************************************************************************/

int deadline_live_test(void)
{
  int rc;

  if (sched_deadline_admitted() != 0)
    {
      printf("livetest: FAIL ledger not clear at start\n");
      return 1;
    }

  rc = case_activate();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_admit();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_edf();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_tie();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_wait();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_deplete();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_switch_charge();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_locked();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_yield();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_exit();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_fork();
  if (rc != 0)
    {
      return rc;
    }

  rc = case_boundary();
  if (rc != 0)
    {
      return rc;
    }

  return case_legacy();
}

static sem_t g_t9_hready;
static sem_t g_t9_wake;
static volatile int g_t9_ran;
static volatile int g_t9_hpid;
static volatile int g_t9_lpid;

static void *t9_high(FAR void *arg)
{
  struct sched_attr attr;

  (void)arg;
  fill_dl(&attr, MS(20), MS(40), MS(40));
  if (sched_setattr(0, &attr, 0) != 0)
    {
      g_t9_hpid = -errno;
      sem_post(&g_t9_hready);
      return NULL;
    }

  g_t9_hpid = (int)gettid();
  sem_post(&g_t9_hready);
  if (sem_wait(&g_t9_wake) == 0)
    {
      g_t9_ran = (int)gettid();
    }

  revert_self();
  return NULL;
}

static void *t9_low(FAR void *arg)
{
  struct sched_attr attr;
  unsigned long spins;

  (void)arg;
  fill_dl(&attr, MS(50), MS(200), MS(200));
  if (sched_setattr(0, &attr, 0) != 0)
    {
      g_t9_lpid = -errno;
      return NULL;
    }

  g_t9_lpid = (int)gettid();
  sched_deadline_t9_expect((pid_t)g_t9_lpid, (pid_t)g_t9_hpid);
  if (sched_deadline_t9_kick(&g_t9_wake) != 0)
    {
      g_t9_lpid = -EAGAIN;
      revert_self();
      return NULL;
    }

  for (spins = 0; spins < 50000000ul && g_t9_ran == 0; spins++)
    {
    }

  revert_self();
  return NULL;
}

int deadline_t9_irq_test(void)
{
  pthread_t high;
  pthread_t low;
  struct sched_deadline_t9_s wit;
  int charged_low;
  int high_unchanged;

  g_t9_ran = 0;
  g_t9_hpid = 0;
  g_t9_lpid = 0;
  sem_init(&g_t9_hready, 0, 0);
  sem_init(&g_t9_wake, 0, 0);
  if (spawn(&high, 120, t9_high, NULL) != 0)
    {
      printf("deadline t9: UNVERIFIED spawn high errno=%d\n", errno);
      return 2;
    }

  if (wait_sem(&g_t9_hready, 2) != 0 || g_t9_hpid <= 0)
    {
      printf("deadline t9: UNVERIFIED high pid=%d\n", g_t9_hpid);
      pthread_join(high, NULL);
      return 2;
    }

  if (spawn(&low, 110, t9_low, NULL) != 0)
    {
      printf("deadline t9: UNVERIFIED spawn low errno=%d\n", errno);
      sem_post(&g_t9_wake);
      pthread_join(high, NULL);
      return 2;
    }

  pthread_join(low, NULL);
  if (g_t9_ran == 0)
    {
      sem_post(&g_t9_wake);
    }

  pthread_join(high, NULL);
  sched_deadline_t9_read(&wit);
  printf("deadline t9: low=%d high=%d ran=%d\n",
         g_t9_lpid, g_t9_hpid, g_t9_ran);
  printf("deadline t9: tick_irq=%d tick_running=%d tick_head=%d "
         "tick_now=%llu tick_rem=%llu\n",
         wit.tick_irq, wit.tick_running, wit.tick_head,
         (unsigned long long)wit.tick_now,
         (unsigned long long)wit.tick_rem);
  printf("deadline t9: sw_irq=%d from=%d to=%d running=%d head=%d "
         "now=%llu from_before=%llu from_after=%llu to_before=%llu "
         "to_after=%llu\n",
         wit.sw_irq, wit.sw_from, wit.sw_to, wit.sw_running, wit.sw_head,
         (unsigned long long)wit.sw_now,
         (unsigned long long)wit.sw_from_before,
         (unsigned long long)wit.sw_from_after,
         (unsigned long long)wit.sw_to_before,
         (unsigned long long)wit.sw_to_after);
  sem_destroy(&g_t9_hready);
  sem_destroy(&g_t9_wake);
  if (wit.sw_irq == 0 || g_t9_ran != g_t9_hpid)
    {
      printf("deadline t9: UNVERIFIED irq divergence not observed\n");
      return 2;
    }

  charged_low = wit.sw_from == g_t9_lpid &&
                wit.sw_running == g_t9_lpid &&
                wit.tick_running == g_t9_lpid &&
                wit.sw_from_after <= wit.sw_from_before;
  high_unchanged = wit.sw_to == g_t9_hpid &&
                   wit.sw_head == g_t9_hpid &&
                   wit.sw_running != wit.sw_head &&
                   wit.sw_to_before == wit.sw_to_after;
  if (!charged_low || !high_unchanged)
    {
      printf("deadline t9: FAIL charge target\n");
      return 1;
    }

  if (wit.sw_now == wit.tick_now &&
      wit.sw_from_before == wit.tick_rem &&
      wit.sw_from_after != wit.sw_from_before)
    {
      printf("deadline t9: FAIL same-stamp double debit\n");
      return 1;
    }

  printf("deadline t9: PASS irq running=%d head=%d\n",
         wit.sw_running, wit.sw_head);
  return 0;
}
