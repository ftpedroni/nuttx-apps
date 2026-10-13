/****************************************************************************
 * apps/testing/sched/deadline/deadline_main.c
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The
 * ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 ****************************************************************************/

/****************************************************************************
 * Included Files
 ****************************************************************************/

#include <nuttx/config.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <malloc.h>
#include <pthread.h>
#include <sched.h>

/****************************************************************************
 * Pre-processor Definitions
 ****************************************************************************/

#define DL_BAD_PID ((pid_t)32767)

int deadline_live_test(void);
int deadline_t9_irq_test(void);
int deadline_boundary_test(void);

/****************************************************************************
 * Private Data
 ****************************************************************************/

static int g_failed;

/****************************************************************************
 * Private Functions
 ****************************************************************************/

static void note_ok(FAR const char *name)
{
  printf("deadlinetest: ok %s\n", name);
}

static void note_fail(FAR const char *name, int rc, int err, int want)
{
  printf("deadlinetest: FAIL %s rc=%d errno=%d want -1/%d\n",
         name, rc, err, want);
  g_failed = 1;
}

static int current_policy(void)
{
  return sched_getscheduler(0);
}

static int current_priority(void)
{
  struct sched_param param;

  if (sched_getparam(0, &param) != 0)
    {
      return -1;
    }

  return param.sched_priority;
}

static void fill_valid(FAR struct sched_attr *attr)
{
  memset(attr, 0, sizeof(*attr));
  attr->size           = sizeof(*attr);
  attr->sched_policy   = SCHED_DEADLINE;
  attr->sched_runtime  = 1000;
  attr->sched_deadline = 3000;
  attr->sched_period   = 3000;
}

/* Reject a call and require the caller policy and priority to stay. */

static void expect_rejected(FAR const char *name, int rc, int want,
                            int policy, int priority)
{
  int err = errno;

  if (rc != -1 || err != want)
    {
      note_fail(name, rc, err, want);
    }
  else if (current_policy() != policy || current_priority() != priority)
    {
      printf("deadlinetest: FAIL %s mutated policy %d->%d prio %d->%d\n",
             name, policy, current_policy(), priority, current_priority());
      g_failed = 1;
    }
  else
    {
      note_ok(name);
    }
}

static void check_layout(void)
{
  _Static_assert(sizeof(struct sched_attr) == 48, "sched_attr size");
  _Static_assert(offsetof(struct sched_attr, size) == 0, "size");
  _Static_assert(offsetof(struct sched_attr, sched_policy) == 4, "policy");
  _Static_assert(offsetof(struct sched_attr, sched_flags) == 8, "flags");
  _Static_assert(offsetof(struct sched_attr, sched_nice) == 16, "nice");
  _Static_assert(offsetof(struct sched_attr, sched_priority) == 20,
                 "priority");
  _Static_assert(offsetof(struct sched_attr, sched_runtime) == 24,
                 "runtime");
  _Static_assert(offsetof(struct sched_attr, sched_deadline) == 32,
                 "deadline");
  _Static_assert(offsetof(struct sched_attr, sched_period) == 40, "period");
  _Static_assert(SCHED_ATTR_SIZE_VER0 == sizeof(struct sched_attr), "ver0");

  note_ok("layout");
}

static void check_getattr_self(void)
{
  struct sched_attr attr;
  struct sched_param param;
  int policy;
  int rc;

  policy = current_policy();
  if (sched_getparam(0, &param) != 0)
    {
      note_fail("getparam self", -1, errno, 0);
      return;
    }

  memset(&attr, 0x5a, sizeof(attr));
  rc = sched_getattr(0, &attr, sizeof(attr), 0);
  if (rc != 0 || attr.size != sizeof(attr) ||
      attr.sched_policy != (uint32_t)policy ||
      attr.sched_priority != (uint32_t)param.sched_priority ||
      attr.sched_flags != 0 || attr.sched_nice != 0 ||
      attr.sched_runtime != 0 || attr.sched_deadline != 0 ||
      attr.sched_period != 0)
    {
      printf("deadlinetest: FAIL getattr self rc=%d policy %d/%u prio %d/%u\n",
             rc, policy, (unsigned int)attr.sched_policy,
             param.sched_priority, (unsigned int)attr.sched_priority);
      g_failed = 1;
      return;
    }

  if (policy != SCHED_FIFO && policy != SCHED_RR && policy != SCHED_SPORADIC)
    {
      printf("deadlinetest: FAIL getattr self unexpected policy %d\n",
             policy);
      g_failed = 1;
      return;
    }

  printf("deadlinetest: ok getattr self policy=%d prio=%d\n",
         policy, param.sched_priority);
}

/* A successful activation has to be undone before the next tick.
 * The budget in fill_valid() is far below the tick, and this task
 * must not stay deadline while the rest of the suite runs.
 */

static int revert_caller(int policy, int priority)
{
  struct sched_param param;

  memset(&param, 0, sizeof(param));
  param.sched_priority = priority;
  return sched_setscheduler(0, policy, &param);
}

static void check_deadline_refused(void)
{
  struct sched_attr attr;
  struct sched_param param;
  uint8_t saved[sizeof(struct sched_attr)];
  int policy;
  int priority;
  int rc;

  policy = current_policy();
  priority = current_priority();
  fill_valid(&attr);

  rc = sched_setattr(0, &attr, 0);
  if (rc != 0 || sched_getscheduler(0) != SCHED_DEADLINE)
    {
      note_fail("valid deadline", rc, errno, 0);
      revert_caller(policy, priority);
    }
  else
    {
      struct sched_attr got;

      memset(&got, 0, sizeof(got));
      if (sched_getattr(0, &got, sizeof(got), 0) != 0 ||
          got.sched_runtime != attr.sched_runtime ||
          got.sched_deadline != attr.sched_deadline ||
          got.sched_period != attr.sched_period)
        {
          note_fail("valid deadline getattr", -1, errno, 0);
        }
      else
        {
          note_ok("valid deadline");
        }

      if (revert_caller(policy, priority) != 0 ||
          current_policy() != policy || current_priority() != priority)
        {
          note_fail("valid deadline revert", -1, errno, 0);
        }
      else
        {
          note_ok("valid deadline revert");
        }
    }

  memset(&param, 0, sizeof(param));
  param.sched_priority = priority;
  rc = sched_setscheduler(0, SCHED_DEADLINE, &param);
  expect_rejected("setscheduler deadline", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_runtime = 0;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("runtime 0", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_deadline = 0;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("deadline 0", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_period = 0;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("period 0", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_runtime = 5;
  attr.sched_deadline = 4;
  attr.sched_period = 4;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("runtime > deadline", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_runtime = 1;
  attr.sched_deadline = 3;
  attr.sched_period = 2;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("deadline > period", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_policy = 99;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("unknown policy", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_policy = SCHED_BATCH;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("batch", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_policy = SCHED_IDLE;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("idle", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_flags = 1;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("nonzero sched_flags", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_nice = 1;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("nonzero nice", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_priority = 1;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("nonzero deadline priority", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  rc = sched_setattr(0, &attr, 1);
  expect_rejected("nonzero function flags", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.size = 0;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("size 0", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.size = sizeof(attr) - 1;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("size short", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.size = sizeof(attr) + 8;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("size long", rc, EINVAL, policy, priority);

  rc = sched_setattr(0, NULL, 0);
  expect_rejected("setattr NULL", rc, EINVAL, policy, priority);

  memset(&attr, 0x5a, sizeof(attr));
  memcpy(saved, &attr, sizeof(attr));
  rc = sched_getattr(0, &attr, sizeof(attr) - 1, 0);
  if (rc != -1 || errno != EINVAL || memcmp(saved, &attr, sizeof(attr)) != 0)
    {
      note_fail("getattr short size", rc, errno, EINVAL);
    }
  else if (current_policy() != policy || current_priority() != priority)
    {
      printf("deadlinetest: FAIL getattr short size mutated task\n");
      g_failed = 1;
    }
  else
    {
      note_ok("getattr short size");
    }

  rc = sched_getattr(0, NULL, sizeof(attr), 0);
  expect_rejected("getattr NULL", rc, EINVAL, policy, priority);

  rc = sched_getattr(0, &attr, sizeof(attr), 1);
  expect_rejected("getattr flags", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_runtime = UINT64_MAX;
  attr.sched_deadline = UINT64_MAX;
  attr.sched_period = UINT64_MAX;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("uint64 max triple", rc, EBUSY, policy, priority);

  fill_valid(&attr);
  attr.sched_runtime = UINT64_MAX;
  attr.sched_deadline = UINT64_MAX - 1;
  attr.sched_period = UINT64_MAX;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("runtime above deadline near max", rc, EINVAL,
                  policy, priority);

  fill_valid(&attr);
  attr.sched_runtime = 1;
  attr.sched_deadline = UINT64_MAX;
  attr.sched_period = UINT64_MAX - 1;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("deadline above period near max", rc, EINVAL,
                  policy, priority);

  fill_valid(&attr);
  attr.sched_runtime = 1;
  attr.sched_deadline = 1;
  attr.sched_period = UINT64_MAX;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("constrained deadline", rc, EINVAL, policy, priority);

  fill_valid(&attr);
  attr.sched_runtime = 1;
  attr.sched_deadline = UINT64_MAX;
  attr.sched_period = UINT64_MAX;
  rc = sched_setattr(0, &attr, 0);
  if (rc != 0)
    {
      note_fail("period uint64 max", rc, errno, 0);
    }
  else if (revert_caller(policy, priority) != 0 ||
           current_policy() != policy || current_priority() != priority)
    {
      note_fail("period uint64 max revert", -1, errno, 0);
    }
  else
    {
      note_ok("period uint64 max");
    }

  fill_valid(&attr);
  rc = sched_setattr(DL_BAD_PID, &attr, 0);
  expect_rejected("setattr bad pid", rc, ESRCH, policy, priority);

  rc = sched_getattr(DL_BAD_PID, &attr, sizeof(attr), 0);
  expect_rejected("getattr bad pid", rc, ESRCH, policy, priority);

  fill_valid(&attr);
  attr.sched_policy = SCHED_FIFO;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("setattr fifo not applied", rc, EOPNOTSUPP,
                  policy, priority);

  fill_valid(&attr);
  attr.sched_policy = SCHED_OTHER;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("setattr other not applied", rc, EOPNOTSUPP,
                  policy, priority);

#if CONFIG_RR_INTERVAL > 0
  fill_valid(&attr);
  attr.sched_policy = SCHED_RR;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("setattr rr not applied", rc, EOPNOTSUPP,
                  policy, priority);
#else
  fill_valid(&attr);
  attr.sched_policy = SCHED_RR;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("setattr rr disabled", rc, EINVAL, policy, priority);
#endif

#ifdef CONFIG_SCHED_SPORADIC
  fill_valid(&attr);
  attr.sched_policy = SCHED_SPORADIC;
  rc = sched_setattr(0, &attr, 0);
  expect_rejected("setattr sporadic not applied", rc, EOPNOTSUPP,
                  policy, priority);
#endif
}

static void check_no_leak(void)
{
  struct sched_attr attr;
  struct mallinfo before;
  struct mallinfo after;
  int policy;
  int priority;
  int i;
  int rc;

  policy = current_policy();
  priority = current_priority();
  fill_valid(&attr);
  before = mallinfo();

  for (i = 0; i < 200; i++)
    {
      rc = sched_setattr(0, &attr, 0);
      if (rc != 0 || revert_caller(policy, priority) != 0 ||
          current_policy() != policy)
        {
          note_fail("repeat valid", rc, errno, 0);
          revert_caller(policy, priority);
          return;
        }

      attr.sched_runtime = 0;
      rc = sched_setattr(0, &attr, 0);
      if (rc != -1 || errno != EINVAL)
        {
          note_fail("repeat zero", rc, errno, EINVAL);
          return;
        }

      attr.sched_runtime = 1000;
    }

  after = mallinfo();
  if (after.uordblks != before.uordblks ||
      current_policy() != policy || current_priority() != priority)
    {
      printf("deadlinetest: FAIL leak uordblks %d->%d policy %d prio %d\n",
             before.uordblks, after.uordblks, current_policy(),
             current_priority());
      g_failed = 1;
    }
  else
    {
      note_ok("repeat no allocation");
    }
}

static void check_legacy_api(void)
{
  struct sched_param param;
  int policy;
  int priority;
  int rc;
  int nudged;

  policy = current_policy();
  priority = current_priority();
  if (priority < 1)
    {
      note_fail("legacy priority", -1, 0, 0);
      return;
    }

  memset(&param, 0, sizeof(param));
  param.sched_priority = priority;
  rc = sched_setscheduler(0, SCHED_FIFO, &param);
  if (rc != 0 || current_policy() != SCHED_FIFO ||
      current_priority() != priority)
    {
      printf("deadlinetest: FAIL setscheduler fifo rc=%d policy=%d\n",
             rc, current_policy());
      g_failed = 1;
    }
  else
    {
      note_ok("setscheduler fifo");
    }

  nudged = priority < 255 ? priority + 1 : priority - 1;
  param.sched_priority = nudged;
  rc = sched_setparam(0, &param);
  if (rc != 0 || current_priority() != nudged)
    {
      printf("deadlinetest: FAIL setparam rc=%d prio=%d\n",
             rc, current_priority());
      g_failed = 1;
    }
  else
    {
      note_ok("setparam");
    }

  rc = sched_getparam(0, &param);
  if (rc != 0 || param.sched_priority != nudged)
    {
      note_fail("getparam", rc, errno, 0);
    }
  else
    {
      note_ok("getparam");
    }

  param.sched_priority = priority;
  rc = sched_setparam(0, &param);
  if (rc != 0 || current_priority() != priority)
    {
      printf("deadlinetest: FAIL restore priority rc=%d\n", rc);
      g_failed = 1;
    }

#if CONFIG_RR_INTERVAL > 0
  struct sched_attr attr;

  param.sched_priority = priority;
  rc = sched_setscheduler(0, SCHED_RR, &param);
  if (rc != 0 || current_policy() != SCHED_RR)
    {
      printf("deadlinetest: FAIL setscheduler rr rc=%d policy=%d errno=%d\n",
             rc, current_policy(), errno);
      g_failed = 1;
    }
  else
    {
      memset(&attr, 0, sizeof(attr));
      rc = sched_getattr(0, &attr, sizeof(attr), 0);
      if (rc != 0 || attr.sched_policy != SCHED_RR ||
          attr.sched_priority != (uint32_t)priority ||
          attr.sched_runtime != 0)
        {
          printf("deadlinetest: FAIL getattr rr rc=%d policy=%u\n",
                 rc, (unsigned int)attr.sched_policy);
          g_failed = 1;
        }
      else
        {
          note_ok("getattr rr");
        }
    }
#endif

  param.sched_priority = priority;
  rc = sched_setscheduler(0, policy == SCHED_RR ? SCHED_RR : SCHED_FIFO,
                          &param);
  if (rc != 0 || current_policy() != policy || current_priority() != priority)
    {
      printf("deadlinetest: FAIL restore policy rc=%d policy=%d want=%d\n",
             rc, current_policy(), policy);
      g_failed = 1;
    }
  else if (sched_getscheduler(0) != policy)
    {
      note_fail("getscheduler", -1, errno, 0);
    }
  else
    {
      note_ok("legacy restored");
    }
}

#ifdef CONFIG_SCHED_SPORADIC
static void *sporadic_worker(FAR void *arg)
{
  struct sched_param param;
  struct sched_attr attr;
  int *status = arg;
  int rc;

  memset(&param, 0, sizeof(param));
  param.sched_priority = 100;
  param.sched_ss_low_priority = 20;
  param.sched_ss_max_repl = 1;
  param.sched_ss_repl_period.tv_sec = 2;
  param.sched_ss_init_budget.tv_sec = 1;

  rc = sched_setscheduler(0, SCHED_SPORADIC, &param);
  if (rc != 0)
    {
      *status = errno == 0 ? -1 : errno;
      return NULL;
    }

  memset(&attr, 0, sizeof(attr));
  rc = sched_getattr(0, &attr, sizeof(attr), 0);
  if (rc != 0 || attr.sched_policy != SCHED_SPORADIC ||
      attr.sched_priority != 100 || attr.sched_runtime != 0 ||
      attr.sched_deadline != 0 || attr.sched_period != 0 ||
      attr.sched_flags != 0)
    {
      *status = -2;
      return NULL;
    }

  *status = 0;
  return NULL;
}

static void check_sporadic_getattr(void)
{
  pthread_t thread;
  int status = -3;
  int rc;

  rc = pthread_create(&thread, NULL, sporadic_worker, &status);
  if (rc != 0)
    {
      printf("deadlinetest: FAIL sporadic pthread rc=%d\n", rc);
      g_failed = 1;
      return;
    }

  pthread_join(thread, NULL);
  if (status != 0)
    {
      printf("deadlinetest: FAIL sporadic getattr status=%d\n", status);
      g_failed = 1;
    }
  else
    {
      note_ok("getattr sporadic");
    }
}
#endif

/****************************************************************************
 * Public Functions
 ****************************************************************************/

int main(int argc, FAR char *argv[])
{
  g_failed = 0;
  if (argc > 1 && strcmp(argv[1], "boundary") == 0)
    {
      int boundary = deadline_boundary_test();

      if (boundary != 0)
        {
          printf("deadlinetest: FAIL live case %d\n", boundary);
          return 1;
        }

      printf("deadlinetest: PASS boundary\n");
      return 0;
    }
  check_layout();
  check_getattr_self();
  check_deadline_refused();
  check_no_leak();
  check_legacy_api();

#ifdef CONFIG_SCHED_SPORADIC
  check_sporadic_getattr();
#else
  note_ok("sporadic skipped");
#endif

#ifdef CONFIG_TESTING_DEADLINE
  {
    int order = sched_deadline_order_test();

    if (order != 0)
      {
        printf("deadlinetest: FAIL order case %d\n", order);
        g_failed = 1;
      }
    else
      {
        note_ok("order");
      }
  }

  {
    int cbs = sched_deadline_cbs_test();

    if (cbs != 0)
      {
        printf("deadlinetest: FAIL cbs case %d\n", cbs);
        g_failed = 1;
      }
    else
      {
        note_ok("cbs");
      }
  }

  {
    int timed = sched_deadline_time_test();

    if (timed != 0)
      {
        printf("deadlinetest: FAIL time case %d\n", timed);
        g_failed = 1;
      }
    else
      {
        note_ok("time");
      }
  }

  {
    int admit = sched_deadline_admit_test();

    if (admit != 0)
      {
        printf("deadlinetest: FAIL admit case %d\n", admit);
        g_failed = 1;
      }
    else
      {
        note_ok("admit");
      }
  }

  {
    int oracle = sched_deadline_oracle_test();

    if (oracle != 0)
      {
        printf("deadlinetest: FAIL oracle\n");
        g_failed = 1;
      }
    else
      {
        note_ok("oracle");
      }
  }

  {
    int live = deadline_live_test();

    if (live != 0)
      {
        printf("deadlinetest: FAIL live case %d\n", live);
        g_failed = 1;
      }
    else
      {
        note_ok("live");
      }
  }

  {
    int t9 = deadline_t9_irq_test();

    if (t9 == 1)
      {
        printf("deadlinetest: FAIL t9\n");
        g_failed = 1;
      }
    else if (t9 == 2)
      {
        printf("deadlinetest: UNVERIFIED t9\n");
      }
  }
#endif

  if (g_failed)
    {
      printf("deadlinetest: FAIL\n");
      return 1;
    }

  printf("deadlinetest: PASS\n");
  return 0;
}
