/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation. All rights reserved.
 *   Copyright (c) 2019 Mellanox Technologies LTD. All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "szd/szd.h"
#include "szd/szd_iouring.h"
#include "szd/szd_spdk.h"
#include "szd/szd_status_code.h"

#include <spdk/env.h>
#include <spdk/likely.h>
#include <spdk/log.h>
#include <spdk/nvme.h>
#include <spdk/nvme_spec.h>
#include <spdk/nvme_zns.h>
#include <spdk/string.h>
#include <spdk/util.h>

#ifdef __cplusplus
extern "C" {
#endif
#ifdef __cplusplus
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

const DeviceOptions DeviceOptions_default = {"znsdevice", true, true, false};
const DeviceOpenOptions DeviceOpenOptions_default = {0, 0};
const Completion Completion_default = {false, SZD_SC_SUCCESS, 0};
const DeviceManagerInternal DeviceManagerInternal_default = {0, 0};
const DeviceInfo DeviceInfo_default = {0, 0, 0, 0, 0, 0, 0, 0, 0, "SZD", 0};

#ifdef NDEBUG
// When no debugging, we require that no functions will use invalid nulled
// params.
#define RETURN_ERR_ON_NULL(x)                                                  \
  do {                                                                         \
  } while (0)
#else
// TODO: unlikely and do while worth it?
#define RETURN_ERR_ON_NULL(x)                                                  \
  do {                                                                         \
    if (spdk_unlikely((x) == NULL)) {                                          \
      return (SZD_SC_NOT_ALLOCATED);                                           \
    }                                                                          \
  } while (0)
#endif

int __szd_register_backend(EngineManager *em, enum szd_io_backend backend) {
  switch (backend) {
  case SZD_IO_BACKEND_SPDK:
    return szd_spdk_register_backend(em);
    break;
  case SZD_IO_BACKEND_IO_URING:
    return szd_io_uring_register_backend(em);
    break;
  default:
    break;
  }
  return -1;
}

int szd_init(EngineManager **em, DeviceOptions *options,
             enum szd_io_backend backend) {
  int ret = 0;
  RETURN_ERR_ON_NULL(options);
  RETURN_ERR_ON_NULL(em);
  *em = (EngineManager *)calloc(1, sizeof(EngineManager));
  RETURN_ERR_ON_NULL(*em);
  DeviceManager *dm = (DeviceManager *)calloc(1, sizeof(DeviceManager));
  (*em)->manager_ = dm;
  RETURN_ERR_ON_NULL(dm);
  // Setup engine
  if ((ret = __szd_register_backend(*em, backend)) < 0) {
    free(dm);
    return ret;
  }
  if ((ret = (*em)->backend.init(&dm, options)) < 0) {
    free(dm);
    return ret;
  }
  SZD_DTRACE_PROBE(szd_init);
  return SZD_SC_SUCCESS;
}

int szd_get_device_info(EngineManager *em, DeviceInfo *info) {
  RETURN_ERR_ON_NULL(info);
  RETURN_ERR_ON_NULL(em);
  RETURN_ERR_ON_NULL(em->manager_);
  if (em->backend.get_device_info(info, em->manager_) < 0) {
    return SZD_SC_UNKNOWN;
  }
  return SZD_SC_SUCCESS;
}

static int __szd_open_create_internal(EngineManager *em,
                                      DeviceOpenOptions *options) {
  DeviceManager *dm = em->manager_;
  uint64_t zone_min = options->min_zone;
  uint64_t zone_max = options->max_zone;
  uint64_t zone_max_allowed = dm->info.lba_cap / dm->info.zone_size;
  if (zone_min != 0 && zone_min > zone_max_allowed) {
    return SZD_SC_SPDK_ERROR_OPEN;
  }
  if (zone_max == 0) {
    zone_max = zone_max_allowed;
  } else {
    zone_max = zone_max > zone_max_allowed ? zone_max_allowed : zone_max;
  }
  if (zone_min > zone_max) {
    return SZD_SC_SPDK_ERROR_OPEN;
  }
  DeviceManagerInternal *internal_ =
      (DeviceManagerInternal *)calloc(1, sizeof(DeviceManagerInternal));
  *internal_ = DeviceManagerInternal_default;
  RETURN_ERR_ON_NULL(internal_);
  internal_->zone_min_ = zone_min;
  internal_->zone_max_ = zone_max;
  em->internal_ = (void *)internal_;
  return SZD_SC_SUCCESS;
}

int szd_open(EngineManager *em, const char *traddr,
             DeviceOpenOptions *options) {
  int rc;
  // look for device
  if ((rc = em->backend.open(em->manager_, traddr, options)) != 0) {
    return rc;
  }
  // Setup information immediately.
  if ((rc = szd_get_device_info(em, &em->manager_->info)) != 0) {
    return rc;
  }
  // Shadow container
  if ((rc = __szd_open_create_internal(em, options)) != 0) {
    return rc;
  }
  // Create a container.
  DeviceManagerInternal *internal_ = (DeviceManagerInternal *)em->internal_;
  em->manager_->info.min_lba =
      internal_->zone_min_ * em->manager_->info.zone_size;
  em->manager_->info.max_lba =
      internal_->zone_max_ * em->manager_->info.zone_size;
  SZD_DTRACE_PROBE(szd_open);
  return rc;
}

int szd_close(EngineManager *em) {
  RETURN_ERR_ON_NULL(em);
  int rc;
  // Close backend
  rc = em->backend.close(em->manager_);
  // Close internal business
  em->manager_->info = DeviceInfo_default;
  em->manager_->info.name = "\xef\xbe\xad\xde";
  if (em->internal_ != NULL) {
    free(em->internal_);
    em->internal_ = NULL;
  }
  SZD_DTRACE_PROBE(szd_closed);
  return rc != 0 ? SZD_SC_SPDK_ERROR_CLOSE : SZD_SC_SUCCESS;
}

int szd_destroy(EngineManager *em) {
  RETURN_ERR_ON_NULL(em);
  int rc = SZD_SC_SUCCESS;
  if (em->internal_ != NULL) {
    rc = szd_close(em);
  }
  rc = em->backend.destroy(em->manager_) || rc;
  if (em->manager_ != NULL) {
    free(em->manager_);
  }
  free(em);
  SZD_DTRACE_PROBE(szd_destroy);
  return rc;
}

int szd_reinit(EngineManager **em) {
  RETURN_ERR_ON_NULL(em);
  RETURN_ERR_ON_NULL(*em);
  int rc = (*em)->backend.reinit(&(*em)->manager_);
  if (rc != 0) {
    return SZD_SC_SPDK_ERROR_CLOSE;
  }
  return SZD_SC_SUCCESS;
}

int szd_probe(EngineManager *em, void **probe) {
  RETURN_ERR_ON_NULL(em);
  RETURN_ERR_ON_NULL(probe);
  return em->backend.probe(em->manager_, probe);
}

void szd_free_probe_information(EngineManager *em, void *probe_info) {
  em->backend.free_probe(em->manager_, probe_info);
}

int szd_create_qpair(EngineManager *em, QPair **qpair) {
  RETURN_ERR_ON_NULL(em);
  RETURN_ERR_ON_NULL(qpair);
  int rc = em->backend.create_qpair(em->manager_, qpair);
  SZD_DTRACE_PROBE(szd_create_qpair);
  return rc;
}

int szd_destroy_qpair(EngineManager *em, QPair *qpair) {
  RETURN_ERR_ON_NULL(qpair);
  int rc = em->backend.destroy_qpair(em->manager_, qpair);
  SZD_DTRACE_PROBE(szd_destroy_qpair);
  return rc;
}

void *szd_calloc(EngineManager *em, uint64_t __allign, size_t __nmemb,
                 size_t __size) {
  return em->backend.buf_calloc(__allign, __nmemb, __size);
}

void szd_free(EngineManager *em, void *buffer) { em->backend.free(buffer); }

int szd_read_with_diag(EngineManager *em, QPair *qpair, uint64_t lba,
                       void *buffer, uint64_t size, uint64_t *nr_reads) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceManager *dm = qpair->man;
  DeviceInfo info = dm->info;

  // zone pointers
  uint64_t slba = (lba / info.zone_size) * info.zone_size;
  uint64_t current_zone_end = slba + info.zone_cap;
  // Oops, let me fix this for you
  if (spdk_unlikely(lba >= current_zone_end)) {
    slba += info.zone_size;
    lba = slba + lba - current_zone_end;
    current_zone_end = slba + info.zone_cap;
  }
  // Progress variables
  uint64_t lbas_to_process = (size + info.lba_size - 1) / info.lba_size;
  uint64_t lbas_processed = 0;
  // Used to determine next IO call
  uint64_t step_size =
      (info.mdts / info.lba_size); // If lba_size > mdts, we have a big problem,
                                   // but not because of the read.
  uint64_t current_step_size = step_size;

  // Otherwise we have an out of range.
  uint64_t number_of_zones_traversed =
      (lbas_to_process + (lba - slba)) / info.zone_cap;
  if (szd_unlikely(lba < info.min_lba ||
                   slba + number_of_zones_traversed * info.zone_size >
                       info.max_lba)) {
    return SZD_SC_SPDK_ERROR_READ;
  }

  // Read in steps of max MDTS bytess and respect boundaries
  while (lbas_processed < lbas_to_process) {
    // Read accross a zone border.
    if (lba + step_size >= current_zone_end) {
      current_step_size = current_zone_end - lba;
    } else {
      current_step_size = step_size;
    }
    // printf("STEPSIXE %lu %lu \n", lbas_processed, current_step_size);
    // Do not read too much (more than mdts or requested)
    current_step_size = lbas_to_process - lbas_processed > current_step_size
                            ? current_step_size
                            : lbas_to_process - lbas_processed;
    rc = em->backend.read(qpair, lba,
                          (char *)buffer + lbas_processed * info.lba_size,
                          current_step_size * info.lba_size, current_step_size);
#ifdef SZD_PERF_COUNTERS
    if (nr_reads != NULL) {
      *nr_reads += 1;
    }
#else
    (void)nr_reads;
#endif
    if (spdk_unlikely(rc != 0)) {
      return SZD_SC_SPDK_ERROR_READ;
    }
    lbas_processed += current_step_size;
    lba += current_step_size;
    // To the next zone we go
    if (lba >= current_zone_end) {
      slba += info.zone_size;
      lba = slba;
      current_zone_end = slba + info.zone_cap;
    }
  }
  return SZD_SC_SUCCESS;
}

int szd_read(EngineManager *em, QPair *qpair, uint64_t lba, void *buffer,
             uint64_t size) {
  return szd_read_with_diag(em, qpair, lba, buffer, size, NULL);
}

int szd_append_with_diag(EngineManager *em, QPair *qpair, uint64_t *lba,
                         void *buffer, uint64_t size, uint64_t *nr_appends) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceManager *dm = qpair->man;
  DeviceInfo info = dm->info;

  // Zone pointers
  uint64_t slba = (*lba / info.zone_size) * info.zone_size;
  uint64_t current_zone_end = slba + info.zone_cap;
  // Oops, let me fix this for you
  if (spdk_unlikely(*lba >= current_zone_end)) {
    slba += info.zone_size;
    *lba = slba + *lba - current_zone_end;
    current_zone_end = slba + info.zone_cap;
  }
  // Progress variables
  uint64_t lbas_to_process = (size + info.lba_size - 1) / info.lba_size;
  uint64_t lbas_processed = 0;
  // Used to determine next IO call
  uint64_t step_size =
      (info.zasl / info.lba_size); // < If lba_size > zasl, we have a big
                                   // problem, but not because of the append.
  uint64_t current_step_size = step_size;

  // Error if we have an out of range.
  uint64_t number_of_zones_traversed =
      (lbas_to_process + (*lba - slba)) / info.zone_cap;
  if (szd_unlikely(*lba < info.min_lba ||
                   slba + number_of_zones_traversed * info.zone_size >
                       info.max_lba)) {
    SPDK_ERRLOG("SZD: Append is out of allowed range\n");
    return SZD_SC_SPDK_ERROR_APPEND;
  }

  // Append in steps of max ZASL bytes and respect boundaries
  while (lbas_processed < lbas_to_process) {
    // Append across a zone border.
    if ((*lba + step_size) >= current_zone_end) {
      current_step_size = current_zone_end - *lba;
    } else {
      current_step_size = step_size;
    }
    // Do not append too much (more than ZASL or what is requested)
    current_step_size = lbas_to_process - lbas_processed > current_step_size
                            ? current_step_size
                            : lbas_to_process - lbas_processed;

    rc = em->backend.append(
        qpair, slba, (char *)buffer + lbas_processed * info.lba_size,
        current_step_size * info.lba_size, current_step_size);
#ifdef SZD_PERF_COUNTERS
    if (nr_appends != NULL) {
      *nr_appends += 1;
    }
#else
    (void)nr_appends;
#endif
    if (spdk_unlikely(rc != 0)) {
      SPDK_ERRLOG("SZD: Error creating append request\n");
      return SZD_SC_SPDK_ERROR_APPEND;
    }
    *lba = *lba + current_step_size;
    lbas_processed += current_step_size;
    // To the next zone we go
    if (*lba >= current_zone_end) {
      slba += info.zone_size;
      *lba = slba;
      current_zone_end = slba + info.zone_cap;
    }
  }
  return SZD_SC_SUCCESS;
}

int szd_append(EngineManager *em, QPair *qpair, uint64_t *lba, void *buffer,
               uint64_t size) {
  return szd_append_with_diag(em, qpair, lba, buffer, size, NULL);
}

int szd_append_async_with_diag(EngineManager *em, QPair *qpair, uint64_t *lba,
                               void *buffer, uint64_t size,
                               uint64_t *nr_appends, Completion *completion) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceManager *dm = qpair->man;
  DeviceInfo info = dm->info;

  // Zone pointers
  uint64_t slba = (*lba / info.zone_size) * info.zone_size;
  uint64_t current_zone_end = slba + info.zone_cap;
  // Oops, let me fix this for you
  if (spdk_unlikely(*lba > current_zone_end)) {
    slba += info.zone_size;
    *lba = slba + *lba - current_zone_end;
    current_zone_end = slba + info.zone_cap;
  }
  // Progress variables
  uint64_t lbas_to_process = (size + info.lba_size - 1) / info.lba_size;
  uint32_t id = completion->id;
  *completion = Completion_default;
  completion->id = id;

  // Error if we have an out of range or we cross a zone border.
  uint64_t number_of_zones_traversed =
      (lbas_to_process + (*lba - slba)) / info.zone_cap;
  if (szd_unlikely(*lba < info.min_lba || *lba > info.max_lba ||
                   number_of_zones_traversed > 1 ||
                   lbas_to_process > info.zasl / info.lba_size)) {
    SPDK_ERRLOG("SZD: Async append out of range\n");
    return SZD_SC_SPDK_ERROR_APPEND;
  }

  completion->done = false;
  completion->err = 0x00;
  rc = em->backend.append_async(qpair, slba, (char *)buffer,
                                lbas_to_process * info.lba_size,
                                lbas_to_process, completion);
#ifdef SZD_PERF_COUNTERS
  if (nr_appends != NULL) {
    *nr_appends += 1;
  }
#else
  (void)nr_appends;
#endif
  if (spdk_unlikely(rc != 0)) {
    SPDK_ERRLOG("SZD: Error creating append request\n");
    return SZD_SC_SPDK_ERROR_APPEND;
  }
  *lba = *lba + lbas_to_process;
  return SZD_SC_SUCCESS;
}

int szd_append_async(EngineManager *em, QPair *qpair, uint64_t *lba,
                     void *buffer, uint64_t size, Completion *completion) {
  return szd_append_async_with_diag(em, qpair, lba, buffer, size, NULL,
                                    completion);
}

int szd_write_with_diag(EngineManager *em, QPair *qpair, uint64_t *lba,
                        void *buffer, uint64_t size, uint64_t *nr_writes) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceManager *dm = qpair->man;
  DeviceInfo info = dm->info;

  // Zone pointers
  uint64_t slba = (*lba / info.zone_size) * info.zone_size;
  uint64_t current_zone_end = slba + info.zone_cap;
  // Oops, let me fix this for you
  if (spdk_unlikely(*lba >= current_zone_end)) {
    slba += info.zone_size;
    *lba = slba + *lba - current_zone_end;
    current_zone_end = slba + info.zone_cap;
  }
  uint64_t lba_tmp = *lba;
  // Progress variables
  uint64_t lbas_to_process = (size + info.lba_size - 1) / info.lba_size;
  uint64_t lbas_processed = 0;
  // Used to determine next IO call
  uint64_t step_size =
      (info.mdts / info.lba_size); // < If lba_size > mdts, we have a big
                                   // problem, but not because of the append.
  uint64_t current_step_size = step_size;

  // Error if we have an out of range.
  uint64_t number_of_zones_traversed =
      (lbas_to_process + (*lba - slba)) / info.zone_cap;
  if (szd_unlikely(*lba < info.min_lba ||
                   slba + number_of_zones_traversed * info.zone_size >
                       info.max_lba)) {
    SPDK_ERRLOG("SZD: Write is out of allowed range\n");
    return SZD_SC_SPDK_ERROR_APPEND;
  }

  // Append in steps of max MDTS bytes and respect boundaries
  while (lbas_processed < lbas_to_process) {
    // Append across a zone border.
    if ((*lba + step_size) >= current_zone_end) {
      current_step_size = current_zone_end - *lba;
    } else {
      current_step_size = step_size;
    }
    // Do not append too much (more than mdts or what is requested)
    current_step_size = lbas_to_process - lbas_processed > current_step_size
                            ? current_step_size
                            : lbas_to_process - lbas_processed;

    rc = em->backend.append(
        qpair, lba_tmp, (char *)buffer + lbas_processed * info.lba_size,
        current_step_size * info.lba_size, current_step_size);
#ifdef SZD_PERF_COUNTERS
    if (nr_writes != NULL) {
      *nr_writes += 1;
    }
#else
    (void)nr_writes;
#endif
    if (szd_unlikely(rc != 0)) {
      SPDK_ERRLOG("SZD: Error creating write request\n");
      return SZD_SC_SPDK_ERROR_APPEND;
    }
    lba_tmp += current_step_size;
    *lba = lba_tmp;
    lbas_processed += current_step_size;
    // To the next zone we go
    if (*lba >= current_zone_end) {
      slba += info.zone_size;
      lba_tmp = slba;
      *lba = slba;
      current_zone_end = slba + info.zone_cap;
    }
  }
  return SZD_SC_SUCCESS;
}

int szd_write(EngineManager *em, QPair *qpair, uint64_t *lba, void *buffer,
              uint64_t size) {
  return szd_write_with_diag(em, qpair, lba, buffer, size, NULL);
}

int szd_poll_async(EngineManager *em, QPair *qpair, Completion *completion) {
  return em->backend.poll_async(qpair, completion);
}

int szd_poll_once(EngineManager *em, QPair *qpair, Completion *completion) {
  return em->backend.poll_once(qpair, completion);
}

void szd_poll_once_raw(EngineManager *em, QPair *qpair) {
  return em->backend.poll_once_raw(qpair);
}

int szd_reset(EngineManager *em, QPair *qpair, uint64_t slba) {
  RETURN_ERR_ON_NULL(qpair);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (spdk_unlikely(slba < info.min_lba || slba >= info.lba_cap)) {
    return SZD_SC_SPDK_ERROR_READ;
  }
  int rc = em->backend.reset_zone(qpair, slba);
  if (spdk_unlikely(rc != 0)) {
    return SZD_SC_SPDK_ERROR_RESET;
  }
  return rc;
}

int szd_reset_all(EngineManager *em, QPair *qpair) {
  RETURN_ERR_ON_NULL(qpair);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  int rc = SZD_SC_SUCCESS;
  // We can not do full reset, if we only "own" a  part.
  if (info.min_lba > 0 || info.max_lba < info.lba_cap) {
    // What are you doing?
    if (szd_unlikely(info.min_lba > info.max_lba)) {
      return SZD_SC_SPDK_ERROR_RESET;
    }
    for (uint64_t slba = info.min_lba; slba < info.max_lba;
         slba += info.zone_size) {
      if ((rc = szd_reset(em, qpair, slba)) != 0) {
        return rc;
      }
    }
  } else {
    rc = em->backend.reset_all_zones(qpair);
    if (szd_unlikely(rc != 0)) {
      return SZD_SC_SPDK_ERROR_RESET;
    }
  }
  return rc;
}

int szd_finish_zone(EngineManager *em, QPair *qpair, uint64_t slba) {
  RETURN_ERR_ON_NULL(qpair);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (spdk_unlikely(slba < info.min_lba || slba > info.lba_cap)) {
    return SZD_SC_SPDK_ERROR_FINISH;
  }
  int rc = em->backend.finish_zone(qpair, slba);
  if (szd_unlikely(rc != 0)) {
    return SZD_SC_SPDK_ERROR_FINISH;
  }
  return rc;
}

int szd_get_zone_heads(EngineManager *em, QPair *qpair, uint64_t slba,
                       uint64_t eslba, uint64_t *write_head) {
  // Inspired by SPDK/nvme/identify.c
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->man);
  // Otherwise we have an out of range.
  DeviceManager *dm = qpair->man;
  DeviceInfo info = dm->info;
  if (szd_unlikely(slba < info.min_lba || slba >= info.max_lba ||
                   eslba < info.min_lba || eslba >= info.max_lba ||
                   slba > eslba || slba % info.zone_size != 0 ||
                   eslba % info.zone_size != 0)) {
    return SZD_SC_SPDK_ERROR_REPORT_ZONES;
  }
  int rc = SZD_SC_SUCCESS;
  rc = em->backend.get_zone_heads(qpair, slba, eslba, write_head);
  return rc;
}

int szd_get_zone_head(EngineManager *em, QPair *qpair, uint64_t slba,
                      uint64_t *write_head) {
  return szd_get_zone_heads(em, qpair, slba, slba, write_head);
}

int szd_get_zone_cap(EngineManager *em, QPair *qpair, uint64_t slba,
                     uint64_t *zone_cap) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->man);
  // Otherwise we have an out of range.
  DeviceManager *dm = qpair->man;
  DeviceInfo info = dm->info;
  if (szd_unlikely(slba < info.min_lba || slba > info.max_lba)) {
    return SZD_SC_SPDK_ERROR_READ;
  }
  int rc = em->backend.get_zone_head(qpair, slba, zone_cap);
  return rc;
}

void szd_print_zns_status(int status) {
  fprintf(stdout, "SZD: status = %s\n", szd_status_code_msg(status));
}

long int szd_spdk_strtol(const char *nptr, int base) {
  return spdk_strtol(nptr, base);
}

void __szd_error_log(const char *file, const int line, const char *func,
                     const char *format, ...) {
  // TODO: cleanup
  // For now resort to SPDK's internal error printing (not-maintainable)
  va_list ap;

  va_start(ap, format);
  spdk_vlog(SPDK_LOG_ERROR, file, line, func, format, ap);
  va_end(ap);
}

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
