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

typedef struct spdk_nvme_transport_id t_spdk_nvme_transport_id;
typedef struct spdk_nvme_ctrlr t_spdk_nvme_ctrlr;
typedef struct spdk_nvme_ctrlr_opts t_spdk_nvme_ctrlr_opts;
typedef struct spdk_nvme_ns t_spdk_nvme_ns;
typedef struct spdk_nvme_qpair t_spdk_nvme_qpair;
typedef struct spdk_nvme_cpl t_spdk_nvme_cpl;
typedef struct spdk_nvme_zns_ns_data t_spdk_nvme_zns_ns_data;
typedef struct spdk_nvme_ns_data t_spdk_nvme_ns_data;
typedef struct spdk_nvme_zns_ctrlr_data t_spdk_nvme_zns_ctrlr_data;
typedef struct spdk_nvme_ctrlr_data t_spdk_nvme_ctrlr_data;

#ifdef __cplusplus
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

const DeviceOptions DeviceOptions_default = {"znsdevice", true};
const DeviceOpenOptions DeviceOpenOptions_default = {0, 0};
const Completion Completion_default = {false, SZD_SC_SUCCESS};
const DeviceManagerInternal DeviceManagerInternal_default = {0, 0};
const DeviceInfo DeviceInfo_default = {0, 0, 0, 0, 0, 0, 0, 0, "SZD"};

// Needed because of DPDK and reattaching, we need to remember what we have
// seen...
static char *found_devices[MAX_DEVICE_COUNT];
static size_t found_devices_len[MAX_DEVICE_COUNT];
static size_t found_devices_number = 0;

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

int szd_init(DeviceManager **manager, DeviceOptions *options) {
  RETURN_ERR_ON_NULL(options);
  RETURN_ERR_ON_NULL(manager);
  *manager = (DeviceManager *)calloc(1, sizeof(DeviceManager));
  RETURN_ERR_ON_NULL(*manager);
  // Setup options
  struct spdk_env_opts opts;
  if (options->setup_spdk) {
    opts.name = options->name;
    spdk_env_opts_init(&opts);
  }
  // Setup SPDK
  (*manager)->g_trid =
      (t_spdk_nvme_transport_id *)calloc(1, sizeof(t_spdk_nvme_transport_id));
  RETURN_ERR_ON_NULL((*manager)->g_trid);
  spdk_nvme_trid_populate_transport((*manager)->g_trid,
                                    SPDK_NVME_TRANSPORT_PCIE);
  if (spdk_unlikely(spdk_env_init(!options->setup_spdk ? NULL : &opts) < 0)) {
    free((*manager)->g_trid);
    free(*manager);
    return SZD_SC_SPDK_ERROR_INIT;
  }
  // setup stub info, we do not want to create extra UB.
  (*manager)->info = DeviceInfo_default;
  (*manager)->info.name = options->name;
  (*manager)->ctrlr = NULL;
  (*manager)->ns = NULL;
  (*manager)->private_ = NULL;
  SZD_DTRACE_PROBE(szd_init);
  return SZD_SC_SUCCESS;
}

int szd_get_device_info(DeviceInfo *info, DeviceManager *manager) {
  RETURN_ERR_ON_NULL(info);
  RETURN_ERR_ON_NULL(manager);
  RETURN_ERR_ON_NULL(manager->ctrlr);
  RETURN_ERR_ON_NULL(manager->ns);
  info->lba_size = (uint64_t)spdk_nvme_ns_get_sector_size(manager->ns);
  info->zone_size =
      (uint64_t)spdk_nvme_zns_ns_get_zone_size_sectors(manager->ns);
  info->mdts = (uint64_t)spdk_nvme_ctrlr_get_max_xfer_size(manager->ctrlr);
  info->zasl =
      (uint64_t)spdk_nvme_zns_ctrlr_get_max_zone_append_size(manager->ctrlr);
  info->lba_cap = (uint64_t)spdk_nvme_ns_get_num_sectors(manager->ns);
  info->min_lba = manager->info.min_lba;
  info->max_lba = manager->info.max_lba;
  // printf("INFO: %lu %lu %lu %lu %lu %lu %lu \n", info->lba_size,
  // info->zone_size, info->mdts, info->zasl,
  //   info->lba_cap, info->min_lba, info->max_lba);
  // TODO: zone cap can differ between zones...
  QPair **temp = (QPair **)calloc(1, sizeof(QPair *));
  szd_create_qpair(manager, temp);
  szd_get_zone_cap(*temp, info->min_lba, &info->zone_cap);
  szd_destroy_qpair(*temp);
  free(temp);
  return SZD_SC_SUCCESS;
}

bool __szd_open_probe_cb(void *cb_ctx,
                         const struct spdk_nvme_transport_id *trid,
                         struct spdk_nvme_ctrlr_opts *opts) {
  DeviceTarget *prober = (DeviceTarget *)cb_ctx;
  if (!prober->traddr) {
    return false;
  }
  // You trying to overflow?
  if (strlen(prober->traddr) < prober->traddr_len) {
    return false;
  }
  if (strlen((const char *)trid->traddr) < prober->traddr_len) {
    return false;
  }
  if (strncmp((const char *)trid->traddr, prober->traddr, prober->traddr_len) !=
      0) {
    return false;
  }
  (void)opts;
  return true;
}

void __szd_open_attach_cb(void *cb_ctx,
                          const struct spdk_nvme_transport_id *trid,
                          struct spdk_nvme_ctrlr *ctrlr,
                          const struct spdk_nvme_ctrlr_opts *opts) {
  DeviceTarget *prober = (DeviceTarget *)cb_ctx;
  if (prober == NULL) {
    return;
  }
  prober->manager->ctrlr = ctrlr;
  // take any ZNS namespace, we do not care which.
  for (int nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
       nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
    struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
    if (ns == NULL) {
      continue;
    }
    if (spdk_nvme_ns_get_csi(ns) != SPDK_NVME_CSI_ZNS) {
      continue;
    }
    prober->manager->ns = ns;
    prober->found = true;
    break;
  }
  (void)trid;
  (void)opts;
  return;
}

void __szd_open_remove_cb(void *cb_ctx, struct spdk_nvme_ctrlr *ctrlr) {
  (void)cb_ctx;
  (void)ctrlr;
}

int __szd_open_create_private(DeviceManager *manager,
                              DeviceOpenOptions *options) {
  uint64_t zone_min = options->min_zone;
  uint64_t zone_max = options->max_zone;
  uint64_t zone_max_allowed = manager->info.lba_cap / manager->info.zone_size;
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
  DeviceManagerInternal *private_ =
      (DeviceManagerInternal *)calloc(1, sizeof(DeviceManagerInternal));
  *private_ = DeviceManagerInternal_default;
  RETURN_ERR_ON_NULL(private_);
  private_->zone_min_ = zone_min;
  private_->zone_max_ = zone_max;
  manager->private_ = (void *)private_;
  return SZD_SC_SUCCESS;
}

int szd_open(DeviceManager *manager, const char *traddr,
             DeviceOpenOptions *options) {
  DeviceTarget prober = {.manager = manager,
                         .traddr = traddr,
                         .traddr_len = strlen(traddr),
                         .found = false};
  // This is needed because of DPDK not properly recognising reattached devices.
  // So force traddr.
  bool already_found_once = false;
  for (size_t i = 0; i < found_devices_number; i++) {
    if (found_devices_len[i] == strlen(traddr) &&
        memcmp(found_devices[i], traddr, found_devices_len[i])) {
      already_found_once = true;
    }
  }
  if (already_found_once) {
    memset(manager->g_trid, 0, sizeof(*(manager->g_trid)));
    spdk_nvme_trid_populate_transport(manager->g_trid,
                                      SPDK_NVME_TRANSPORT_PCIE);
    memcpy(manager->g_trid->traddr, traddr,
           spdk_min(strlen(traddr), sizeof(manager->g_trid->traddr)));
  }
  // Find controller.
  int probe_ctx;
  probe_ctx = spdk_nvme_probe(manager->g_trid, &prober,
                              (spdk_nvme_probe_cb)__szd_open_probe_cb,
                              (spdk_nvme_attach_cb)__szd_open_attach_cb,
                              (spdk_nvme_remove_cb)__szd_open_remove_cb);
  // Dettach if broken.
  if (probe_ctx != 0) {
    if (manager->ctrlr != NULL) {
      return spdk_nvme_detach(manager->ctrlr) || SZD_SC_SPDK_ERROR_OPEN;
    } else {
      return SZD_SC_SPDK_ERROR_OPEN;
    }
  }
  if (!prober.found) {
    if (manager->ctrlr != NULL) {
      return spdk_nvme_detach(manager->ctrlr) || SZD_SC_SPDK_ERROR_OPEN;
    } else {
      return SZD_SC_SPDK_ERROR_OPEN;
    }
  }
  // Setup information immediately.
  int rc = szd_get_device_info(&manager->info, manager);
  if (rc != 0) {
    return rc;
  }
  rc = __szd_open_create_private(manager, options);
  if (rc != 0) {
    return rc;
  }
  // Create a container.
  DeviceManagerInternal *private_ = (DeviceManagerInternal *)manager->private_;
  manager->info.min_lba = private_->zone_min_ * manager->info.zone_size;
  manager->info.max_lba = private_->zone_max_ * manager->info.zone_size;
  SZD_DTRACE_PROBE(szd_open);
  return rc;
}

int szd_close(DeviceManager *manager) {
  RETURN_ERR_ON_NULL(manager);
  if (spdk_unlikely(manager->ctrlr == NULL)) {
    return SZD_SC_NOT_ALLOCATED;
  }
  int rc = spdk_nvme_detach(manager->ctrlr);
  manager->ctrlr = NULL;
  manager->ns = NULL;
  // Prevents wrongly assuming a device is attached.
  manager->info = DeviceInfo_default;
  manager->info.name = "\xef\xbe\xad\xde";
  if (manager->private_ != NULL) {
    free(manager->private_);
    manager->private_ = NULL;
  }
  if (manager->g_trid != NULL) {
    memset(manager->g_trid, 0, sizeof(*(manager->g_trid)));
  }
  SZD_DTRACE_PROBE(szd_closed);
  return rc != 0 ? SZD_SC_SPDK_ERROR_CLOSE : SZD_SC_SUCCESS;
}

int szd_destroy(DeviceManager *manager) {
  RETURN_ERR_ON_NULL(manager);
  int rc = SZD_SC_SUCCESS;
  if (manager->ctrlr != NULL) {
    rc = szd_close(manager);
  }
  if (manager->g_trid != NULL) {
    free(manager->g_trid);
    manager->g_trid = NULL;
  }
  free(manager);
  spdk_env_fini();
  SZD_DTRACE_PROBE(szd_destroy);
  return rc;
}

int szd_reinit(DeviceManager **manager) {
  RETURN_ERR_ON_NULL(manager);
  RETURN_ERR_ON_NULL(*manager);
  const char *name = (*manager)->info.name;
  int rc = szd_destroy(*manager);
  if (rc != 0) {
    return SZD_SC_SPDK_ERROR_CLOSE;
  }
  DeviceOptions options = {.name = name, .setup_spdk = false};
  return szd_init(manager, &options);
}

bool __szd_probe_probe_cb(void *cb_ctx,
                          const struct spdk_nvme_transport_id *trid,
                          struct spdk_nvme_ctrlr_opts *opts) {
  (void)cb_ctx;
  (void)trid;
  (void)opts;
  return true;
}

void __szd_probe_attach_cb(void *cb_ctx,
                           const struct spdk_nvme_transport_id *trid,
                           struct spdk_nvme_ctrlr *ctrlr,
                           const struct spdk_nvme_ctrlr_opts *opts) {
  ProbeInformation *prober = (ProbeInformation *)cb_ctx;
  // Very important lock! We probe concurrently and alter one struct.
  pthread_mutex_lock(prober->mut);
  if (prober->devices >= MAX_DEVICE_COUNT - 1) {
    SPDK_ERRLOG("SZD: At the moment no more than %x devices are supported \n",
                MAX_DEVICE_COUNT);
  } else {
    prober->traddr[prober->devices] =
        (char *)calloc(strlen(trid->traddr) + 1, sizeof(char));
    memcpy(prober->traddr[prober->devices], trid->traddr, strlen(trid->traddr));
    prober->ctrlr[prober->devices] = ctrlr;
    for (int nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
         nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
      struct spdk_nvme_ns *ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
      prober->zns[prober->devices] =
          spdk_nvme_ns_get_csi(ns) == SPDK_NVME_CSI_ZNS;
    }
    prober->devices++;
    // hidden global state...
    bool found = false;
    for (size_t i = 0; i < found_devices_number; i++) {
      if (found_devices_len[i] == strlen(trid->traddr) &&
          memcmp(found_devices[i], trid->traddr, found_devices_len[i])) {
        found = true;
      }
    }
    if (!found) {
      found_devices_len[found_devices_number] = strlen(trid->traddr);
      found_devices[found_devices_number] =
          (char *)calloc(found_devices_len[found_devices_number], sizeof(char));
      memcpy(found_devices[found_devices_number], trid->traddr,
             found_devices_len[found_devices_number]);
      found_devices_number++;
    }
  }
  pthread_mutex_unlock(prober->mut);
  (void)opts;
}

int szd_probe(DeviceManager *manager, ProbeInformation **probe) {
  RETURN_ERR_ON_NULL(manager);
  RETURN_ERR_ON_NULL(probe);
  *probe = (ProbeInformation *)calloc(1, sizeof(ProbeInformation));
  RETURN_ERR_ON_NULL(*probe);
  (*probe)->traddr = (char **)calloc(MAX_DEVICE_COUNT, sizeof(char *));
  (*probe)->ctrlr = (struct spdk_nvme_ctrlr **)calloc(
      MAX_DEVICE_COUNT, sizeof(t_spdk_nvme_ctrlr *));
  (*probe)->zns = (bool *)calloc(MAX_DEVICE_COUNT, sizeof(bool));
  (*probe)->mut = (pthread_mutex_t *)calloc(1, sizeof(pthread_mutex_t));
  if (pthread_mutex_init((*probe)->mut, NULL) != 0) {
    return SZD_SC_SPDK_ERROR_PROBE;
  }
  int rc;
  rc = spdk_nvme_probe(manager->g_trid, *probe,
                       (spdk_nvme_probe_cb)__szd_probe_probe_cb,
                       (spdk_nvme_attach_cb)__szd_probe_attach_cb, NULL);
  if (rc != 0) {
    return SZD_SC_SPDK_ERROR_PROBE;
  }
  // Thread safe removing of devices, they have already been probed.
  pthread_mutex_lock((*probe)->mut);
  for (size_t i = 0; i < (*probe)->devices; i++) {
    // keep error message.
    rc = spdk_nvme_detach((*probe)->ctrlr[i]) | rc;
  }
  pthread_mutex_unlock((*probe)->mut);
  return rc != 0 ? SZD_SC_SPDK_ERROR_PROBE : SZD_SC_SUCCESS;
}

void szd_free_probe_information(ProbeInformation *probe_info) {
  free(probe_info->zns);
  for (uint8_t i = 0; i < probe_info->devices; i++) {
    free(probe_info->traddr[i]);
  }
  free(probe_info->traddr);
  free(probe_info->ctrlr);
  free(probe_info->mut);
  free(probe_info);
}

int szd_create_qpair(DeviceManager *man, QPair **qpair) {
  RETURN_ERR_ON_NULL(man);
  RETURN_ERR_ON_NULL(man->ctrlr);
  RETURN_ERR_ON_NULL(qpair);
  *qpair = (QPair *)calloc(1, sizeof(QPair));
  RETURN_ERR_ON_NULL(*qpair);
  (*qpair)->qpair = spdk_nvme_ctrlr_alloc_io_qpair(man->ctrlr, NULL, 0);
  (*qpair)->man = man;
  RETURN_ERR_ON_NULL((*qpair)->qpair);
  SZD_DTRACE_PROBE(szd_create_qpair);
  return SZD_SC_SUCCESS;
}

int szd_destroy_qpair(QPair *qpair) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->qpair);
  spdk_nvme_ctrlr_free_io_qpair(qpair->qpair);
  qpair->man = NULL;
  free(qpair);
  SZD_DTRACE_PROBE(szd_destroy_qpair);
  return SZD_SC_SUCCESS;
}

void *__reserve_dma(uint64_t size) {
  return spdk_zmalloc(size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
}

void *szd_calloc(uint64_t __allign, size_t __nmemb, size_t __size) {
  size_t expanded_size = __nmemb * __size;
  if (spdk_unlikely(expanded_size % __allign != 0 || __allign == 0)) {
    return NULL;
  }
  return spdk_zmalloc(expanded_size, __allign, NULL, SPDK_ENV_SOCKET_ID_ANY,
                      SPDK_MALLOC_DMA);
}

void szd_free(void *buffer) { spdk_free(buffer); }

void __operation_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  Completion *completed = (Completion *)arg;
  completed->done = true;
  // force non error to always be 0.
  completed->err =
      spdk_nvme_cpl_is_error(completion) ? completion->status.sc : 0x00;
}

void __append_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

void __read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

void __reset_zone_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

void __finish_zone_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

void __get_zone_head_complete(void *arg,
                              const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

#define POLL_QPAIR(qpair, target)                                              \
  do {                                                                         \
    spdk_nvme_qpair_process_completions((qpair), 0);                           \
  } while (!(target))

int szd_read_with_diag(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                       uint64_t *nr_reads) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceInfo info = qpair->man->info;

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
  Completion completion = Completion_default;

  // Otherwise we have an out of range.
  uint64_t number_of_zones_traversed =
      (lbas_to_process + (lba - slba)) / info.zone_cap;
  if (spdk_unlikely(lba < info.min_lba ||
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
    // Do not read too much (more than mdts or requested)
    current_step_size = lbas_to_process - lbas_processed > current_step_size
                            ? current_step_size
                            : lbas_to_process - lbas_processed;

    completion.done = false;
    completion.err = 0x00;
    rc = spdk_nvme_ns_cmd_read(qpair->man->ns, qpair->qpair,
                               (char *)buffer + lbas_processed * info.lba_size,
                               lba,               /* LBA start */
                               current_step_size, /* number of LBAs */
                               __read_complete, &completion, 0);
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
    // Synchronous reads, busy wait.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (spdk_unlikely(completion.err != 0)) {
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

int szd_read(QPair *qpair, uint64_t lba, void *buffer, uint64_t size) {
  return szd_read_with_diag(qpair, lba, buffer, size, NULL);
}

int szd_append_with_diag(QPair *qpair, uint64_t *lba, void *buffer,
                         uint64_t size, uint64_t *nr_appends) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceInfo info = qpair->man->info;

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
  Completion completion = Completion_default;

  // Error if we have an out of range.
  uint64_t number_of_zones_traversed =
      (lbas_to_process + (*lba - slba)) / info.zone_cap;
  if (spdk_unlikely(*lba < info.min_lba ||
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

    completion.done = false;
    completion.err = 0x00;

    rc = spdk_nvme_zns_zone_append(
        qpair->man->ns, qpair->qpair,
        (char *)buffer + lbas_processed * info.lba_size, slba, /* LBA start */
        current_step_size, /* number of LBAs */
        __append_complete, &completion, 0);
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
    // Synchronous write, busy wait.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (spdk_unlikely(completion.err != 0)) {
      SPDK_ERRLOG("SZD: Error during append %x\n", completion.err);
      for (uint64_t slba = info.min_lba; slba != info.max_lba;
           slba += info.zone_size) {
        uint64_t zone_head;
        szd_get_zone_head(qpair, slba, &zone_head);
        if (zone_head != slba && zone_head != slba + info.zone_size)
          SPDK_ERRLOG(
              "SZD: Error during append - zone head= [%lu - %lu - %lu]\n",
              slba / info.zone_size, zone_head, slba + info.zone_size);
      }
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

int szd_append(QPair *qpair, uint64_t *lba, void *buffer, uint64_t size) {
  return szd_append_with_diag(qpair, lba, buffer, size, NULL);
}

int szd_append_async_with_diag(QPair *qpair, uint64_t *lba, void *buffer,
                               uint64_t size, uint64_t *nr_appends,
                               Completion *completion) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceInfo info = qpair->man->info;

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
  *completion = Completion_default;

  // Error if we have an out of range or we cross a zone border.
  uint64_t number_of_zones_traversed =
      (lbas_to_process + (*lba - slba)) / info.zone_cap;
  if (spdk_unlikely(*lba < info.min_lba || *lba > info.max_lba ||
                    number_of_zones_traversed > 1 ||
                    lbas_to_process > info.zasl / info.lba_size)) {
    SPDK_ERRLOG("SZD: Async append out of range\n");
    return SZD_SC_SPDK_ERROR_APPEND;
  }

  completion->done = false;
  completion->err = 0x00;
  rc = spdk_nvme_zns_zone_append(qpair->man->ns, qpair->qpair, (char *)buffer,
                                 slba,            /* LBA start */
                                 lbas_to_process, /* number of LBAs */
                                 __append_complete, completion, 0);
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

int szd_append_async(QPair *qpair, uint64_t *lba, void *buffer, uint64_t size,
                     Completion *completion) {
  return szd_append_async_with_diag(qpair, lba, buffer, size, NULL, completion);
}

int szd_poll_async(QPair *qpair, Completion *completion) {
  POLL_QPAIR(qpair->qpair, completion->done);
  if (spdk_unlikely(completion->err != 0)) {
    SPDK_ERRLOG("SZD: Error during polling - code:%x\n", completion->err);
    return SZD_SC_SPDK_ERROR_POLLING;
  }
  return SZD_SC_SUCCESS;
}

int szd_poll_once(QPair *qpair, Completion *completion) {
  if (!completion->done) {
    spdk_nvme_qpair_process_completions(qpair->qpair, 0);
  }
  if (spdk_unlikely(completion->err != 0)) {
    SPDK_ERRLOG("SZD: Error during polling once - code:%x\n", completion->err);
    return SZD_SC_SPDK_ERROR_POLLING;
  }
  return SZD_SC_SUCCESS;
}

void szd_poll_once_raw(QPair *qpair) {
  spdk_nvme_qpair_process_completions(qpair->qpair, 0);
}

int szd_reset(QPair *qpair, uint64_t slba) {
  RETURN_ERR_ON_NULL(qpair);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (spdk_unlikely(slba < info.min_lba || slba >= info.lba_cap)) {
    return SZD_SC_SPDK_ERROR_READ;
  }
  Completion completion = Completion_default;
  int rc =
      spdk_nvme_zns_reset_zone(qpair->man->ns, qpair->qpair,
                               slba,  /* starting LBA of the zone to reset */
                               false, /* don't reset all zones */
                               __reset_zone_complete, &completion);
  if (spdk_unlikely(rc != 0)) {
    return SZD_SC_SPDK_ERROR_RESET;
  }
  // Busy wait
  POLL_QPAIR(qpair->qpair, completion.done);
  if (spdk_unlikely(completion.err != 0)) {
    SPDK_ERRLOG("SZD: Reset error - code:%x \n", completion.err);
    return SZD_SC_SPDK_ERROR_RESET;
  }
  return rc;
}

int szd_reset_all(QPair *qpair) {
  RETURN_ERR_ON_NULL(qpair);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  int rc = SZD_SC_SUCCESS;
  // We can not do full reset, if we only "own" a  part.
  if (info.min_lba > 0 || info.max_lba < info.lba_cap) {
    // What are you doing?
    if (spdk_unlikely(info.min_lba > info.max_lba)) {
      return SZD_SC_SPDK_ERROR_RESET;
    }
    for (uint64_t slba = info.min_lba; slba < info.max_lba;
         slba += info.zone_size) {
      if ((rc = szd_reset(qpair, slba)) != 0) {
        return rc;
      }
    }
  } else {
    Completion completion = Completion_default;
    rc = spdk_nvme_zns_reset_zone(qpair->man->ns, qpair->qpair,
                                  0,    /* starting LBA of the zone to reset */
                                  true, /* reset all zones */
                                  __reset_zone_complete, &completion);
    if (spdk_unlikely(rc != 0)) {
      return SZD_SC_SPDK_ERROR_RESET;
    }
    // Busy wait
    POLL_QPAIR(qpair->qpair, completion.done);
    if (spdk_unlikely(completion.err != 0)) {
      return SZD_SC_SPDK_ERROR_RESET;
    }
  }
  return rc;
}

int szd_finish_zone(QPair *qpair, uint64_t slba) {
  RETURN_ERR_ON_NULL(qpair);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (spdk_unlikely(slba < info.min_lba || slba > info.lba_cap)) {
    return SZD_SC_SPDK_ERROR_FINISH;
  }
  Completion completion = Completion_default;
  int rc =
      spdk_nvme_zns_finish_zone(qpair->man->ns, qpair->qpair,
                                slba,  /* starting LBA of the zone to finish */
                                false, /* don't finish all zones */
                                __finish_zone_complete, &completion);
  if (spdk_unlikely(rc != 0)) {
    return SZD_SC_SPDK_ERROR_FINISH;
  }
  // Busy wait
  POLL_QPAIR(qpair->qpair, completion.done);
  if (spdk_unlikely(completion.err != 0)) {
    return SZD_SC_SPDK_ERROR_FINISH;
  }
  return rc;
}

int szd_get_zone_heads(QPair *qpair, uint64_t slba, uint64_t eslba,
                       uint64_t *write_head) {
  // Inspired by SPDK/nvme/identify.c
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->man);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (spdk_unlikely(slba < info.min_lba || slba > info.max_lba ||
                    eslba < info.min_lba || eslba > info.max_lba ||
                    slba > eslba || slba % info.zone_size != 0 ||
                    eslba % info.zone_size != 0)) {
    return SZD_SC_SPDK_ERROR_READ;
  }

  int rc = SZD_SC_SUCCESS;

  // Setup state variables
  size_t report_bufsize = spdk_nvme_ns_get_max_io_xfer_size(qpair->man->ns);
  uint8_t *report_buf = (uint8_t *)calloc(1, report_bufsize);
  uint64_t reported_zones = 0;
  uint64_t zones_to_report = (eslba - slba) / info.zone_size;

  // Setup logical variables
  const struct spdk_nvme_ns_data *nsdata =
      spdk_nvme_ns_get_data(qpair->man->ns);
  const struct spdk_nvme_zns_ns_data *nsdata_zns =
      spdk_nvme_zns_ns_get_data(qpair->man->ns);
  uint64_t zone_report_size = sizeof(struct spdk_nvme_zns_zone_report);
  uint64_t zone_descriptor_size = sizeof(struct spdk_nvme_zns_zone_desc);
  uint64_t zns_descriptor_size =
      nsdata_zns->lbafe[nsdata->flbas.format].zdes * 64;
  uint64_t max_zones_per_buf =
      zns_descriptor_size
          ? (report_bufsize - zone_report_size) /
                (zone_descriptor_size + zns_descriptor_size)
          : (report_bufsize - zone_report_size) / zone_descriptor_size;

  // Get zone heads iteratively
  while (reported_zones <= zones_to_report) {
    memset(report_buf, 0, report_bufsize);
    // Get as much as we can from SPDK
    Completion completion = Completion_default;
    rc = spdk_nvme_zns_report_zones(
        qpair->man->ns, qpair->qpair, report_buf, report_bufsize, slba,
        SPDK_NVME_ZRA_LIST_ALL, true, __get_zone_head_complete, &completion);
    if (spdk_unlikely(rc != 0)) {
      free(report_buf);
      return SZD_SC_SPDK_ERROR_REPORT_ZONES;
    }
    // Busy wait for the head.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (spdk_unlikely(completion.err != 0)) {
      free(report_buf);
      return SZD_SC_SPDK_ERROR_REPORT_ZONES;
    }

    // retrieve nr_zones
    uint64_t nr_zones = report_buf[0];
    if (nr_zones > max_zones_per_buf || nr_zones == 0) {
      free(report_buf);
      return SZD_SC_SPDK_ERROR_REPORT_ZONES;
    }

    // Retrieve write heads from zone information.
    for (uint64_t i = 0; i < nr_zones && reported_zones <= zones_to_report;
         i++) {
      uint32_t zd_index =
          zone_report_size + i * (zone_descriptor_size + zns_descriptor_size);
      struct spdk_nvme_zns_zone_desc *desc =
          (struct spdk_nvme_zns_zone_desc *)(report_buf + zd_index);
      write_head[reported_zones] = desc->wp;
      if (spdk_unlikely(write_head[reported_zones] < slba)) {
        free(report_buf);
        return SZD_SC_SPDK_ERROR_REPORT_ZONES;
      }
      if (write_head[reported_zones] > slba + desc->zcap) {
        write_head[reported_zones] = slba + info.zone_size;
      }
      // progress
      slba += info.zone_size;
      reported_zones++;
    }
  }
  free(report_buf);
  return SZD_SC_SUCCESS;
}

int szd_get_zone_head(QPair *qpair, uint64_t slba, uint64_t *write_head) {
  return szd_get_zone_heads(qpair, slba, slba, write_head);
}

int szd_get_zone_cap(QPair *qpair, uint64_t slba, uint64_t *zone_cap) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->man);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (spdk_unlikely(slba < info.min_lba || slba > info.max_lba)) {
    return SZD_SC_SPDK_ERROR_READ;
  }

  int rc = SZD_SC_SUCCESS;
  // Get information from a zone.
  size_t report_bufsize = spdk_nvme_ns_get_max_io_xfer_size(qpair->man->ns);
  uint8_t *report_buf = (uint8_t *)calloc(1, report_bufsize);
  {
    Completion completion = Completion_default;
    rc = spdk_nvme_zns_report_zones(
        qpair->man->ns, qpair->qpair, report_buf, report_bufsize, slba,
        SPDK_NVME_ZRA_LIST_ALL, true, __get_zone_head_complete, &completion);
    if (spdk_unlikely(rc != 0)) {
      free(report_buf);
      return SZD_SC_SPDK_ERROR_REPORT_ZONES;
    }
    // Busy wait for the head.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (spdk_unlikely(completion.err != 0)) {
      free(report_buf);
      return SZD_SC_SPDK_ERROR_REPORT_ZONES;
    }
  }
  // Retrieve write head from zone information.
  uint32_t zd_index = sizeof(struct spdk_nvme_zns_zone_report);
  struct spdk_nvme_zns_zone_desc *desc =
      (struct spdk_nvme_zns_zone_desc *)(report_buf + zd_index);
  *zone_cap = desc->zcap;
  free(report_buf);
  return SZD_SC_SUCCESS;
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
