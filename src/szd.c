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
namespace SimpleZNSDeviceNamespace {
#endif

const DeviceOptions DeviceOptions_default = {"znsdevice", true};
const DeviceOpenOptions DeviceOpenOptions_default = {0, 0};
const Completion Completion_default = {false, SZD_SC_SUCCESS};
const DeviceManagerInternal DeviceManagerInternal_default = {0, 0};
const DeviceInfo DeviceInfo_default = {0, 0, 0, 0, 0, 0, 0, "SZD"};

// Needed because of DPDK and reattaching, we need to remember what we have seen...
static char* found_devices[MAX_DEVICE_COUNT];
static size_t found_devices_len[MAX_DEVICE_COUNT];
static size_t found_devices_number = 0;

#define RETURN_ERR_ON_NULL(x)                                                  \
  do {                                                                         \
    if ((x) == NULL) {                                                         \
      return (SZD_SC_NOT_ALLOCATED);                                           \
    }                                                                          \
  } while (0)

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
  if (spdk_env_init(!options->setup_spdk ? NULL : &opts) < 0) {
    free(*manager);
    return SZD_SC_SPDK_ERROR_INIT;
  }
  // setup stub info, we do not want to create extra UB.
  (*manager)->info = DeviceInfo_default;
  (*manager)->info.name = options->name;
  (*manager)->ctrlr = NULL;
  (*manager)->ns = NULL;
  (*manager)->private_ = NULL;
  return SZD_SC_SUCCESS;
}

int szd_get_device_info(DeviceInfo *info, DeviceManager *manager) {
  RETURN_ERR_ON_NULL(info);
  RETURN_ERR_ON_NULL(manager);
  RETURN_ERR_ON_NULL(manager->ctrlr);
  RETURN_ERR_ON_NULL(manager->ns);
  const struct spdk_nvme_ns_data *ns_data = spdk_nvme_ns_get_data(manager->ns);
  const t_spdk_nvme_zns_ns_data *ns_data_zns =
      spdk_nvme_zns_ns_get_data(manager->ns);
  const t_spdk_nvme_ctrlr_data *ctrlr_data =
      spdk_nvme_ctrlr_get_data(manager->ctrlr);
  const t_spdk_nvme_zns_ctrlr_data *ctrlr_data_zns =
      spdk_nvme_zns_ctrlr_get_data(manager->ctrlr);
  union spdk_nvme_cap_register cap =
      spdk_nvme_ctrlr_get_regs_cap(manager->ctrlr);

  info->lba_size = 1UL << ns_data->lbaf[ns_data->flbas.format].lbads;
  info->zone_size = ns_data_zns->lbafe[ns_data->flbas.format].zsze;
  info->mdts = (uint64_t)1 << (12U + cap.bits.mpsmin + ctrlr_data->mdts);
  info->zasl = (uint64_t)ctrlr_data_zns->zasl;
  // If zasl is not set, it is equal to mdts.
  info->zasl = info->zasl == 0UL
                   ? info->mdts
                   : (uint64_t)1 << (12U + cap.bits.mpsmin + info->zasl);
  info->lba_cap = ns_data->ncap;
  info->min_lba = manager->info.min_lba;
  info->max_lba = manager->info.max_lba;
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
  // This is needed because of DPDK not properly recognising reattached devices. So force traddr.
  bool already_found_once = false;
  for (size_t i = 0; i< found_devices_number; i++) {
    if (found_devices_len[i] == strlen(traddr) && 
      memcmp(found_devices[i], traddr, found_devices_len[i])) {
        already_found_once = true;
      }
  }
  if (already_found_once) {
    memset(manager->g_trid, 0, sizeof(*(manager->g_trid)));
	  spdk_nvme_trid_populate_transport(manager->g_trid, SPDK_NVME_TRANSPORT_PCIE);
    memcpy(manager->g_trid->traddr, traddr, spdk_min(strlen(traddr), sizeof(manager->g_trid->traddr)));
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
  return rc;
}

int szd_close(DeviceManager *manager) {
  RETURN_ERR_ON_NULL(manager);
  RETURN_ERR_ON_NULL(manager->ctrlr);
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
    printf("At the moment no more than %x devices are supported \n",
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
    for (size_t i = 0; i< found_devices_number; i++) {
      if (found_devices_len[i] == strlen(trid->traddr) && 
        memcmp(found_devices[i], trid->traddr, found_devices_len[i])) {
          found = true;
        }
    }
    if (!found) {
      found_devices_len[found_devices_number] = strlen(trid->traddr);
      found_devices[found_devices_number] = (char*)calloc(found_devices_len[found_devices_number], 
        sizeof(char));
      memcpy(found_devices[found_devices_number], trid->traddr, found_devices_len[found_devices_number]);
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
  return SZD_SC_SUCCESS;
}

int szd_destroy_qpair(QPair *qpair) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->qpair);
  spdk_nvme_ctrlr_free_io_qpair(qpair->qpair);
  qpair->man = NULL;
  free(qpair);
  return SZD_SC_SUCCESS;
}

void *__reserve_dma(uint64_t size) {
  return spdk_zmalloc(size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
}

void *szd_calloc(uint64_t __allign, size_t __nmemb, size_t __size) {
  size_t expanded_size = __nmemb * __size;
  if (expanded_size % __allign != 0 || __allign == 0) {
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

void __get_zone_head_complete(void *arg,
                              const struct spdk_nvme_cpl *completion) {
  __operation_complete(arg, completion);
}

#define POLL_QPAIR(qpair, target)                                              \
  do {                                                                         \
    while (!(target)) {                                                        \
      spdk_nvme_qpair_process_completions((qpair), 0);                         \
    }                                                                          \
  } while (0)

int szd_read(QPair *qpair, uint64_t lba, void *buffer, uint64_t size) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceInfo info = qpair->man->info;

  uint64_t lba_start = lba;
  uint64_t lbas_to_process = (size + info.lba_size - 1) / info.lba_size;
  uint64_t lbas_processed = 0;
  // If lba_size > mdts, we have a big problem, but not because of the read.
  uint64_t step_size = (info.mdts / info.lba_size);
  uint64_t current_step_size = step_size;
  Completion completion = Completion_default;

  // Otherwise we have an out of range.
  if (lba < info.min_lba || lba + lbas_to_process > info.max_lba) {
    return SZD_SC_SPDK_ERROR_READ;
  }

  while (lbas_processed < lbas_to_process) {
    // Read accross a zone border.
    if ((lba + lbas_processed + step_size) / info.zone_size >
        (lba + lbas_processed) / info.zone_size) {
      current_step_size =
          ((lba + lbas_processed + step_size) / info.zone_size) *
              info.zone_size -
          lbas_processed - lba;
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
                               lba_start,         /* LBA start */
                               current_step_size, /* number of LBAs */
                               __read_complete, &completion, 0);
    if (rc != 0) {
      return SZD_SC_SPDK_ERROR_READ;
    }
    // Synchronous reads, busy wait.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      return SZD_SC_SPDK_ERROR_READ;
    }
    lbas_processed += current_step_size;
    lba_start = lba + lbas_processed;
  }
  return SZD_SC_SUCCESS;
}

int szd_append(QPair *qpair, uint64_t *lba, void *buffer, uint64_t size) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(buffer);
  int rc = SZD_SC_SUCCESS;
  DeviceInfo info = qpair->man->info;

  uint64_t lba_start = (*lba / info.zone_size) * info.zone_size;
  uint64_t lbas_to_process = (size + info.lba_size - 1) / info.lba_size;
  uint64_t lbas_processed = 0;
  // If lba_size > zasl, we have a big problem, but not because of the append.
  uint64_t step_size = (info.zasl / info.lba_size);
  uint64_t current_step_size = step_size;
  Completion completion = Completion_default;

  // Otherwise we have an out of range.
  if (*lba < info.min_lba || *lba + lbas_to_process > info.max_lba) {
    return SZD_SC_SPDK_ERROR_READ;
  }

  while (lbas_processed < lbas_to_process) {
    // Append across a zone border.
    if ((*lba + lbas_processed + step_size) / info.zone_size >
        (*lba + lbas_processed) / info.zone_size) {
      current_step_size =
          ((*lba + lbas_processed + step_size) / info.zone_size) *
              info.zone_size -
          lbas_processed - *lba;
    } else {
      current_step_size = step_size;
    }
    // Do not append too much (more than ZASL or what is requested)
    current_step_size = lbas_to_process - lbas_processed > current_step_size
                            ? current_step_size
                            : lbas_to_process - lbas_processed;

    completion.done = false;
    completion.err = 0x00;
    rc = spdk_nvme_zns_zone_append(qpair->man->ns, qpair->qpair,
                                   (char *)buffer +
                                       lbas_processed * info.lba_size,
                                   lba_start,         /* LBA start */
                                   current_step_size, /* number of LBAs */
                                   __append_complete, &completion, 0);
    if (rc != 0) {
      return SZD_SC_SPDK_ERROR_APPEND;
    }
    // Synchronous write, busy wait.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      *lba = *lba + lbas_processed;
      return SZD_SC_SPDK_ERROR_APPEND;
    }
    lbas_processed += current_step_size;
    lba_start = ((*lba + lbas_processed) / info.zone_size) * info.zone_size;
  }
  *lba = *lba + lbas_processed;
  return SZD_SC_SUCCESS;
}

int szd_reset(QPair *qpair, uint64_t slba) {
  RETURN_ERR_ON_NULL(qpair);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (slba < info.min_lba || slba > info.lba_cap) {
    return SZD_SC_SPDK_ERROR_READ;
  }
  Completion completion = Completion_default;
  int rc =
      spdk_nvme_zns_reset_zone(qpair->man->ns, qpair->qpair,
                               slba,  /* starting LBA of the zone to reset */
                               false, /* don't reset all zones */
                               __reset_zone_complete, &completion);
  if (rc != 0) {
    return SZD_SC_SPDK_ERROR_RESET;
  }
  // Busy wait
  POLL_QPAIR(qpair->qpair, completion.done);
  if (completion.err != 0) {
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
    if (info.min_lba > info.max_lba) {
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
    if (rc != 0) {
      return SZD_SC_SPDK_ERROR_RESET;
    }
    // Busy wait
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      return SZD_SC_SPDK_ERROR_RESET;
    }
  }
  return rc;
}

int szd_get_zone_head(QPair *qpair, uint64_t slba, uint64_t *write_head) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->man);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (slba < info.min_lba || slba > info.max_lba) {
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
    if (rc != 0) {
      free(report_buf);
      return SZD_SC_SPDK_ERROR_REPORT_ZONES;
    }
    // Busy wait for the head.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      free(report_buf);
      return SZD_SC_SPDK_ERROR_REPORT_ZONES;
    }
  }
  // Retrieve write head from zone information.
  uint32_t zd_index = sizeof(struct spdk_nvme_zns_zone_report);
  struct spdk_nvme_zns_zone_desc *desc =
      (struct spdk_nvme_zns_zone_desc *)(report_buf + zd_index);
  *write_head = desc->wp;
  free(report_buf);
  if (*write_head < slba) {
    return SZD_SC_SPDK_ERROR_REPORT_ZONES;
  }
  if (*write_head > slba + info.zone_size) {
    *write_head = slba + info.zone_size;
  }
  return SZD_SC_SUCCESS;
}

void szd_print_zns_status(int status) {
  printf("SZS STATUS: %s\n", szd_status_code_msg(status));
}

long int szd_spdk_strtol(const char *nptr, int base) {
  return spdk_strtol(nptr, base);
}

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif