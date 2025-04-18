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
extern "C" {
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

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

// Needed because of DPDK and reattaching, we need to remember what we have
// seen...
static char *found_devices[MAX_DEVICE_COUNT];
static size_t found_devices_len[MAX_DEVICE_COUNT];
static size_t found_devices_number = 0;

int szd_spdk_register_backend(EngineManager *em) {
  ioengine_backend backend = {
      .init = szd_spdk_init,
      .destroy = szd_spdk_destroy,
      .reinit = szd_spdk_reinit,
      .probe = szd_spdk_probe,
      .free_probe = szd_spdk_free_probe_information,
      .open = szd_spdk_open,
      .close = szd_spdk_close,
      .get_device_info = szd_spdk_get_device_info,
      .create_qpair = szd_spdk_create_qpair,
      .destroy_qpair = szd_spdk_destroy_qpair,
      .buf_calloc = szd_spdk_calloc,
      .free = szd_spdk_free,
      .read = szd_spdk_read,
      .write = szd_spdk_write,
      .append = szd_spdk_append,
      .append_async = szd_spdk_append_async,
      .poll_async = szd_spdk_poll_async,
      .poll_once = szd_spdk_poll_once,
      .poll_once_raw = szd_spdk_poll_once_raw,
      .reset_zone = szd_spdk_reset,
      .reset_all_zones = szd_spdk_reset_all,
      .finish_zone = szd_spdk_finish_zone,
      .get_zone_head = szd_spdk_get_zone_head,
      .get_zone_heads = szd_spdk_get_zone_heads,
      .get_zone_cap = szd_spdk_get_zone_cap,
  };
  em->backend = backend;
  return 0;
}

int szd_spdk_init(DeviceManager **dm, DeviceOptions *options) {
  // Setup options
  struct spdk_env_opts opts;
  if (options->setup_spdk) {
    opts.name = options->name;
    spdk_env_opts_init(&opts);
  }
  (*dm)->private_ = calloc(1, sizeof(SPDKManager));
  SPDKManager *man = (SPDKManager *)((*dm)->private_);
  // Setup SPDK
  man->g_trid =
      (t_spdk_nvme_transport_id *)calloc(1, sizeof(t_spdk_nvme_transport_id));
  RETURN_ERR_ON_NULL(man->g_trid);
  spdk_nvme_trid_populate_transport(man->g_trid, SPDK_NVME_TRANSPORT_PCIE);
  if (spdk_unlikely(spdk_env_init(!options->setup_spdk ? NULL : &opts) < 0)) {
    free(man->g_trid);
    return SZD_SC_SPDK_ERROR_INIT;
  }
  // setup stub info, we do not want to create extra UB.
  (*dm)->info = DeviceInfo_default;
  (*dm)->info.name = options->name;
  man->ctrlr = NULL;
  man->ns = NULL;
  return SZD_SC_SUCCESS;
}

int szd_spdk_get_device_info(DeviceInfo *info, DeviceManager *man) {
  SPDKManager *dm = (SPDKManager *)man->private_;
  RETURN_ERR_ON_NULL(dm->ctrlr);
  RETURN_ERR_ON_NULL(dm->ns);
  info->lba_size = (uint64_t)spdk_nvme_ns_get_sector_size(dm->ns);
  info->zone_size = (uint64_t)spdk_nvme_zns_ns_get_zone_size_sectors(dm->ns);
  info->mdts = (uint64_t)spdk_nvme_ctrlr_get_max_xfer_size(dm->ctrlr);
  info->zasl =
      (uint64_t)spdk_nvme_zns_ctrlr_get_max_zone_append_size(dm->ctrlr);
  info->lba_cap = (uint64_t)spdk_nvme_ns_get_num_sectors(dm->ns);
  info->min_lba = man->info.min_lba;
  info->max_lba = man->info.max_lba;
  // printf("INFO: %lu %lu %lu %lu %lu %lu %lu \n", info->lba_size,
  // info->zone_size, info->mdts, info->zasl,
  //   info->lba_cap, info->min_lba, info->max_lba);
  // TODO: zone cap can differ between zones...
  QPair **temp = (QPair **)calloc(1, sizeof(QPair *));
  szd_spdk_create_qpair(man, temp);
  szd_spdk_get_zone_cap(*temp, info->min_lba, &info->zone_cap);
  szd_spdk_destroy_qpair(man, *temp);
  free(temp);
  return SZD_SC_SUCCESS;
}

bool __szd_spdk_open_probe_cb(void *cb_ctx,
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

void __szd_spdk_open_attach_cb(void *cb_ctx,
                               const struct spdk_nvme_transport_id *trid,
                               struct spdk_nvme_ctrlr *ctrlr,
                               const struct spdk_nvme_ctrlr_opts *opts) {
  DeviceTarget *prober = (DeviceTarget *)cb_ctx;
  if (prober == NULL) {
    return;
  }
  SPDKManager *man = (SPDKManager *)(prober->manager->private_);
  man->ctrlr = ctrlr;
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
    man->ns = ns;
    prober->found = true;
    break;
  }
  (void)trid;
  (void)opts;
  return;
}

void __szd_spdk_open_remove_cb(void *cb_ctx, struct spdk_nvme_ctrlr *ctrlr) {
  (void)cb_ctx;
  (void)ctrlr;
}

int szd_spdk_open(DeviceManager *manager, const char *traddr,
                  DeviceOpenOptions *options) {
  (void)options;
  SPDKManager *man = (SPDKManager *)(manager->private_);
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
    memset(man->g_trid, 0, sizeof(*(man->g_trid)));
    spdk_nvme_trid_populate_transport(man->g_trid, SPDK_NVME_TRANSPORT_PCIE);
    memcpy(man->g_trid->traddr, traddr,
           spdk_min(strlen(traddr), sizeof(man->g_trid->traddr)));
  }
  // Find controller.
  int probe_ctx;
  probe_ctx = spdk_nvme_probe(man->g_trid, &prober,
                              (spdk_nvme_probe_cb)__szd_spdk_open_probe_cb,
                              (spdk_nvme_attach_cb)__szd_spdk_open_attach_cb,
                              (spdk_nvme_remove_cb)__szd_spdk_open_remove_cb);
  // Dettach if broken.
  if (probe_ctx != 0) {
    if (man->ctrlr != NULL) {
      return spdk_nvme_detach(man->ctrlr) || SZD_SC_SPDK_ERROR_OPEN;
    } else {
      return SZD_SC_SPDK_ERROR_OPEN;
    }
  }
  if (!prober.found) {
    if (man->ctrlr != NULL) {
      return spdk_nvme_detach(man->ctrlr) || SZD_SC_SPDK_ERROR_OPEN;
    } else {
      return SZD_SC_SPDK_ERROR_OPEN;
    }
  }
  return SZD_SC_SUCCESS;
}

int szd_spdk_close(DeviceManager *manager) {
  SPDKManager *man = (SPDKManager *)(manager->private_);
  int rc = 0;
  if (spdk_unlikely(man->ctrlr == NULL)) {
    return SZD_SC_NOT_ALLOCATED;
  } else {
    rc = spdk_nvme_detach(man->ctrlr);
    man->ctrlr = NULL;
    man->ns = NULL;
    // Prevents wrongly assuming a device is attached.
    if (man->g_trid != NULL) {
      memset(man->g_trid, 0, sizeof(*(man->g_trid)));
    }
  }
  return rc != 0 ? SZD_SC_SPDK_ERROR_CLOSE : SZD_SC_SUCCESS;
}

int szd_spdk_destroy(DeviceManager *manager) {
  SPDKManager *man = (SPDKManager *)(manager->private_);
  int rc = SZD_SC_SUCCESS;
  if (man->ctrlr != NULL) {
    rc = szd_spdk_close(manager);
  }
  if (man->g_trid != NULL) {
    free(man->g_trid);
    free(man);
    manager->private_ = NULL;
  }
  spdk_env_fini();
  return rc;
}

int szd_spdk_reinit(DeviceManager **manager) {
  const char *name = (*manager)->info.name;
  int rc = szd_spdk_destroy(*manager);
  if (rc != 0) {
    return SZD_SC_SPDK_ERROR_CLOSE;
  }
  DeviceOptions options = {.name = name, .setup_spdk = false};
  return szd_spdk_init(manager, &options);
}

bool __szd_spdk_probe_probe_cb(void *cb_ctx,
                               const struct spdk_nvme_transport_id *trid,
                               struct spdk_nvme_ctrlr_opts *opts) {
  (void)cb_ctx;
  (void)trid;
  (void)opts;
  return true;
}

void __szd_spdk_probe_attach_cb(void *cb_ctx,
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

int szd_spdk_probe(DeviceManager *manager, void **probe) {
  RETURN_ERR_ON_NULL(manager);
  RETURN_ERR_ON_NULL(manager->private_);

  SPDKManager *man = (SPDKManager *)(manager->private_);
  RETURN_ERR_ON_NULL(probe);
  ProbeInformation *p = (ProbeInformation *)calloc(1, sizeof(ProbeInformation));
  *probe = p;
  RETURN_ERR_ON_NULL(*probe);
  p->traddr = (char **)calloc(MAX_DEVICE_COUNT, sizeof(char *));
  p->ctrlr = (struct spdk_nvme_ctrlr **)calloc(MAX_DEVICE_COUNT,
                                               sizeof(t_spdk_nvme_ctrlr *));
  p->zns = (bool *)calloc(MAX_DEVICE_COUNT, sizeof(bool));
  p->mut = (pthread_mutex_t *)calloc(1, sizeof(pthread_mutex_t));
  if (pthread_mutex_init(p->mut, NULL) != 0) {
    return SZD_SC_SPDK_ERROR_PROBE;
  }
  int rc;
  rc = spdk_nvme_probe(man->g_trid, *probe,
                       (spdk_nvme_probe_cb)__szd_spdk_probe_probe_cb,
                       (spdk_nvme_attach_cb)__szd_spdk_probe_attach_cb, NULL);
  if (rc != 0) {
    return SZD_SC_SPDK_ERROR_PROBE;
  }
  // Thread safe removing of devices, they have already been probed.
  pthread_mutex_lock(p->mut);
  for (size_t i = 0; i < p->devices; i++) {
    // keep error message.
    rc = spdk_nvme_detach(p->ctrlr[i]) | rc;
  }
  pthread_mutex_unlock(p->mut);
  return rc != 0 ? SZD_SC_SPDK_ERROR_PROBE : SZD_SC_SUCCESS;
}

void szd_spdk_free_probe_information(DeviceManager *manager, void *probe_info) {
  (void)manager;
  ProbeInformation *pi = (ProbeInformation *)probe_info;
  free(pi->zns);
  for (uint8_t i = 0; i < pi->devices; i++) {
    free(pi->traddr[i]);
  }
  free(pi->traddr);
  free(pi->ctrlr);
  free(pi->mut);
  free(pi);
}

int szd_spdk_create_qpair(DeviceManager *man, QPair **qpair) {
  SPDKManager *spdk_man = (SPDKManager *)(man->private_);
  RETURN_ERR_ON_NULL(spdk_man->ctrlr);
  RETURN_ERR_ON_NULL(qpair);
  *qpair = (QPair *)calloc(1, sizeof(QPair));
  RETURN_ERR_ON_NULL(*qpair);
  (*qpair)->qpair = spdk_nvme_ctrlr_alloc_io_qpair(spdk_man->ctrlr, NULL, 0);
  (*qpair)->man = man;
  RETURN_ERR_ON_NULL((*qpair)->qpair);
  SZD_DTRACE_PROBE(szd_create_qpair);
  return SZD_SC_SUCCESS;
}

int szd_spdk_destroy_qpair(DeviceManager *man, QPair *qpair) {
  (void)man;
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

void *szd_spdk_calloc(uint64_t __allign, size_t __nmemb, size_t __size) {
  size_t expanded_size = __nmemb * __size;
  if (spdk_unlikely(expanded_size % __allign != 0 || __allign == 0)) {
    return NULL;
  }
  return spdk_zmalloc(expanded_size, __allign, NULL, SPDK_ENV_SOCKET_ID_ANY,
                      SPDK_MALLOC_DMA);
}

void szd_spdk_free(void *buffer) { spdk_free(buffer); }

void __spdk_operation_complete(void *arg,
                               const struct spdk_nvme_cpl *completion) {
  Completion *completed = (Completion *)arg;
  completed->done = true;
  // force non error to always be 0.
  completed->err =
      spdk_nvme_cpl_is_error(completion) ? completion->status.sc : 0x00;
}

void __spdk_append_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __spdk_operation_complete(arg, completion);
}

void __spdk_read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
  __spdk_operation_complete(arg, completion);
}

void __spdk_reset_zone_complete(void *arg,
                                const struct spdk_nvme_cpl *completion) {
  __spdk_operation_complete(arg, completion);
}

void __spdk_finish_zone_complete(void *arg,
                                 const struct spdk_nvme_cpl *completion) {
  __spdk_operation_complete(arg, completion);
}

void __spdk_get_zone_head_complete(void *arg,
                                   const struct spdk_nvme_cpl *completion) {
  __spdk_operation_complete(arg, completion);
}

#define POLL_QPAIR(qpair, target)                                              \
  do {                                                                         \
    spdk_nvme_qpair_process_completions((qpair), 0);                           \
  } while (!(target))

int szd_spdk_read(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                  uint64_t blocks) {
  (void)size;
  RETURN_ERR_ON_NULL(qpair);
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);
  int rc = SZD_SC_SUCCESS;

  Completion completion;
  completion.done = false;
  completion.err = 0x00;
  rc = spdk_nvme_ns_cmd_read(spdk_man->ns, qpair->qpair, buffer,
                             lba,    /* LBA start */
                             blocks, /* number of LBAs */
                             __spdk_read_complete, &completion, 0);
  if (spdk_unlikely(rc != 0)) {
    return SZD_SC_SPDK_ERROR_READ;
  }
  // Synchronous reads, busy wait.
  POLL_QPAIR(qpair->qpair, completion.done);
  if (spdk_unlikely(completion.err != 0)) {
    return SZD_SC_SPDK_ERROR_READ;
  }
  return SZD_SC_SUCCESS;
}

int szd_spdk_append(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                    uint64_t blocks) {
  (void)size;
  RETURN_ERR_ON_NULL(qpair);
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);
  int rc = SZD_SC_SUCCESS;

  Completion completion;
  completion.done = false;
  completion.err = 0x00;

  rc = spdk_nvme_zns_zone_append(spdk_man->ns, qpair->qpair, buffer,
                                 lba,    /* LBA start */
                                 blocks, /* number of LBAs */
                                 __spdk_append_complete, &completion, 0);
  if (spdk_unlikely(rc != 0)) {
    SPDK_ERRLOG("SZD: Error creating append request\n");
    return SZD_SC_SPDK_ERROR_APPEND;
  }
  // Synchronous write, busy wait.
  POLL_QPAIR(qpair->qpair, completion.done);
  if (spdk_unlikely(completion.err != 0)) {
    SPDK_ERRLOG("SZD: Error during append %x\n", completion.err);
    return SZD_SC_SPDK_ERROR_APPEND;
  }
  // To the next zone we go
  return SZD_SC_SUCCESS;
}

int szd_spdk_append_async(QPair *qpair, uint64_t lba, void *buffer,
                          uint64_t size, uint64_t blocks,
                          Completion *completion) {
  (void)size;
  RETURN_ERR_ON_NULL(qpair);
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);
  int rc = SZD_SC_SUCCESS;

  *completion = Completion_default;
  completion->done = false;
  completion->err = 0x00;
  rc = spdk_nvme_zns_zone_append(spdk_man->ns, qpair->qpair, buffer,
                                 lba,    /* LBA start */
                                 blocks, /* number of LBAs */
                                 __spdk_append_complete, completion, 0);
  if (spdk_unlikely(rc != 0)) {
    SPDK_ERRLOG("SZD: Error creating append request\n");
    return SZD_SC_SPDK_ERROR_APPEND;
  }
  return SZD_SC_SUCCESS;
}

int szd_spdk_write(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                   uint64_t blocks) {
  (void)size;
  RETURN_ERR_ON_NULL(qpair);
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);
  int rc = SZD_SC_SUCCESS;

  Completion completion;
  completion.done = false;
  completion.err = 0x00;
  rc = spdk_nvme_ns_cmd_write(spdk_man->ns, qpair->qpair, buffer,
                              lba,    /* LBA start */
                              blocks, /* number of LBAs */
                              __spdk_append_complete, &completion, 0);
  if (spdk_unlikely(rc != 0)) {
    SPDK_ERRLOG("SZD: Error creating write request\n");
    return SZD_SC_SPDK_ERROR_APPEND;
  }
  // Synchronous write, busy wait.
  POLL_QPAIR(qpair->qpair, completion.done);
  if (spdk_unlikely(completion.err != 0)) {
    return SZD_SC_SPDK_ERROR_WRITE;
  }
  return SZD_SC_SUCCESS;
}

int szd_spdk_poll_async(QPair *qpair, Completion *completion) {
  POLL_QPAIR(qpair->qpair, completion->done);
  if (spdk_unlikely(completion->err != 0)) {
    SPDK_ERRLOG("SZD: Error during polling - code:%x\n", completion->err);
    return SZD_SC_SPDK_ERROR_POLLING;
  }
  return SZD_SC_SUCCESS;
}

int szd_spdk_poll_once(QPair *qpair, Completion *completion) {
  if (!completion->done) {
    spdk_nvme_qpair_process_completions(qpair->qpair, 0);
  }
  if (spdk_unlikely(completion->err != 0)) {
    SPDK_ERRLOG("SZD: Error during polling once - code:%x\n", completion->err);
    return SZD_SC_SPDK_ERROR_POLLING;
  }
  return SZD_SC_SUCCESS;
}

void szd_spdk_poll_once_raw(QPair *qpair) {
  spdk_nvme_qpair_process_completions(qpair->qpair, 0);
}

int szd_spdk_reset(QPair *qpair, uint64_t slba) {
  RETURN_ERR_ON_NULL(qpair);
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (spdk_unlikely(slba < info.min_lba || slba >= info.lba_cap)) {
    return SZD_SC_SPDK_ERROR_READ;
  }
  Completion completion = Completion_default;
  int rc = spdk_nvme_zns_reset_zone(
      spdk_man->ns, qpair->qpair, slba, /* starting LBA of the zone to reset */
      false,                            /* don't reset all zones */
      __spdk_reset_zone_complete, &completion);
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

int szd_spdk_reset_all(QPair *qpair) {
  RETURN_ERR_ON_NULL(qpair);
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);

  Completion completion = Completion_default;
  int rc = spdk_nvme_zns_reset_zone(spdk_man->ns, qpair->qpair,
                                    0, /* starting LBA of the zone to reset */
                                    true, /* reset all zones */
                                    __spdk_reset_zone_complete, &completion);
  if (spdk_unlikely(rc != 0)) {
    return SZD_SC_SPDK_ERROR_RESET;
  }
  // Busy wait
  POLL_QPAIR(qpair->qpair, completion.done);
  if (spdk_unlikely(completion.err != 0)) {
    return SZD_SC_SPDK_ERROR_RESET;
  }
  return rc;
}

int szd_spdk_finish_zone(QPair *qpair, uint64_t slba) {
  RETURN_ERR_ON_NULL(qpair);
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);

  Completion completion = Completion_default;
  int rc = spdk_nvme_zns_finish_zone(
      spdk_man->ns, qpair->qpair, slba, /* starting LBA of the zone to finish */
      false,                            /* don't finish all zones */
      __spdk_finish_zone_complete, &completion);
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

int szd_spdk_get_zone_heads(QPair *qpair, uint64_t slba, uint64_t eslba,
                            uint64_t *write_head) {
  // Inspired by SPDK/nvme/identify.c
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->man);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);
  int rc = SZD_SC_SUCCESS;

  // Setup state variables
  size_t report_bufsize = spdk_nvme_ns_get_max_io_xfer_size(spdk_man->ns);
  uint8_t *report_buf = (uint8_t *)calloc(1, report_bufsize);
  uint64_t reported_zones = 0;
  uint64_t zones_to_report = (eslba - slba) / info.zone_size;
  struct spdk_nvme_zns_zone_report *zns_report;

  // Setup logical variables
  const struct spdk_nvme_ns_data *nsdata = spdk_nvme_ns_get_data(spdk_man->ns);
  const struct spdk_nvme_zns_ns_data *nsdata_zns =
      spdk_nvme_zns_ns_get_data(spdk_man->ns);
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
  do {
    memset(report_buf, 0, report_bufsize);
    // Get as much as we can from SPDK
    Completion completion = Completion_default;
    rc = spdk_nvme_zns_report_zones(spdk_man->ns, qpair->qpair, report_buf,
                                    report_bufsize, slba,
                                    SPDK_NVME_ZRA_LIST_ALL, true,
                                    __spdk_get_zone_head_complete, &completion);
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
    zns_report = (struct spdk_nvme_zns_zone_report *)report_buf;
    uint64_t nr_zones = zns_report->nr_zones;
    if (nr_zones > max_zones_per_buf || nr_zones == 0) {
      free(report_buf);
      return SZD_SC_SPDK_ERROR_REPORT_ZONES;
    }

    // Retrieve write heads from zone information.
    for (uint64_t i = 0; i < nr_zones && reported_zones <= zones_to_report;
         i++) {
      struct spdk_nvme_zns_zone_desc *desc = &zns_report->descs[i];
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
  } while (reported_zones < zones_to_report);
  free(report_buf);
  return SZD_SC_SUCCESS;
}

int szd_spdk_get_zone_head(QPair *qpair, uint64_t slba, uint64_t *write_head) {
  return szd_spdk_get_zone_heads(qpair, slba, slba, write_head);
}

int szd_spdk_get_zone_cap(QPair *qpair, uint64_t slba, uint64_t *zone_cap) {
  RETURN_ERR_ON_NULL(qpair);
  RETURN_ERR_ON_NULL(qpair->man);
  SPDKManager *spdk_man = (SPDKManager *)(qpair->man->private_);
  // Otherwise we have an out of range.
  DeviceInfo info = qpair->man->info;
  if (spdk_unlikely(slba < info.min_lba || slba > info.max_lba)) {
    return SZD_SC_SPDK_ERROR_READ;
  }

  int rc = SZD_SC_SUCCESS;
  // Get information from a zone.
  size_t report_bufsize = spdk_nvme_ns_get_max_io_xfer_size(spdk_man->ns);
  uint8_t *report_buf = (uint8_t *)calloc(1, report_bufsize);
  {
    Completion completion = Completion_default;
    rc = spdk_nvme_zns_report_zones(spdk_man->ns, qpair->qpair, report_buf,
                                    report_bufsize, slba,
                                    SPDK_NVME_ZRA_LIST_ALL, true,
                                    __spdk_get_zone_head_complete, &completion);
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

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
