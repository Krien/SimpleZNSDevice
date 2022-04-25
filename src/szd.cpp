#include "szd.h"
#include "utils.h"

namespace SimpleZNSDeviceNamespace {
extern "C" {
int z_init(DeviceManager **manager, DeviceOptions *options) {
  RETURN_CODE_ON_NULL(options, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(manager, ZNS_STATUS_NOT_ALLOCATED);
  *manager = (DeviceManager *)calloc(1, sizeof(DeviceManager));
  RETURN_CODE_ON_NULL(*manager, ZNS_STATUS_NOT_ALLOCATED);
  // Setup options
  struct spdk_env_opts opts;
  if (options->setup_spdk) {
    opts.name = options->name;
    spdk_env_opts_init(&opts);
  }
  // Setup SPDK
  (*manager)->g_trid = {};
  spdk_nvme_trid_populate_transport(&(*manager)->g_trid,
                                    SPDK_NVME_TRANSPORT_PCIE);
  if (spdk_env_init(!options->setup_spdk ? NULL : &opts) < 0) {
    free(*manager);
    return ZNS_STATUS_SPDK_ERROR;
  }
  // setup stub info, we do not want to create extra UB.
  (*manager)->info = {.lba_size = 0,
                      .zone_size = 0,
                      .mdts = 0,
                      .zasl = 0,
                      .lba_cap = 0,
                      .name = options->name};
  return ZNS_STATUS_SUCCESS;
}

int z_get_device_info(DeviceInfo *info, DeviceManager *manager) {
  RETURN_CODE_ON_NULL(info, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(manager, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(manager->ctrlr, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(manager->ns, ZNS_STATUS_NOT_ALLOCATED);
  const struct spdk_nvme_ns_data *ns_data = spdk_nvme_ns_get_data(manager->ns);
  const struct spdk_nvme_zns_ns_data *ns_data_zns =
      spdk_nvme_zns_ns_get_data(manager->ns);
  const struct spdk_nvme_ctrlr_data *ctrlr_data =
      spdk_nvme_ctrlr_get_data(manager->ctrlr);
  const spdk_nvme_zns_ctrlr_data *ctrlr_data_zns =
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
  return ZNS_STATUS_SUCCESS;
}

bool __z_open_probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
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

void __z_open_attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
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

void __z_open_remove_cb(void *cb_ctx, struct spdk_nvme_ctrlr *ctrlr) {
  (void)cb_ctx;
  (void)ctrlr;
}

int z_open(DeviceManager *manager, const char *traddr) {
  DeviceTarget prober = {.manager = manager,
                         .traddr = traddr,
                         .traddr_len = strlen(traddr),
                         .found = false};
  // Find and open device
  int probe_ctx;
  probe_ctx = spdk_nvme_probe(&manager->g_trid, &prober,
                              (spdk_nvme_probe_cb)__z_open_probe_cb,
                              (spdk_nvme_attach_cb)__z_open_attach_cb,
                              (spdk_nvme_remove_cb)__z_open_remove_cb);
  if (probe_ctx != 0) {
    return ZNS_STATUS_NOT_ALLOCATED;
  }
  if (!prober.found) {
    return ZNS_STATUS_SPDK_ERROR;
  }
  return z_get_device_info(&manager->info, manager);
}

int z_close(DeviceManager *manager) {
  RETURN_CODE_ON_NULL(manager, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(manager->ctrlr, ZNS_STATUS_NOT_ALLOCATED);
  int rc = spdk_nvme_detach(manager->ctrlr);
  manager->ctrlr = nullptr;
  manager->ns = nullptr;
  // Prevents wrongly assuming a device is attached.
  manager->info = {.lba_size = 0,
                   .zone_size = 0,
                   .mdts = 0,
                   .zasl = 0,
                   .lba_cap = 0,
                   .name = "\xef\xbe\xad\xde"};
  return rc != 0 ? ZNS_STATUS_SPDK_ERROR : ZNS_STATUS_SUCCESS;
}

int z_destroy(DeviceManager *manager) {
  RETURN_CODE_ON_NULL(manager, ZNS_STATUS_NOT_ALLOCATED);
  int rc = ZNS_STATUS_SUCCESS;
  if (manager->ctrlr != NULL) {
    rc = z_close(manager);
  }
  free(manager);
  spdk_env_fini();
  return rc;
}

int z_reinit(DeviceManager **manager) {
  RETURN_CODE_ON_NULL(manager, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(*manager, ZNS_STATUS_NOT_ALLOCATED);
  const char *name = (*manager)->info.name;
  int rc = z_destroy(*manager);
  if (rc != 0 || manager != NULL) {
    return rc | ZNS_STATUS_UNKNOWN;
  }
  DeviceOptions options = {.name = name, .setup_spdk = false};
  return z_init(manager, &options);
}

bool __z_probe_probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                        struct spdk_nvme_ctrlr_opts *opts) {
  (void)cb_ctx;
  (void)trid;
  (void)opts;
  return true;
}

void __z_probe_attach_cb(void *cb_ctx,
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
  }
  pthread_mutex_unlock(prober->mut);
  (void)opts;
}

int z_probe(DeviceManager *manager, ProbeInformation **probe) {
  RETURN_CODE_ON_NULL(manager, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(probe, ZNS_STATUS_NOT_ALLOCATED);
  *probe = (ProbeInformation *)calloc(1, sizeof(ProbeInformation));
  RETURN_CODE_ON_NULL(*probe, ZNS_STATUS_NOT_ALLOCATED);
  (*probe)->traddr = (char **)calloc(MAX_DEVICE_COUNT, sizeof(char *));
  (*probe)->ctrlr = (struct spdk_nvme_ctrlr **)calloc(
      MAX_DEVICE_COUNT, sizeof(spdk_nvme_ctrlr *));
  (*probe)->zns = (bool *)calloc(MAX_DEVICE_COUNT, sizeof(bool));
  (*probe)->mut = (pthread_mutex_t *)calloc(1, sizeof(pthread_mutex_t));
  if (pthread_mutex_init((*probe)->mut, NULL) != 0) {
    return ZNS_STATUS_NOT_ALLOCATED;
  }
  int rc;
  rc = spdk_nvme_probe(&manager->g_trid, *probe,
                       (spdk_nvme_probe_cb)__z_probe_probe_cb,
                       (spdk_nvme_attach_cb)__z_probe_attach_cb, NULL);
  if (rc != 0) {
    return ZNS_STATUS_SPDK_ERROR;
  }
  // Thread safe removing of devices, they have already been probed.
  pthread_mutex_lock((*probe)->mut);
  for (int i = 0; i < (*probe)->devices; i++) {
    // keep error message.
    rc = spdk_nvme_detach((*probe)->ctrlr[i]) | rc;
  }
  pthread_mutex_unlock((*probe)->mut);
  return rc != 0 ? ZNS_STATUS_SPDK_ERROR : ZNS_STATUS_SUCCESS;
}

int z_create_qpair(DeviceManager *man, QPair **qpair) {
  RETURN_CODE_ON_NULL(man, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(qpair, ZNS_STATUS_NOT_ALLOCATED);
  *qpair = (QPair *)calloc(1, sizeof(QPair));
  (*qpair)->qpair = spdk_nvme_ctrlr_alloc_io_qpair(man->ctrlr, NULL, 0);
  (*qpair)->man = man;
  RETURN_CODE_ON_NULL((*qpair)->qpair, ZNS_STATUS_NOT_ALLOCATED);
  return ZNS_STATUS_SUCCESS;
}

int z_destroy_qpair(QPair *qpair) {
  RETURN_CODE_ON_NULL(qpair, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(qpair->qpair, ZNS_STATUS_NOT_ALLOCATED);
  spdk_nvme_ctrlr_free_io_qpair(qpair->qpair);
  qpair->man = NULL;
  free(qpair);
  return ZNS_STATUS_SUCCESS;
}

void *__reserve_dma(uint64_t size) {
  return spdk_zmalloc(size, 0, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
}

void *z_calloc(QPair *qpair, size_t __nmemb, size_t __size) {
  size_t expanded_size = __nmemb * __size;
  size_t allign = (size_t)qpair->man->info.lba_size;
  if (expanded_size % allign != 0 || allign == 0) {
    return NULL;
  }
  return spdk_zmalloc(expanded_size, allign, NULL, SPDK_ENV_SOCKET_ID_ANY,
                      SPDK_MALLOC_DMA);
}

void z_free(QPair *qpair, void *buffer) {
  spdk_free(buffer);
  (void)qpair;
}

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

int z_read(QPair *qpair, uint64_t lba, void *buffer, uint64_t size) {
  RETURN_CODE_ON_NULL(qpair, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(buffer, ZNS_STATUS_NOT_ALLOCATED);
  int rc = ZNS_STATUS_SUCCESS;
  DeviceInfo info = qpair->man->info;

  uint64_t lba_start = lba;
  uint64_t lbas_to_process = (size + info.lba_size - 1) / info.lba_size;
  uint64_t lbas_processed = 0;
  // If lba_size > mdts, we have a big problem, but not because of the read.
  uint64_t step_size = (info.mdts / info.lba_size);
  uint64_t current_step_size = step_size;
  Completion completion = {};

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
      return ZNS_STATUS_SPDK_ERROR;
    }
    // Synchronous reads, busy wait.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      return ZNS_STATUS_SPDK_ERROR;
    }
    lbas_processed += current_step_size;
    lba_start = lba + lbas_processed;
  }
  return ZNS_STATUS_SUCCESS;
}

int z_append(QPair *qpair, uint64_t lba, void *buffer, uint64_t size) {
  RETURN_CODE_ON_NULL(qpair, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(buffer, ZNS_STATUS_NOT_ALLOCATED);
  int rc = ZNS_STATUS_SUCCESS;
  DeviceInfo info = qpair->man->info;

  uint64_t lba_start = (lba / info.zone_size) * info.zone_size;
  uint64_t lbas_to_process = (size + info.lba_size - 1) / info.lba_size;
  uint64_t lbas_processed = 0;
  // If lba_size > zasl, we have a big problem, but not because of the append.
  uint64_t step_size = (info.zasl / info.lba_size);
  uint64_t current_step_size = step_size;
  Completion completion = {};

  while (lbas_processed < lbas_to_process) {
    // Append across a zone border.
    if ((lba + lbas_processed + step_size) / info.zone_size >
        (lba + lbas_processed) / info.zone_size) {
      current_step_size =
          ((lba + lbas_processed + step_size) / info.zone_size) *
              info.zone_size -
          lbas_processed - lba;
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
      return ZNS_STATUS_SPDK_ERROR;
    }
    // Synchronous write, busy wait.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      return ZNS_STATUS_SPDK_ERROR;
    }
    lbas_processed += current_step_size;
    lba_start = ((lba + lbas_processed) / info.zone_size) * info.zone_size;
  }
  return ZNS_STATUS_SUCCESS;
}

int z_reset(QPair *qpair, uint64_t slba, bool all) {
  RETURN_CODE_ON_NULL(qpair, ZNS_STATUS_NOT_ALLOCATED);
  Completion completion = {.done = false, .err = 0x00};
  int rc =
      spdk_nvme_zns_reset_zone(qpair->man->ns, qpair->qpair,
                               slba, /* starting LBA of the zone to reset */
                               all,  /* don't reset all zones */
                               __reset_zone_complete, &completion);
  if (rc != 0) {
    return ZNS_STATUS_SPDK_ERROR;
  }
  // Busy wait
  POLL_QPAIR(qpair->qpair, completion.done);
  if (completion.err != 0) {
    return ZNS_STATUS_SPDK_ERROR;
  }
  return rc;
}

int z_get_zone_head(QPair *qpair, uint64_t slba, uint64_t *write_head) {
  RETURN_CODE_ON_NULL(qpair, ZNS_STATUS_NOT_ALLOCATED);
  RETURN_CODE_ON_NULL(qpair->man, ZNS_STATUS_NOT_ALLOCATED);
  int rc = ZNS_STATUS_SUCCESS;

  // Get information from a zone.
  size_t report_bufsize = spdk_nvme_ns_get_max_io_xfer_size(qpair->man->ns);
  uint8_t *report_buf = (uint8_t *)calloc(1, report_bufsize);
  {
    Completion completion = {.done = false, .err = 0};
    rc = spdk_nvme_zns_report_zones(
        qpair->man->ns, qpair->qpair, report_buf, report_bufsize, slba,
        SPDK_NVME_ZRA_LIST_ALL, true, __get_zone_head_complete, &completion);
    if (rc != 0) {
      free(report_buf);
      return ZNS_STATUS_SPDK_ERROR;
    }
    // Busy wait for the head.
    POLL_QPAIR(qpair->qpair, completion.done);
    if (completion.err != 0) {
      free(report_buf);
      return ZNS_STATUS_SPDK_ERROR;
    }
  }
  // Retrieve write head from zone information.
  uint32_t zd_index = sizeof(struct spdk_nvme_zns_zone_report);
  struct spdk_nvme_zns_zone_desc *desc =
      (struct spdk_nvme_zns_zone_desc *)(report_buf + zd_index);
  *write_head = desc->wp;
  free(report_buf);
  if (*write_head < slba) {
    return ZNS_STATUS_SPDK_ERROR;
  }
  return ZNS_STATUS_SUCCESS;
}

void z_print_zns_status(int status) {
  switch (status) {
  case ZNS_STATUS_SUCCESS:
    printf("succes\n");
    break;
  case ZNS_STATUS_NOT_ALLOCATED:
    printf("structure not allocated\n");
    break;
  case ZNS_STATUS_SPDK_ERROR:
    printf("spdk error\n");
    break;
  default:
    printf("unknown status\n");
    break;
  }
}
}
} // namespace SimpleZNSDeviceNamespace
