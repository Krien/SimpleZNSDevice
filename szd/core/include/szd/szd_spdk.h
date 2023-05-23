#pragma once
#ifndef SZD_SPDK_H
#define SZD_SPDK_H

#include "szd.h"

#ifdef __cplusplus
extern "C" {
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

// "Forward declare" SPDK structs to prevent pollution.
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

#define MAX_TRADDR_LENGTH 0x100
#define MAX_DEVICE_COUNT 0x100

typedef struct {
  t_spdk_nvme_transport_id
      *g_trid;              /**< transport id used to communicate with SSD*/
  t_spdk_nvme_ctrlr *ctrlr; /**< Controller of the selected SSD*/
  t_spdk_nvme_ns *ns;       /**< Selected namespace of the selected SSD*/
} SPDKManager;

/**
 * @brief General structure that aids in managing one ZNS namespace.
 * The core structure in SimpleZnsDevice.
 */
typedef struct {
  t_spdk_nvme_transport_id
      *g_trid;              /**< transport id used to communicate with SSD*/
  t_spdk_nvme_ctrlr *ctrlr; /**< Controller of the selected SSD*/
  t_spdk_nvme_ns *ns;       /**< Selected namespace of the selected SSD*/
} DeviceManagerSpdk;

/**
 * @brief Thread unsafe I/O channel.
 * Can be used for writing and reading of data.
 */
typedef struct {
  t_spdk_nvme_qpair *qpair; /**< internal I/O channel */
  DeviceManager *man;       /**< Manager of the channel*/
} SPDKQPair;

/**
 * @brief Structure used for identifying devices.
 */
typedef struct {
  char **traddr; /**< transport ids of all probed devices.*/
  bool *zns;     /**< Foreach probed device, is it a ZNS device?*/
  struct spdk_nvme_ctrlr **ctrlr; /**< The controller(s) of the devices.*/
  uint8_t devices;                /**< Used to identify global device count.*/
  pthread_mutex_t *mut; /**< Ensures that probe information is thread safe.*/
} ProbeInformation;

/**
 * @brief Structure that is used when looking for a device by trid.
 */
typedef struct {
  DeviceManager *manager; /**< The manager associated with the probing.*/
  const char *traddr; /**< The transport id of the device that is targeted.*/
  const size_t traddr_len; /**< Length in bytes to check for the target id
                              (long ids).*/
  bool found;              /**< Whether the device is found or not.*/
} DeviceTarget;

int szd_spdk_register_backend(EngineManager *em);

int szd_spdk_init(DeviceManager **manager, DeviceOptions *options);
int szd_spdk_destroy(DeviceManager *manager);
int szd_spdk_reinit(DeviceManager **manager);
int szd_spdk_probe(DeviceManager *manager, void **probe_info);
void szd_spdk_free_probe_information(DeviceManager *manager, void *probe_info);
int szd_spdk_open(DeviceManager *manager, const char *traddr,
                  DeviceOpenOptions *options);
int szd_spdk_close(DeviceManager *man);
int szd_spdk_get_device_info(DeviceInfo *info, DeviceManager *manager);

int szd_spdk_create_qpair(DeviceManager *man, QPair **qpair);
int szd_spdk_destroy_qpair(DeviceManager *manager, QPair *qpair);
void *szd_spdk_calloc(uint64_t __allign, size_t __nmemb, size_t __size);
void szd_spdk_free(void *buffer);

int szd_spdk_read(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                  uint64_t blocks);
int szd_spdk_write(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                   uint64_t blocks);

int szd_spdk_append(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                    uint64_t blocks);
int szd_spdk_append_async(QPair *qpair, uint64_t lba, void *buffer,
                          uint64_t size, uint64_t blocks,
                          Completion *completion);

int szd_spdk_poll_async(QPair *qpair, Completion *completion);
int szd_spdk_poll_once(QPair *qpair, Completion *completion);
void szd_spdk_poll_once_raw(QPair *qpair);

int szd_spdk_reset(QPair *qpair, uint64_t slba);
int szd_spdk_reset_all(QPair *qpair);
int szd_spdk_finish_zone(QPair *qpair, uint64_t slba);
int szd_spdk_get_zone_head(QPair *qpair, uint64_t slba, uint64_t *write_head);
int szd_spdk_get_zone_heads(QPair *qpair, uint64_t slba, uint64_t eslba,
                            uint64_t *write_head);
int szd_spdk_get_zone_cap(QPair *qpair, uint64_t slba, uint64_t *zone_cap);
long int szd_spdk_strtol(const char *nptr, int base);

void __szd_spdk_error_log(const char *file, const int line, const char *func,
                          const char *format, ...);
bool __szd_spdk_probe_probe_cb(void *cb_ctx,
                               const t_spdk_nvme_transport_id *trid,
                               t_spdk_nvme_ctrlr_opts *opts);
void __szd_spdk_probe_attach_cb(void *cb_ctx,
                                const t_spdk_nvme_transport_id *trid,
                                struct spdk_nvme_ctrlr *ctrlr,
                                const struct spdk_nvme_ctrlr_opts *opts);
int __szd_spdk_open_create_private(DeviceManager *manager,
                                   DeviceOpenOptions *options);
bool __szd_spdk_open_probe_cb(void *cb_ctx,
                              const t_spdk_nvme_transport_id *trid,
                              t_spdk_nvme_ctrlr_opts *opts);
void __szd_spdk_open_attach_cb(void *cb_ctx,
                               const t_spdk_nvme_transport_id *trid,
                               t_spdk_nvme_ctrlr *ctrlr,
                               const t_spdk_nvme_ctrlr_opts *opts);
void __szd_spdk_open_remove_cb(void *cb_ctx, t_spdk_nvme_ctrlr *ctrlr);
void *__reserve_dma(uint64_t size);
void __spdk_operation_complete(void *arg, const t_spdk_nvme_cpl *completion);
void __spdk_read_complete(void *arg, const t_spdk_nvme_cpl *completion);
void __spdk_append_complete(void *arg, const t_spdk_nvme_cpl *completion);
void __spdk_reset_zone_complete(void *arg, const t_spdk_nvme_cpl *completion);
void __spdk_finish_zone_complete(void *arg, const t_spdk_nvme_cpl *completion);
void __spdk_get_zone_head_complete(void *arg,
                                   const t_spdk_nvme_cpl *completion);

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
#endif