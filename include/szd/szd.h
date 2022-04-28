#ifndef SZD_H
#define SZD_H

#include "szd/szd_errno.h"
#include "szd/szd_namespace.h"

#include "spdk/endian.h"
#include "spdk/env.h"
#include "spdk/log.h"
#include "spdk/nvme.h"
#include "spdk/nvme_intel.h"
#include "spdk/nvme_ocssd.h"
#include "spdk/nvme_zns.h"
#include "spdk/nvmf_spec.h"
#include "spdk/pci_ids.h"
#include "spdk/stdinc.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "spdk/uuid.h"
#include "spdk/vmd.h"

#ifdef __cplusplus
namespace SimpleZNSDeviceNamespace {
extern "C" {
#endif

#define MAX_TRADDR_LENGTH 0x100
#define MAX_DEVICE_COUNT 0x100
/**
 * @brief Options to pass to the ZNS device on initialisation.
 */
typedef struct {
  const char *name =
      "znsdevice"; /**< Name used by SPDK to identify application. */
  const bool setup_spdk = true; /**< Set to false during reset. */
} DeviceOptions;

/**
 * @brief Options to pick when opening a device.
 */
typedef struct {
  const uint64_t min_zone = 0; /**< Minimum zone that is available to SZD. */
  const uint64_t max_zone = 0; /**< Maximum zone that is available to SZD. 0
                                  will default to maxzone. */
} DeviceOpenOptions;

/**
 * @brief Holds general information about a ZNS device.
 */
typedef struct {
  uint64_t
      lba_size; /**< Size of one block, also known as logical block address.*/
  uint64_t zone_size; /**<  Size of one zone in lbas.*/
  uint64_t mdts;      /**<  Maximum data transfer size in bytes.*/
  uint64_t zasl;      /**<  Maximum size of one append command in bytes.*/
  uint64_t min_lba;   /**< Minumum lba that is allowed to be written to.*/
  uint64_t max_lba;   /**< Maximum lba that is allowed to be written to.*/
  uint64_t lba_cap;   /**<  Amount of lbas available on the device.*/
  const char *name;   /**< Name used by SPDK to identify device.*/
} DeviceInfo;

/**
 * @brief Do not touch, is to be used by Device Manager only
 */
typedef struct {
  uint64_t zone_min_;
  uint64_t zone_max_;
} DeviceManagerInternal;

/**
 * @brief General structure that aids in managing one ZNS namespace.
 * The core structure in SimpleZnsDevice.
 */
typedef struct {
  struct spdk_nvme_transport_id g_trid =
      {}; /**< transport id used to communicate with SSD*/
  struct spdk_nvme_ctrlr *ctrlr; /**< Controller of the selected SSD*/
  spdk_nvme_ns *ns;              /**< Selected namespace of the selected SSD*/
  DeviceInfo info = {};          /**< Information of selected SSD*/
  void *private_;                /**< To be used by SZD only */
} DeviceManager;

/**
 * @brief Thread unsafe I/O channel.
 * Can be used for writing and reading of data.
 */
typedef struct {
  spdk_nvme_qpair *qpair; /**< internal I/O channel */
  DeviceManager *man;     /**< Manager of the channel*/
} QPair;

/**
 * @brief Used for synchronous I/O calls to communicate (QPairs and their
 * callbacks).
 */
typedef struct {
  bool done = false; /**< Synchronous call is done.*/
  int err = 0;       /**< return code after call is done.*/
} Completion;

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
  const size_t
      traddr_len; /**< Length in bytes to check for the target id (long ids).*/
  bool found;     /**< Whether the device is found or not.*/
} DeviceTarget;

/**
 * @brief inits SPDK and the general device manager, always call ONCE before
 * ANY other function is called.
 * @param manager pointer to manager that will be initialised.
 */
int z_init(DeviceManager **manager, DeviceOptions *options);

/**
 * @brief Closes the device if open and destroys the manager.
 * @param manager non-null manager to destroy.
 */
int z_destroy(DeviceManager *manager);

/**
 * @brief Only works when device is not NULL, it recreates the device context.
 * @param manager akready existing manager to recreate.
 */
int z_reinit(DeviceManager **manager);

/**
 * @brief probes all devices that can be attached by SPDK and set probing
 * information for them.
 * @param probe sets to be a list of information on all attachable devices.
 */
int z_probe(DeviceManager *manager, ProbeInformation **probe_info);

/**
 * @brief Opens a ZNS device, provided it exists and is a ZNS device.
 * This device is then set as the current device in the manager.
 */
int z_open(DeviceManager *manager, const char *traddr,
           DeviceOpenOptions *options);

/**
 * @brief  If the manager holds a device, shut it down and free associated
 * data.
 */
int z_close(DeviceManager *man);

/**
 * @brief If a device is attached to manager, gets its information and store
 * it in info.
 */
int z_get_device_info(DeviceInfo *info, DeviceManager *manager);

/**
 * @brief Creates a Qpair to be used for I/O oprations
 * @param qpair, pointer to unallocated qpair pointer to be created.
 */
int z_create_qpair(DeviceManager *man, QPair **qpair);

/**
 * @brief Destroys the qpair if it is still valid.
 */
int z_destroy_qpair(QPair *qpair);

/**
 * @brief Custom calloc that uses DMA logic necessary for SPDK.
 * Must be alligned with the device lba_size (see DeviceInfo).
 */
void *z_calloc(QPair *qpair, size_t __nmemb, size_t __size);

/**
 * @brief Custom free that can free memory from z_calloc.
 */
void z_free(QPair *qpair, void *buffer);

/**
 * @brief Reads n bytes synchronously from the ZNS device.
 * @param qpair channel to use for I/O
 * @param lba logical block address to read from (can read in non-written
 * areas)
 * @param buffer zcalloced buffer to store the read data in.
 * @param size Amount of data to read in bytes (lba_size alligned)
 */
int z_read(QPair *qpair, uint64_t lba, void *buffer, uint64_t size);

/**
 * @brief Append z_calloced data synchronously to a zone.
 * @param qpair channel to use for I/O
 * @param lba logical block address to write to (UNVERIFIED, but must equal
 * write_head of zone), will be updated after each succesful write.
 * @param buffer zcalloced data
 * @param size size of buffer
 */
int z_append(QPair *qpair, uint64_t *lba, void *buffer, uint64_t size);

/**
 * @brief Resets a zone synchronously, allowing it to be reused.
 * @param qpair channel to use for I/O
 * @param slba starting logical block address of zone to reset
 */
int z_reset(QPair *qpair, uint64_t slba);

/**
 * @brief Resets all zones within min and max lba.
 */
int z_reset_all(QPair *qpair);

/**
 * @brief Gets the write head of a zone synchronously as a logical block
 * address (lba).
 * @param qpair channel to use for I/O
 * @param slba starting logical block address of zone to get the write head
 * from.
 * @param write_head pointer to store the write head in.
 */
int z_get_zone_head(QPair *qpair, uint64_t slba, uint64_t *write_head);

void z_print_zns_status(int status);

bool __z_probe_probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                        struct spdk_nvme_ctrlr_opts *opts);

void __z_probe_attach_cb(void *cb_ctx,
                         const struct spdk_nvme_transport_id *trid,
                         struct spdk_nvme_ctrlr *ctrlr,
                         const struct spdk_nvme_ctrlr_opts *opts);

int __z_open_create_private(DeviceManager *manager, DeviceOpenOptions *options);

bool __z_open_probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                       struct spdk_nvme_ctrlr_opts *opts);

void __z_open_attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                        struct spdk_nvme_ctrlr *ctrlr,
                        const struct spdk_nvme_ctrlr_opts *opts);

void __z_open_remove_cb(void *cb_ctx, struct spdk_nvme_ctrlr *ctrlr);

void *__reserve_dma(uint64_t size);

void __operation_complete(void *arg, const struct spdk_nvme_cpl *completion);

void __read_complete(void *arg, const struct spdk_nvme_cpl *completion);

void __append_complete(void *arg, const struct spdk_nvme_cpl *completion);

void __reset_zone_complete(void *arg, const struct spdk_nvme_cpl *completion);

void __get_zone_head_complete(void *arg,
                              const struct spdk_nvme_cpl *completion);

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
#endif
