/** \file
 * Main SZD interface.
 */
#pragma once
#ifndef SZD_H
#define SZD_H

#include "szd/szd_namespace.h"
#include "szd/szd_status_code.h"
#include <pthread.h>
#ifdef SZD_USDT
#include <sys/sdt.h>
#endif
#ifdef __cplusplus
extern "C" {
#include <cstdint>
#include <cstdio>
#else
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#endif
#ifdef __cplusplus
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

/**
 * @brief Options to pass to the ZNS device on initialisation.
 */
typedef struct {
  const char *name;      /**< Name used by SPDK to identify application. */
  const bool setup_spdk; /**< Set to false during reset. */
  const bool iouring_sqthread; /**< Set to false during reset. */
  const bool iouring_fixed;    /**< Set to false during reset. */
} DeviceOptions;
extern const DeviceOptions DeviceOptions_default;

/**
 * @brief Options to pick when opening a device.
 */
typedef struct {
  const uint64_t min_zone; /**< Minimum zone that is available to SZD. */
  const uint64_t max_zone; /**< Maximum zone that is available to SZD. 0
                                  will default to maxzone. */
} DeviceOpenOptions;
extern const DeviceOpenOptions DeviceOpenOptions_default;

/**
 * @brief Holds general information about a ZNS device.
 */
typedef struct {
  uint64_t
      lba_size; /**< Size of one block, also known as logical block address.*/
  uint64_t zone_size;     /**<  Size of one zone in lbas.*/
  uint64_t zone_cap;      /**< Size of user availabe space in one zone. */
  uint64_t min_page_size; /** minimum cap page size */
  uint64_t mdts;          /**<  Maximum data transfer size in bytes.*/
  uint64_t zasl;          /**<  Maximum size of one append command in bytes.*/
  uint64_t lba_cap;       /**<  Amount of lbas available on the device.*/
  uint64_t min_lba;       /**< Minimum lba that is allowed to be written to.*/
  uint64_t max_lba;       /**< Maximum lba that is allowed to be written to.*/
  const char *name;       /**< Name used by SZD to identify device.*/
  uint32_t nsid;          /**< nsid*/
} DeviceInfo;
extern const DeviceInfo DeviceInfo_default;

/**
 * @brief Do not touch, is to be used by Device Manager only
 */
typedef struct {
  uint64_t zone_min_;
  uint64_t zone_max_;
} DeviceManagerInternal;
extern const DeviceManagerInternal DeviceManagerInternal_default;

/**
 * @brief General structure that aids in managing one ZNS namespace.
 * The core structure in SimpleZnsDevice.
 */
typedef struct {
  DeviceInfo info; /**< Information of selected SSD*/
  void *private_;  /**< To be used by backend only */
} DeviceManager;

/**
 * @brief Thread unsafe I/O channel.
 * Can be used for writing and reading of data.
 */
typedef struct {
  void *qpair;        /**< internal I/O channel */
  DeviceManager *man; /**< Manager of the channel*/
} QPair;

/**
 * @brief Used for synchronous I/O calls to communicate (QPairs and their
 * callbacks).
 */
typedef struct {
  bool done;    /**< Synchronous call is done.*/
  uint16_t err; /**< return code after call is done.*/
  uint32_t id; /**<Used for debugging*/
} Completion;
extern const Completion Completion_default;

/**
 * @brief used for adding backends
 */
typedef struct {
  /* Status */
  int (*init)(DeviceManager **dm, DeviceOptions *opt);
  int (*destroy)(DeviceManager *dm);
  int (*reinit)(DeviceManager **dm);
  int (*probe)(DeviceManager *dm, void **probe_info);
  void (*free_probe)(DeviceManager *dm, void *probe_info);
  int (*open)(DeviceManager *manager, const char *traddr,
              DeviceOpenOptions *options);
  int (*close)(DeviceManager *manager);
  int (*get_device_info)(DeviceInfo *info, DeviceManager *dm);
  /* Handling */
  int (*create_qpair)(DeviceManager *dm, QPair **qpair);
  int (*destroy_qpair)(DeviceManager *dm, QPair *qpair);
  void *(*buf_calloc)(uint64_t __allign, size_t __nmemb, size_t __size);
  void (*free)(void *buf);
  /* I/O */
  int (*read)(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
              uint64_t blocks);
  int (*write)(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
               uint64_t blocks);

  int (*append)(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                uint64_t blocks);
  int (*append_async)(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                      uint64_t blocks, Completion *completion);
  int (*poll_async)(QPair *qpair, Completion *completion);
  int (*poll_once)(QPair *qpair, Completion *completion);
  void (*poll_once_raw)(QPair *qpair);
  /* zone management */
  int (*reset_zone)(QPair *qpair, uint64_t slba);
  int (*reset_all_zones)(QPair *qpair);
  int (*finish_zone)(QPair *qpair, uint64_t slba);
  int (*get_zone_head)(QPair *qpair, uint64_t slba, uint64_t *write_head);
  int (*get_zone_heads)(QPair *qpair, uint64_t slba, uint64_t eslba,
                        uint64_t *write_head);
  int (*get_zone_cap)(QPair *qpair, uint64_t slba, uint64_t *zone_cap);
} ioengine_backend;
enum szd_io_backend { SZD_IO_BACKEND_SPDK = 0, SZD_IO_BACKEND_IO_URING = 2 };

/**
 * @brief General structure that aids in managing one ZNS namespace.
 * The core structure in SimpleZnsDevice.
 */
typedef struct {
  ioengine_backend backend;
  DeviceManager *manager_;          /**< To be used by SZD only */
  DeviceManagerInternal *internal_; /**< To be used by SZD only */
} EngineManager;

/**
 * @brief inits SPDK and the general device manager, always call ONCE before
 * ANY other function is called.
 * @param manager pointer to manager that will be initialised.
 */
int szd_init(EngineManager **manager, DeviceOptions *options,
             enum szd_io_backend backend);

/**
 * @brief Closes the device if open and destroys the manager.
 * @param manager non-null manager to destroy.
 */
int szd_destroy(EngineManager *manager);

/**
 * @brief Only works when device is not NULL, it recreates the device context.
 * @param manager akready existing manager to recreate.
 */
int szd_reinit(EngineManager **manager);

/**
 * @brief probes all devices that can be attached by SPDK and set probing
 * information for them.
 * @param probe sets to be a list of information on all attachable devices.
 */
int szd_probe(EngineManager *manager, void **probe_info);

/**
 * @brief Frees information from probe information.
 */
void szd_free_probe_information(EngineManager *manager, void *probe_info);

/**
 * @brief Opens a ZNS device, provided it exists and is a ZNS device.
 * This device is then set as the current device in the manager.
 */
int szd_open(EngineManager *manager, const char *traddr,
             DeviceOpenOptions *options);

/**
 * @brief  If the manager holds a device, shut it down and free associated
 * data.
 */
int szd_close(EngineManager *man);

/**
 * @brief If a device is attached to manager, gets its information and store
 * it in info.
 */
int szd_get_device_info(EngineManager *manager, DeviceInfo *info);

/**
 * @brief Creates a Qpair to be used for I/O oprations
 * @param qpair, pointer to unallocated qpair pointer to be created.
 */
int szd_create_qpair(EngineManager *man, QPair **qpair);

/**
 * @brief Destroys the qpair if it is still valid.
 */
int szd_destroy_qpair(EngineManager *man, QPair *qpair);

/**
 * @brief Custom calloc that uses DMA logic necessary for SPDK.
 * Must be alligned with the device lba_size (see DeviceInfo).
 */
void *szd_calloc(EngineManager *dm, uint64_t __allign, size_t __nmemb,
                 size_t __size);

/**
 * @brief Custom free that can free memory from z_calloc.
 */
void szd_free(EngineManager *dm, void *buffer);

/**
 * @brief Reads n bytes synchronously from the ZNS device.
 * @param qpair channel to use for I/O
 * @param lba logical block address to read from (can read in non-written
 * areas)
 * @param buffer zcalloced buffer to store the read data in.
 * @param size Amount of data to read in bytes (lba_size alligned)
 * @param nr_reads ptr to variable that can be used for diagnostics, can be
 * set to NULL.
 */
int szd_read(EngineManager *manager, QPair *qpair, uint64_t lba, void *buffer,
             uint64_t size);
int szd_read_with_diag(EngineManager *manager, QPair *qpair, uint64_t lba,
                       void *buffer, uint64_t size, uint64_t *nr_reads);

int szd_write(EngineManager *manager, QPair *qpair, uint64_t *lba, void *buffer,
              uint64_t size);
int szd_write_with_diag(EngineManager *manager, QPair *qpair, uint64_t *lba,
                        void *buffer, uint64_t size, uint64_t *nr_writes);

/**
 * @brief Append z_calloced data synchronously to a zone.
 * @param qpair channel to use for I/O
 * @param lba logical block address to write to (UNVERIFIED, but must equal
 * write_head of zone), will be updated after each succesful write.
 * @param buffer zcalloced data
 * @param size size of buffer
 * @param nr_appends ptr to variable that can be used for diagnostics, can be
 * set to NULL.
 */
int szd_append(EngineManager *manager, QPair *qpair, uint64_t *lba,
               void *buffer, uint64_t size);
int szd_append_with_diag(EngineManager *manager, QPair *qpair, uint64_t *lba,
                         void *buffer, uint64_t size, uint64_t *nr_appends);

/**
 * @brief Append z_calloced data asynchronously to a zone.
 * @param qpair channel to use for I/O
 * @param lba logical block address to write to (UNVERIFIED, but must equal
 * write_head of zone), will be updated after each succesful write.
 * @param buffer zcalloced data
 * @param size size of buffer
 * @param nr_appends ptr to variable that can be used for diagnostics, can be
 * set to NULL.
 * @param completion can be used to poll for completion later on (sync)
 */
int szd_append_async(EngineManager *manager, QPair *qpair, uint64_t *lba,
                     void *buffer, uint64_t size, Completion *completion);
int szd_append_async_with_diag(EngineManager *manager, QPair *qpair,
                               uint64_t *lba, void *buffer, uint64_t size,
                               uint64_t *nr_appends, Completion *completion);
/**
 * @brief
 * Can be used on an asynchronously function to ensure that is synced to the
 * storage.
 */
int szd_poll_async(EngineManager *manager, QPair *qpair,
                   Completion *completion);
int szd_poll_once(EngineManager *manager, QPair *qpair, Completion *completion);
// Rawest poll, no error handling or finish checks
void szd_poll_once_raw(EngineManager *manager, QPair *qpair);

/**
 * @brief Resets a zone synchronously, allowing it to be reused.
 * @param qpair channel to use for I/O
 * @param slba starting logical block address of zone to reset
 */
int szd_reset(EngineManager *manager, QPair *qpair, uint64_t slba);

/**
 * @brief Resets all zones within min and max lba.
 */
int szd_reset_all(EngineManager *manager, QPair *qpair);

/**
 * @brief Finishes a zone synchronously, preventing too many active zones.
 * @param qpair channel to use for I/O
 * @param slba starting logical block address of zone to reset
 */
int szd_finish_zone(EngineManager *manager, QPair *qpair, uint64_t slba);

/**
 * @brief Gets the write head of a zone synchronously as a logical block
 * address (lba).
 * @param qpair channel to use for I/O
 * @param slba starting logical block address of zone to get the write head
 * from.
 * @param write_head pointer to store the write head in.
 */
int szd_get_zone_head(EngineManager *manager, QPair *qpair, uint64_t slba,
                      uint64_t *write_head);

/**
 * @brief Gets the write head of a zone synchronously as a logical block
 * address (lba).
 * @param qpair channel to use for I/O
 * @param slba starting logical block address of zone to get the write head
 * from.
 * @param eslba end logical block address of zone to get the write head
 * from. Must still be the beginning of the zone can be equal to slba.
 * @param write_head pointer to store the write heads in. Must be allocated
 * before and must have at least (eslba - slba + 1) / zone_size.
 */
int szd_get_zone_heads(EngineManager *manager, QPair *qpair, uint64_t slba,
                       uint64_t eslba, uint64_t *write_head);

/**
 * @brief Gets the write head of a zone synchronously as a logical block
 * address (lba).
 * @param qpair channel to use for I/O
 * @param slba starting logical block address of zone to get the write head
 * from.
 * @param zone_cap capacity of a zone.
 */
int szd_get_zone_cap(EngineManager *manager, QPair *qpair, uint64_t slba,
                     uint64_t *zone_cap);

/**
 * @brief Converts status code of SZD to human readable messages.
 * @param status If an SZD code retun appropriate message, else return default
 * message.
 */
void szd_print_zns_status(int status);

#ifdef NDEBUG
#define SZD_LOG_ERROR(...)                                                     \
  do {                                                                         \
  } while (0)
#else
#define SZD_LOG_ERROR(...)                                                     \
  __szd_error_log(__FILE__, __LINE__, __func__, __VA_ARGS__)
#endif

// Taken directly (renamed) from spdk/likely (no leakage)
#define szd_unlikely(cond) __builtin_expect((cond), 0)
#define szd_likely(cond) __builtin_expect(!!(cond), 1)

#ifdef SZD_USDT
#define SZD_DTRACE_PROBE(name) DTRACE_PROBE(szd, name)
#define SZD_DTRACE_PROBE1(name, a1) DTRACE_PROBE1(szd, name, a1)
#define SZD_DTRACE_PROBE2(name, a1, a2) DTRACE_PROBE2(szd, name, a1, a2)
#else
#define SZD_DTRACE_PROBE(...)                                                  \
  do {                                                                         \
  } while (0)
#define SZD_DTRACE_PROBE1(...)                                                 \
  do {                                                                         \
  } while (0)
#define SZD_DTRACE_PROBE2(...)                                                 \
  do {                                                                         \
  } while (0)
#endif

int __szd_register_backend(EngineManager *dm, enum szd_io_backend backend);

void __szd_error_log(const char *file, const int line, const char *func,
                     const char *format, ...);

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
#endif
