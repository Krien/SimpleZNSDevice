#pragma once
#ifndef SZD_IOURING_H
#define SZD_IOURING_H

#include "szd.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

int szd_io_uring_register_backend(EngineManager *dm);

int szd_io_uring_init(DeviceManager **manager, DeviceOptions *options);
int szd_io_uring_destroy(DeviceManager *manager);
int szd_io_uring_reinit(DeviceManager **manager);
int szd_io_uring_probe(DeviceManager *dm, void **probe_info);
void szd_io_uring_free_probe(DeviceManager *dm, void *probe_info);

int szd_io_uring_open(DeviceManager *manager, const char *traddr,
                      DeviceOpenOptions *options);
int szd_io_uring_close(DeviceManager *man);
int szd_io_uring_get_device_info(DeviceInfo *info, DeviceManager *manager);

int szd_io_uring_create_qpair(DeviceManager *man, QPair **qpair);
int szd_io_uring_destroy_qpair(DeviceManager *man, QPair *qpair);

void *szd_io_uring_calloc(uint64_t __allign, size_t __nmemb, size_t __size);
void szd_io_uring_free(void *buffer);

int szd_io_uring_read(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                      uint64_t blocks);
int szd_io_uring_write(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                       uint64_t blocks);
int szd_io_uring_append(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                        uint64_t blocks);
int szd_io_uring_append_async(QPair *qpair, uint64_t lba, void *buffer,
                              uint64_t size, uint64_t blocks,
                              Completion *completion);

int szd_io_uring_poll_async(QPair *qpair, Completion *completion);
int szd_io_uring_poll_once(QPair *qpair, Completion *completion);
void szd_io_uring_poll_once_raw(QPair *qpair);

int szd_io_uring_reset(QPair *qpair, uint64_t slba);
int szd_io_uring_reset_all(QPair *qpair);
int szd_io_uring_finish_zone(QPair *qpair, uint64_t slba);
int szd_io_uring_get_zone_head(QPair *qpair, uint64_t slba,
                               uint64_t *write_head);
int szd_io_uring_get_zone_heads(QPair *qpair, uint64_t slba, uint64_t eslba,
                                uint64_t *write_head);
int szd_io_uring_get_zone_cap(QPair *qpair, uint64_t slba, uint64_t *zone_cap);

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
#endif