#pragma once
#ifndef SZD_IOCTL_H
#define SZD_IOCTL_H

#include "szd.h"

#ifdef __cplusplus
extern "C" {
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

int ioctl_open_zone(int fd, int nsid, uint64_t zslba);
int ioctl_close_zone(int fd, int nsid, uint64_t zslba);
int ioctl_reset_zone(int fd, int nsid, uint64_t zslba);
int ioctl_reset_all_zones(int fd, int nsid);
int ioctl_finish_zone(int fd, int nsid, uint64_t zslba);
int ioctl_get_zone_heads(int fd, int nsid, uint64_t zone_cnt,
                         uint64_t zone_size, uint64_t zslba, uint64_t zeslba,
                         uint64_t *zone_heads);
int ioctl_get_zone_head(int fd, int nsid, uint64_t zone_cnt, uint64_t zone_size,
                        uint64_t zslba, uint64_t *zone_head);
int ioctl_get_zone_cap(int fd, int nsid, uint64_t zone_cnt, uint64_t zslba,
                       uint64_t *zone_cap);

int ioctl_get_nsid(int fd, uint32_t *nsid);
int ioctl_admin_identify_ns_nvme(int fd, int nsid, void *data);
int ioctl_admin_identify_ns_zns(int fd, int nsid, void *data);
int ioctl_admin_identify_ctrl_nvme(int fd, int nsid, void *data);
int ioctl_admin_identify_ctrl_zns(int fd, int nsid, void *data);
int nvme_registers_get_cap(int fd, uint64_t *cap);
int ioctl_get_nvme_info(int fd, DeviceInfo *info);

int __ioctl_mgmt_command(int fd, int nsid, unsigned char opcode,
                         unsigned int cmd, uint64_t zslba, void *data,
                         unsigned int data_len);
int __ioctl_mgmt_send_command(int fd, int nsid, uint64_t zslba,
                              unsigned int nvme_zns_send_action);
int __ioctl_admin_identify(int fd, int nsid, void *data, unsigned int cns,
                           unsigned int csi);
#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
#endif
