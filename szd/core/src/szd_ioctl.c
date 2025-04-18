#include "szd/szd_ioctl.h"

#ifdef __cplusplus
extern "C" {
#endif
#ifdef __cplusplus
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

// #include <errno.h>
#include <nvme/ioctl.h>
// #include <signal.h>
// #include <stdio.h>
#include <stdlib.h>
// #include <string.h>
// #include <sys/mman.h>
// #include <sys/syscall.h>
// #include <sys/wait.h>
// #include <unistd.h>

#define NVME_ZNS_SEND_SELECT_ALL (1 << 8)
#define MAX_TRANSFER_SIZE (1 << 16)

int __ioctl_mgmt_command(int fd, int nsid, unsigned char opcode,
                         unsigned int cmd, uint64_t zslba, void *data,
                         unsigned int data_len) {
  struct nvme_passthru_cmd nvme_cmd = {
      .opcode = opcode,
      .nsid = nsid,
      .addr = (__u64)(uintptr_t)data,
      .data_len = data_len,
      .cdw10 = (unsigned int)(((__u64)zslba) & 0xffffffff),
      .cdw11 = (unsigned int)(((__u64)zslba) >> 32),
      .cdw12 = (data_len >> 2) - 1,
      .cdw13 = cmd,
      .timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT,
  };
  return ioctl(fd, NVME_IOCTL_IO_CMD, &nvme_cmd);
}

int __ioctl_mgmt_send_command(int fd, int nsid, uint64_t zslba,
                              unsigned int nvme_zns_send_action) {
  return __ioctl_mgmt_command(fd, nsid, nvme_zns_cmd_mgmt_send,
                              nvme_zns_send_action, zslba, NULL, 0);
}

int ioctl_open_zone(int fd, int nsid, uint64_t zslba) {
  return __ioctl_mgmt_send_command(fd, nsid, zslba, NVME_ZNS_ZSA_OPEN);
}

int ioctl_close_zone(int fd, int nsid, uint64_t zslba) {
  return __ioctl_mgmt_send_command(fd, nsid, zslba, NVME_ZNS_ZSA_CLOSE);
}

int ioctl_reset_zone(int fd, int nsid, uint64_t zslba) {
  return __ioctl_mgmt_send_command(fd, nsid, zslba, NVME_ZNS_ZSA_RESET);
}

int ioctl_reset_all_zones(int fd, int nsid) {
  return __ioctl_mgmt_send_command(
      fd, nsid, 0, NVME_ZNS_ZSA_RESET | NVME_ZNS_SEND_SELECT_ALL);
}

int ioctl_finish_zone(int fd, int nsid, uint64_t zslba) {
  return __ioctl_mgmt_send_command(fd, nsid, zslba, NVME_ZNS_ZSA_FINISH);
}

int __ioctl_mgmt_recv_command(int fd, int nsid, uint64_t zslba,
                              unsigned int nvme_zns_recv_action, void *data,
                              uint32_t data_len) {
  return __ioctl_mgmt_command(fd, nsid, nvme_zns_cmd_mgmt_recv,
                              nvme_zns_recv_action, zslba, data, data_len);
}

int ioctl_get_zone_heads(int fd, int nsid, uint64_t zone_cnt,
                         uint64_t zone_size, uint64_t zslba, uint64_t zeslba,
                         uint64_t *zone_heads) {
  int ret = 0;
  int nr_zones = zone_cnt;
  uint64_t reported_zones = 0;
  uint32_t data_len = sizeof(struct nvme_zone_report) +
                      (nr_zones * sizeof(struct nvme_zns_desc));
  struct nvme_zone_report *data = (struct nvme_zone_report *)malloc(data_len);
  ret = __ioctl_mgmt_recv_command(fd, nsid, zslba, NVME_ZNS_ZRA_REPORT_ZONES,
                                  (void *)data, data_len);
  if (ret < 0) {
    free(data);
    return ret;
  }
  uint64_t zones_to_read = (zeslba - zslba) / zone_size;
  zones_to_read = (zslba / zone_size) + zones_to_read > zone_cnt
                      ? zone_cnt - (zslba / zone_size)
                      : zones_to_read;
  for (uint64_t j = 0; j <= zones_to_read; j++) {
    struct nvme_zns_desc *desc = (struct nvme_zns_desc *)&(data->entries[j]);
    if (desc->wp > desc->zslba + desc->zcap) {
      zone_heads[reported_zones++] = desc->zslba + zone_size;
    } else {
      zone_heads[reported_zones++] = desc->wp;
    }
  }
  free(data);
  return ret;
}

int ioctl_get_zone_head(int fd, int nsid, uint64_t zone_cnt, uint64_t zone_size,
                        uint64_t zslba, uint64_t *zone_head) {
  return ioctl_get_zone_heads(fd, nsid, zone_cnt, zone_size, zslba, zslba,
                              zone_head);
}

int ioctl_get_zone_cap(int fd, int nsid, uint64_t zone_cnt, uint64_t zslba,
                       uint64_t *zone_cap) {
  int ret = 0;
  int nr_zones = zone_cnt;
  uint32_t data_len = sizeof(struct nvme_zone_report) +
                      (nr_zones * sizeof(struct nvme_zns_desc));
  struct nvme_zone_report *data = (struct nvme_zone_report *)malloc(data_len);
  ret = __ioctl_mgmt_recv_command(fd, nsid, zslba, NVME_ZNS_ZRA_REPORT_ZONES,
                                  (void *)data, data_len);
  if (ret < 0) {
    free(data);
    return ret;
  }
  struct nvme_zns_desc *desc = (struct nvme_zns_desc *)&(data->entries[0]);
  *zone_cap = desc->zcap;
  free(data);
  return ret;
}

int ioctl_get_nsid(int fd, uint32_t *nsid) {
  int32_t ns = ioctl(fd, NVME_IOCTL_ID);
  if (ns > 0) {
    *nsid = (uint32_t)ns;
    return 0;
  }
  return -1;
}

int __ioctl_admin_identify(int fd, int nsid, void *data, unsigned int cns,
                           unsigned int csi) {
  struct nvme_passthru_cmd cmd = {
      .opcode = nvme_admin_identify,
      .nsid = nsid,
      .addr = (__u64)(uintptr_t)data,
      .data_len = NVME_IDENTIFY_DATA_SIZE,
      .cdw10 = cns,
      .cdw11 = csi << 24,
      .timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT,
  };
  return ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd);
}

int ioctl_admin_identify_ns_nvme(int fd, int nsid, void *data) {
  return __ioctl_admin_identify(fd, nsid, data, NVME_IDENTIFY_CNS_NS,
                                NVME_CSI_NVM);
}

int ioctl_admin_identify_ns_zns(int fd, int nsid, void *data) {
  return __ioctl_admin_identify(fd, nsid, data, NVME_IDENTIFY_CNS_CSI_NS,
                                NVME_CSI_ZNS);
}

int ioctl_admin_identify_ctrl_nvme(int fd, int nsid, void *data) {
  return __ioctl_admin_identify(fd, nsid, data, NVME_IDENTIFY_CNS_CTRL,
                                NVME_CSI_NVM);
}

int ioctl_admin_identify_ctrl_zns(int fd, int nsid, void *data) {
  return __ioctl_admin_identify(fd, nsid, data, NVME_IDENTIFY_CNS_CSI_CTRL,
                                NVME_CSI_ZNS);
}

int nvme_registers_get_cap(int fd, uint64_t *cap) {
  // TODO: Implement
  *cap = 0;
  (void)fd;
  return 0;
}

static uint64_t __nvme_get_lba_cap(struct nvme_id_ns *nin) { return nin->nsze; }

static uint64_t __nvme_get_lba_size(struct nvme_id_ns *nin) {
  return 1 << nin->lbaf[(nin->flbas & 0xf)].ds;
}

static uint64_t __nvme_get_minpage_size(uint64_t cap) {
  uint64_t min_page_size = 1ULL << (12 + NVME_CAP_MPSMIN(cap));
  return min_page_size;
}

static uint64_t __nvme_get_mdts(uint64_t min_page_size,
                                struct nvme_id_ctrl *nic) {
  uint64_t mdts = nic->mdts;
  if (mdts > 0) {
    mdts = min_page_size * (1 << mdts);
  }
  if (mdts == 0 || mdts > MAX_TRANSFER_SIZE) {
    mdts = MAX_TRANSFER_SIZE;
  }
  return mdts;
}

static uint64_t __nvme_get_zasl(uint64_t min_page_size,
                                struct nvme_zns_id_ctrl *zic, uint64_t mdts) {

  uint64_t zasl = zic->zasl;
  if (zasl == 0) {
    zasl = mdts;
  } else {
    zasl = min_page_size * (1 << zasl);
  }
  if (zasl > MAX_TRANSFER_SIZE) {
    zasl = MAX_TRANSFER_SIZE;
  }
  return zasl;
}

static uint64_t __nvme_get_sze(struct nvme_id_ns *nin,
                               struct nvme_zns_id_ns *zin) {
  return zin->lbafe[nin->flbas].zsze;
}

int ioctl_get_nvme_info(int fd, DeviceInfo *info) {
  int ret;
  struct nvme_id_ns nin;
  struct nvme_zns_id_ns zin;
  struct nvme_id_ctrl nic;
  struct nvme_zns_id_ctrl zic;
  uint64_t cap;

  // Determine nsid
  if ((ret = ioctl_get_nsid(fd, &info->nsid)) < 0) {
    return ret;
  }

  // Get NVMe data
  ret = ioctl_admin_identify_ns_nvme(fd, info->nsid, &nin) ||
        ioctl_admin_identify_ns_zns(fd, info->nsid, &zin) ||
        ioctl_admin_identify_ctrl_nvme(fd, info->nsid, &nic) ||
        ioctl_admin_identify_ctrl_zns(fd, info->nsid, &zic) ||
        nvme_registers_get_cap(fd, &cap);
  if (ret != 0) {
    return ret;
  }

  info->lba_cap = __nvme_get_lba_cap(&nin);
  info->lba_size = __nvme_get_lba_size(&nin);
  info->min_page_size = __nvme_get_minpage_size(cap);
  info->mdts = __nvme_get_mdts(info->min_page_size, &nic);
  info->zasl = __nvme_get_zasl(info->min_page_size, &zic, info->mdts);
  info->zone_size = __nvme_get_sze(&nin, &zin);

  // Hack zone cap for now
  ret = ioctl_get_zone_cap(fd, info->nsid, info->lba_cap / info->zone_size,
                           0 /*slba*/, &info->zone_cap);
  return ret;
}

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
