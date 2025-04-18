#include "szd/szd_iouring.h"
#include "szd/szd_ioctl.h"
#include <liburing.h>
#include <nvme/ioctl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
#endif

typedef struct {
  int fd;
  bool sqthread;
  bool fixed;
} UringDeviceManager;

int szd_io_uring_register_backend(EngineManager *em) {
  ioengine_backend backend = {
      .init = szd_io_uring_init,
      .destroy = szd_io_uring_destroy,
      .reinit = szd_io_uring_reinit,
      .probe = szd_io_uring_probe,
      .free_probe = szd_io_uring_free_probe,
      .open = szd_io_uring_open,
      .close = szd_io_uring_close,
      .get_device_info = szd_io_uring_get_device_info,
      .create_qpair = szd_io_uring_create_qpair,
      .destroy_qpair = szd_io_uring_destroy_qpair,
      .buf_calloc = szd_io_uring_calloc,
      .free = szd_io_uring_free,
      .read = szd_io_uring_read,
      .write = szd_io_uring_write,
      .append = szd_io_uring_append,
      .append_async = szd_io_uring_append_async,
      .poll_async = szd_io_uring_poll_async,
      .poll_once = szd_io_uring_poll_once,
      .poll_once_raw = szd_io_uring_poll_once_raw,
      .reset_zone = szd_io_uring_reset,
      .reset_all_zones = szd_io_uring_reset_all,
      .finish_zone = szd_io_uring_finish_zone,
      .get_zone_head = szd_io_uring_get_zone_head,
      .get_zone_heads = szd_io_uring_get_zone_heads,
      .get_zone_cap = szd_io_uring_get_zone_cap,
  };
  em->backend = backend;
  return 0;
}

int szd_io_uring_init(DeviceManager **dm, DeviceOptions *options) {
  // Dynamic stuff
  (*dm)->private_ = calloc(sizeof(UringDeviceManager), 1);
  UringDeviceManager *priv_ = (UringDeviceManager *)(*dm)->private_;
  // setup stub info, we do not want to create extra UB.
  (*dm)->info = DeviceInfo_default;
  (*dm)->info.name = options->name;
  priv_->fd = -1;
  return SZD_SC_SUCCESS;
}

static int check_nvme_device(const char *filename) {
  int ret = 0;
  struct stat stat_buffer;
  ret = stat(filename, &stat_buffer);
  if (ret != 0) {
    return -1;
  }
  if (!S_ISCHR(stat_buffer.st_mode)) {
    ret = -1;
    fprintf(stderr, "Currently only support char devices\n");
    return ret;
  }
  return ret;
}

int szd_io_uring_get_device_info(DeviceInfo *info, DeviceManager *dm) {
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  return ioctl_get_nvme_info(priv_->fd, info);
}

int szd_io_uring_open(DeviceManager *dm, const char *filename,
                      DeviceOpenOptions *options) {
  (void)options;
  int ret;
  int open_flags;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;

  // Check device
  ret = check_nvme_device(filename);
  if (ret < 0) {
    return ret;
  }

  // Open device
  open_flags = O_RDWR;
  priv_->fd = open(filename, open_flags);
  if (priv_->fd < 0) {
    ret = -1;
    return ret;
  }
  return ret;
}

int szd_io_uring_destroy(DeviceManager *dm) {
  int ret = 0;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  if (priv_->fd > 0) {
    close(priv_->fd);
    priv_->fd = -1;
  }
  free(priv_);
  dm->private_ = NULL;
  return ret;
}

int szd_io_uring_reinit(DeviceManager **dm) {
  const char *name = (*dm)->info.name;
  UringDeviceManager *priv_ = (UringDeviceManager *)(*dm)->private_;
  bool sqthread = priv_->sqthread;
  int ret = szd_io_uring_destroy(*dm);
  if (ret < 0) {
    return ret;
  }
  DeviceOptions options = {
      .name = name, .setup_spdk = false, .iouring_sqthread = sqthread};
  return szd_io_uring_init(dm, &options);
}

int szd_io_uring_probe(DeviceManager *dm, void **probe_info) {
  (void)dm;
  (void)probe_info;
  return 0;
}

void szd_io_uring_free_probe(DeviceManager *dm, void *probe_info) {
  (void)dm;
  (void)probe_info;
}

int szd_io_uring_close(DeviceManager *dm) {
  int ret = 0;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  if (priv_ != NULL && priv_->fd > 0) {
    close(priv_->fd);
    priv_->fd = -1;
    ret = 0;
  } else {
    ret = -1;
  }
  return ret;
}

int szd_io_uring_create_qpair(DeviceManager *dm, QPair **qpair) {
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  int ret;

  *qpair = (QPair *)calloc(1, sizeof(QPair));
  (*qpair)->man = dm;
  (*qpair)->qpair = calloc(1, sizeof(struct io_uring));

  struct io_uring_params p = {};
  // Setup io_uring
  // p.flags = IORING_SETUP_IOPOLL;
  p.flags |= IORING_SETUP_SQE128;
  p.flags |= IORING_SETUP_CQE32;
  // p.flags  |= IORING_SETUP_CQSIZE;
  //  p.flags  |= IORING_SETUP_COOP_TASKRUN;
  //  p.flags  |= IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN;
  if (priv_->sqthread)
    p.flags |= IORING_SETUP_SQPOLL;
  p.cq_entries = 64;
  ret = io_uring_queue_init_params(64, (*qpair)->qpair, &p);
  if (ret != 0) {
    return ret;
  }

  if (priv_->sqthread) {
    struct io_uring *ring = (struct io_uring *)(*qpair)->qpair;
    ret = io_uring_register_files(ring, &(priv_->fd), 1);
  }

  return SZD_SC_SUCCESS;
}

int szd_io_uring_destroy_qpair(DeviceManager *man, QPair *qpair) {
  int rc = 0;
  UringDeviceManager *priv_ = (UringDeviceManager *)man->private_;
  struct io_uring *ring = (struct io_uring *)qpair->qpair;
  qpair->man = NULL;
  if (priv_->sqthread) {
    rc = io_uring_unregister_files(ring);
  }
  if (priv_->fixed) {
    // We do not use fixed bufs yet
    // ret = io_uring_unregister_buffers(&priv_->ring) || ret;
  }
  io_uring_queue_exit(ring);
  free(qpair->qpair);
  free(qpair);
  return rc;
}

void *szd_io_uring_calloc(uint64_t __allign, size_t __nmemb, size_t __size) {
  return aligned_alloc(__allign, __nmemb * __size);
}

void szd_io_uring_free(void *buffer) { free(buffer); }

static int szd_io_uring_req_submit(DeviceManager *dm, struct io_uring *ring,
                                   Completion *completion, uint32_t nsid,
                                   uint8_t opcode, uint64_t slba,
                                   uint64_t blocks, void *buf, uint64_t bytes,
                                   uint64_t offset) {
  int ret = 0;

  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  struct io_uring_sqe *sqe;
  struct nvme_uring_cmd *cmd;

  // prepare write
  sqe = io_uring_get_sqe(ring);
  io_uring_prep_write(sqe, priv_->sqthread ? 0 : priv_->fd, buf, bytes, offset);

  // Alter Submission queue for passthrough
  sqe->user_data = (__u64)(uintptr_t)completion;
  sqe->opcode = IORING_OP_URING_CMD;
  sqe->cmd_op = NVME_URING_CMD_IO;
  if (priv_->sqthread)
    sqe->flags |= IOSQE_FIXED_FILE;

  // Alter Submission cmd for passthrough
  cmd = (struct nvme_uring_cmd *)sqe->cmd;
  memset(cmd, 0, sizeof(struct nvme_uring_cmd));
  cmd->opcode = opcode;
  cmd->cdw10 = ((__u64)slba) & 0xffffffff;
  cmd->cdw11 = ((__u64)slba) >> 32;
  cmd->cdw12 = (__u32)blocks - 1;
  cmd->addr = (__u64)(uintptr_t)buf;
  cmd->data_len = bytes;
  cmd->nsid = nsid;
  cmd->timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT;

  // Submit
  ret = io_uring_submit(ring);
  return ret;
}

static int szd_io_uring_req_complete(DeviceManager *dm, struct io_uring *ring) {
  (void)dm;
  int ret = 0;
  struct io_uring_cqe *cqe;
  ret = io_uring_wait_cqe(ring, &cqe);
  if (ret != 0) {
    return ret;
  }
  ((Completion *)(cqe->user_data))->done = true;
  ((Completion *)(cqe->user_data))->err = ret = cqe->res;
  io_uring_cqe_seen(ring, cqe);
  return ret;
}

static int szd_io_uring_req_sync(DeviceManager *dm, struct io_uring *ring,
                                 uint32_t nsid, uint8_t opcode, uint64_t slba,
                                 uint64_t blocks, void *buf, uint64_t bytes,
                                 uint64_t offset) {
  int ret = 0;
  Completion completion = Completion_default;
  ret = szd_io_uring_req_submit(dm, ring, &completion, nsid, opcode, slba,
                                blocks, buf, bytes, offset);
  if (ret < 0) {
    return ret;
  }
  ret = szd_io_uring_req_complete(dm, ring);
  return ret;
}

int szd_io_uring_read(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                      uint64_t blocks) {
  DeviceManager *dm = qpair->man;
  struct io_uring *ring = (struct io_uring *)(qpair->qpair);
  uint32_t nsid = dm->info.nsid;
  int ret = 0;
  ret = szd_io_uring_req_sync(dm, ring, nsid, nvme_cmd_read, lba, blocks,
                              buffer, size, 0);
  return ret;
}

int szd_io_uring_write(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                       uint64_t blocks) {
  DeviceManager *dm = qpair->man;
  struct io_uring *ring = (struct io_uring *)(qpair->qpair);
  uint32_t nsid = dm->info.nsid;
  int ret = 0;
  ret = szd_io_uring_req_sync(dm, ring, nsid, nvme_cmd_write, lba, blocks,
                              buffer, size, 0);
  return ret;
}

int szd_io_uring_append(QPair *qpair, uint64_t lba, void *buffer, uint64_t size,
                        uint64_t blocks) {
  DeviceManager *dm = qpair->man;
  struct io_uring *ring = (struct io_uring *)(qpair->qpair);
  uint32_t nsid = dm->info.nsid;
  int ret = 0;
  ret = szd_io_uring_req_sync(dm, ring, nsid, nvme_zns_cmd_append, lba, blocks,
                              buffer, size, 0);
  return ret;
}

int szd_io_uring_append_async(QPair *qpair, uint64_t lba, void *buffer,
                              uint64_t size, uint64_t blocks,
                              Completion *completion) {
  completion->done = false;
  completion->err = 0;
  DeviceManager *dm = qpair->man;
  struct io_uring *ring = (struct io_uring *)(qpair->qpair);
  uint32_t nsid = dm->info.nsid;
  printf("completion start (async) %u\n", completion->id);
  int ret =
      szd_io_uring_req_submit(dm, ring, completion, nsid, nvme_zns_cmd_append,
                              lba, blocks, buffer, size, 0);
  if (ret > 0) {
    ret = 0;
  }
  return ret;
}

int szd_io_uring_poll_async(QPair *qpair, Completion *completion) {
  (void)completion;
  int ret = 0;
  DeviceManager *dm = qpair->man;
  struct io_uring *ring = (struct io_uring *)(qpair->qpair);
  while (!completion->done) {
    ret = szd_io_uring_req_complete(dm, ring);
  }
  return ret;
}

int szd_io_uring_poll_once(QPair *qpair, Completion *completion) {
  struct io_uring *ring = (struct io_uring *)(qpair->qpair);
  int ret = 0;
  struct io_uring_cqe *cqe = NULL;
  if (!completion->done) {
    ret = io_uring_peek_cqe(ring, &cqe);
    if (ret != 0 || cqe == NULL) {
      return ret;
    }
    ((Completion *)(cqe->user_data))->done = true;
    ((Completion *)(cqe->user_data))->err = cqe->res;
    printf("completion done (once) %u\n", ((Completion *)(cqe->user_data))->id);
    io_uring_cqe_seen(ring, cqe);
  }
  return ret;
}

void szd_io_uring_poll_once_raw(QPair *qpair) {
  struct io_uring *ring = (struct io_uring *)(qpair->qpair);
  struct io_uring_cqe *cqe = NULL;
  if (io_uring_peek_cqe(ring, &cqe) == 0 && cqe != NULL) {
    ((Completion *)(cqe->user_data))->done = true;
    ((Completion *)(cqe->user_data))->err = cqe->res;
    printf("completion done (peek) %u\n", ((Completion *)(cqe->user_data))->id);
    io_uring_cqe_seen(ring, cqe);
  }
}

int szd_io_uring_reset(QPair *qpair, uint64_t slba) {
  DeviceManager *dm = qpair->man;
  uint32_t nsid = dm->info.nsid;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  int fd = priv_->fd;
  return ioctl_reset_zone(fd, nsid, slba);
}

int szd_io_uring_reset_all(QPair *qpair) {
  DeviceManager *dm = qpair->man;
  uint32_t nsid = dm->info.nsid;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  int fd = priv_->fd;
  return ioctl_reset_all_zones(fd, nsid);
}

int szd_io_uring_finish_zone(QPair *qpair, uint64_t slba) {
  DeviceManager *dm = qpair->man;
  uint32_t nsid = dm->info.nsid;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  int fd = priv_->fd;
  return ioctl_finish_zone(fd, nsid, slba);
}

int szd_io_uring_get_zone_head(QPair *qpair, uint64_t slba,
                               uint64_t *write_head) {
  DeviceManager *dm = qpair->man;
  uint32_t nsid = dm->info.nsid;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  int fd = priv_->fd;
  return ioctl_get_zone_head(fd, nsid, dm->info.lba_cap / dm->info.zone_size,
                             dm->info.zone_size, slba, write_head);
}

int szd_io_uring_get_zone_heads(QPair *qpair, uint64_t slba, uint64_t eslba,
                                uint64_t *zone_heads) {
  DeviceManager *dm = qpair->man;
  uint32_t nsid = dm->info.nsid;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  int fd = priv_->fd;
  return ioctl_get_zone_heads(fd, nsid, dm->info.lba_cap / dm->info.zone_size,
                              dm->info.zone_size, slba, eslba, zone_heads);
}

int szd_io_uring_get_zone_cap(QPair *qpair, uint64_t slba, uint64_t *zone_cap) {
  DeviceManager *dm = qpair->man;
  uint32_t nsid = dm->info.nsid;
  UringDeviceManager *priv_ = (UringDeviceManager *)dm->private_;
  int fd = priv_->fd;
  return ioctl_get_zone_cap(fd, nsid, dm->info.lba_cap / dm->info.zone_size,
                            slba, zone_cap);
}

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
