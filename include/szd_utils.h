#include "spdk/nvme.h"
namespace SimpleZNSDeviceNamespace {
extern "C" {
#define RETURN_CODE_ON_NULL(x, err)                                            \
  do {                                                                         \
    if ((x) == nullptr) {                                                      \
      return (err);                                                            \
    }                                                                          \
  } while (0)

#define POLL_QPAIR(qpair, target)                                              \
  do {                                                                         \
    while (!(target)) {                                                        \
      spdk_nvme_qpair_process_completions((qpair), 0);                         \
    }                                                                          \
  } while (0)

#define ZNS_STATUS_SUCCESS 0x00
#define ZNS_STATUS_NOT_ALLOCATED 0x01
#define ZNS_STATUS_SPDK_ERROR 0x02
#define ZNS_STATUS_UNKNOWN 0x03
}
} // namespace SimpleZNSDeviceNamespace