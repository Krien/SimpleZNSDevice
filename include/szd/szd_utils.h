#pragma once
#ifndef SZD_UTILS_H
#define SZD_UTILS_H

#include "spdk/nvme.h"
#include "szd/szd_errno.h"
#include "szd/szd_namespace.h"

#ifdef __cplusplus
namespace SimpleZNSDeviceNamespace {
extern "C" {
#endif

#define RETURN_ERR_ON_NULL(x)                                                  \
  do {                                                                         \
    if ((x) == nullptr) {                                                      \
      return (SZD_SC_NOT_ALLOCATED);                                           \
    }                                                                          \
  } while (0)

#define POLL_QPAIR(qpair, target)                                              \
  do {                                                                         \
    while (!(target)) {                                                        \
      spdk_nvme_qpair_process_completions((qpair), 0);                         \
    }                                                                          \
  } while (0)
#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
#endif