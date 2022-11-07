#include "szd/szd_status.hpp"
#include "szd/szd_namespace.h"

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDStatus FromStatus(int status) {
  if (!szd_is_valid_code(status)) {
    return SZDStatus::Unknown;
  }
  switch (status) {
  case SZD_SC_SUCCESS:
    return SZDStatus::Success;
    break;
  case SZD_SC_NOT_ALLOCATED:
    return SZDStatus::NotAllocated;
    break;
  case SZD_SC_SPDK_ERROR_INIT:
  case SZD_SC_SPDK_ERROR_OPEN:
  case SZD_SC_SPDK_ERROR_CLOSE:
  case SZD_SC_SPDK_ERROR_PROBE:
    return SZDStatus::DeviceError;
    break;
  case SZD_SC_SPDK_ERROR_APPEND:
  case SZD_SC_SPDK_ERROR_READ:
  case SZD_SC_SPDK_ERROR_RESET:
  case SZD_SC_SPDK_ERROR_REPORT_ZONES:
  case SZD_SC_SPDK_ERROR_FINISH:
  case SZD_SC_SPDK_ERROR_POLLING:
    return SZDStatus::IOError;
    break;
  case SZD_SC_SPDK_ERROR_ZCALLOC:
    return SZDStatus::MemoryError;
    break;
  default:
    return SZDStatus::Unknown;
    break;
  }
  // -Wreturn-type has no knowledge of switch default??
  return SZDStatus::Unknown;
}

SZDStatusDetailed FromStatusDetailed(int status) {
  return SZDStatusDetailed{.sc = FromStatus(status),
                           .msg = szd_status_code_msg(status)};
}
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
