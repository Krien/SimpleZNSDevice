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
    return SZDStatus::InvalidArguments;
    break;
  default:
    return SZDStatus::IOError;
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
