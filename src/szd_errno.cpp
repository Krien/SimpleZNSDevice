#include "szd/szd_errno.h"
#include "szd/szd_namespace.h"

#ifdef __cplusplus
namespace SimpleZNSDeviceNamespace {
#endif

const char *szd_status_code_msg(int status) {
  switch (status) {
  case SZD_SC_SUCCESS:
    return "succes\n";
    break;
  case SZD_SC_NOT_ALLOCATED:
    return "structure not allocated";
    break;
  case SZD_SC_SPDK_ERROR_INIT:
    return "Could not init SPDK";
    break;
  case SZD_SC_SPDK_ERROR_OPEN:
    return "Could not open ZNS device";
    break;
  case SZD_SC_SPDK_ERROR_CLOSE:
    return "Could not close ZNS device";
    break;
  case SZD_SC_SPDK_ERROR_PROBE:
    return "Could not probe ZNS device";
    break;
  case SZD_SC_SPDK_ERROR_APPEND:
    return "Could not append to ZNS device";
    break;
  case SZD_SC_SPDK_ERROR_READ:
    return "Could not read from ZNS device";
    break;
  case SZD_SC_SPDK_ERROR_RESET:
    return "Could not report zones from ZNS device";
    break;
  case SZD_SC_SPDK_ERROR_ZCALLOC:
    return "Could not allocate DMA backed SPDK memory";
    break;
  case SZD_SC_SPDK_ERROR_QPAIR:
    return "Error when calling the Qpair";
    break;
  default:
    return "unknown status";
    break;
  }
}

bool szd_is_valid_code(int status) {
  return status >= 0 && status < SZD_SC_UNKNOWN;
}

#ifdef __cplusplus
} // namespace SimpleZNSDeviceNamespace
#endif
