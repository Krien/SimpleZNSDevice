#include "szd/szd_status_code.h"
#include "szd/szd_namespace.h"

#ifdef __cplusplus
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
extern "C" {
#endif

const char *szd_status_code_msg(int status) {
  switch (status) {
  case SZD_SC_SUCCESS:
    return "Succes";
    break;
  case SZD_SC_NOT_ALLOCATED:
    return "structure not be allocated";
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
  case SZD_SC_SPDK_ERROR_FINISH:
    return "Could not finish zone to ZNS device";
    break;
  case SZD_SC_SPDK_ERROR_POLLING:
    return "Error during polling outstanding I/O request";
    break;
  default:
    return "Unknown status";
    break;
  }
}

bool szd_is_valid_code(int status) {
  return status >= 0 && status < SZD_SC_UNKNOWN;
}

#ifdef __cplusplus
}
} // namespace SimpleZNSDeviceNamespace
#endif
