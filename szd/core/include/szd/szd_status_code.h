/** \file
 * Status codes that can be returned by the SZD interface.
 */
#pragma once
#ifndef SZD_STATUS_CODE_H
#define SZD_STATUS_CODE_H

#include "szd/szd_namespace.h"

#ifdef __cplusplus
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
extern "C" {
#else
#include <stdbool.h>
#endif

/**
 * @brief return codes as used by SZD
 */
enum szd_status_code {
  SZD_SC_SUCCESS = 0x00,
  SZD_SC_NOT_ALLOCATED = 0x01,
  SZD_SC_SPDK_ERROR_INIT = 0x02,
  SZD_SC_SPDK_ERROR_OPEN = 0x03,
  SZD_SC_SPDK_ERROR_CLOSE = 0x04,
  SZD_SC_SPDK_ERROR_PROBE = 0x05,
  SZD_SC_SPDK_ERROR_APPEND = 0x06,
  SZD_SC_SPDK_ERROR_READ = 0x07,
  SZD_SC_SPDK_ERROR_RESET = 0x08,
  SZD_SC_SPDK_ERROR_REPORT_ZONES = 0x09,
  SZD_SC_SPDK_ERROR_ZCALLOC = 0x0A,
  SZD_SC_SPDK_ERROR_QPAIR = 0x0B,
  SZD_SC_UNKNOWN = 0x0C
};

extern const char *szd_status_code_msg(int status);
extern bool szd_is_valid_code(int status);

#ifdef __cplusplus
}
}
#endif
#endif
