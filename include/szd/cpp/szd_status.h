/** \file
 * Translates C-Style SZD return codes to more advanced C++-like codes.
 * */
#ifndef SZD_CPP_STATUS_H
#define SZD_CPP_STATUS_H

#include "szd/szd_namespace.h"
#include "szd/szd_status_code.h"

namespace SimpleZNSDeviceNamespace {
enum class SZDStatus { Success, InvalidArguments, IOError, Unknown };
struct SZDStatusDetailed {
  SZDStatus sc;
  const char *msg;
};

SZDStatus FromStatus(int status);
SZDStatusDetailed FromStatusDetailed(int status);

} // namespace SimpleZNSDeviceNamespace
#endif