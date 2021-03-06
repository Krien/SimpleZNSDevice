/** \file
 * Minimal C++ wrapper around SZD device.
 * */
#ifndef SZD_CPP_DEVICE_H
#define SZD_CPP_DEVICE_H

#include "szd/szd.h"
#include "szd/szd_namespace.h"
#include "szd/szd_status.hpp"

#include <string>
#include <vector>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {

struct DeviceOpenInfo {
  std::string traddr;
  bool is_zns;
};

class SZDDevice {
public:
  explicit SZDDevice(const std::string &application_name);
  SZDDevice();
  // No copying or implicits
  SZDDevice(const SZDDevice &) = delete;
  SZDDevice &operator=(const SZDDevice &) = delete;
  ~SZDDevice();
  SZDStatus Init();
  SZDStatus Reinit();
  SZDStatus Probe(std::vector<DeviceOpenInfo> &info);
  SZDStatus Open(const std::string &device_name, uint64_t min_zone,
                 uint64_t max_zone);
  SZDStatus Open(const std::string &device_name);
  SZDStatus Close();
  SZDStatus GetInfo(DeviceInfo *info) const;
  SZDStatus Destroy();

  inline DeviceManager *GetDeviceManager() {
    return initialised_device_ ? *manager_ : nullptr;
  }

private:
  const std::string application_name_;
  // state
  bool initialised_device_;
  bool device_opened_;
  SZD::DeviceManager **manager_;
  std::string opened_device_;
};

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
#endif