/** \file
 * Minimal C++ wrapper around SZD device.
 * */
#ifndef SZD_CPP_DEVICE_H
#define SZD_CPP_DEVICE_H

#include "szd/cpp/szd_status.h"
#include "szd/szd.h"
#include "szd/szd_namespace.h"

#include <string>
#include <vector>

namespace SimpleZNSDeviceNamespace {

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

  inline DeviceManager *GetDeviceManager() { return manager_; }

private:
  const std::string application_name_;
  // state
  bool initialised_device_;
  bool device_opened_;
  SZD::DeviceManager *manager_;
  std::string opened_device_;
};

} // namespace SimpleZNSDeviceNamespace
#endif