#include "szd/cpp/szd_device.h"
#include "szd/szd_namespace.h"

namespace SimpleZNSDeviceNamespace {

SZDDevice::SZDDevice(const std::string &application_name)
    : application_name_(application_name), initialised_spdk_(false),
      device_opened_(false), manager_(new DeviceManager()), opened_device_() {}

SZDDevice::~SZDDevice() {
  if (initialised_spdk_ || device_opened_) {
    Destroy();
  }
}

SZDStatus SZDDevice::Init() {
  DeviceOptions opts = {.name = application_name_.data(),
                        .setup_spdk = !initialised_spdk_};
  SZDStatus s = FromStatus(szd_init(&manager_, &opts));
  if (s == SZDStatus::Success) {
    initialised_spdk_ = true;
  }
  return s;
}

SZDStatus SZDDevice::Reinit() {
  initialised_spdk_ = true;
  return Init();
}

SZDStatus SZDDevice::Probe(std::vector<DeviceOpenInfo> &info) const {
  if (!initialised_spdk_) {
    return SZDStatus::InvalidArguments;
  }
  ProbeInformation *prober = new ProbeInformation();
  SZDStatus s = FromStatus(szd_probe(manager_, &prober));
  if (s != SZDStatus::Success) {
    return s;
  }
  for (uint8_t dev = 0; dev < prober->devices; dev++) {
    info.push_back(DeviceOpenInfo{.traddr = std::string(prober->traddr[dev]),
                                  .is_zns = prober->zns[dev]});
  }
  return s;
}

SZDStatus SZDDevice::Open(const std::string &device_name, uint64_t min_zone,
                          uint64_t max_zone) {
  if (!initialised_spdk_) {
    return SZDStatus::InvalidArguments;
  }
  opened_device_.assign(device_name.data(), device_name.size());
  DeviceOpenOptions oopts = {.min_zone = min_zone, .max_zone = max_zone};
  SZDStatus s = FromStatus(szd_open(manager_, opened_device_.data(), &oopts));
  if (s == SZDStatus::Success) {
    device_opened_ = true;
  }
  return s;
}

SZDStatus SZDDevice::Open(const std::string &device_name) {
  return Open(device_name, 0, 0);
}

SZDStatus SZDDevice::GetInfo(DeviceInfo *info) const {
  if (!device_opened_) {
    return SZDStatus::InvalidArguments;
  }
  *info = manager_->info;
  return SZDStatus::Success;
}

SZDStatus SZDDevice::Destroy() {
  if (!initialised_spdk_ || !device_opened_) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_destroy(manager_));
  device_opened_ = false;
  return s;
}
} // namespace SimpleZNSDeviceNamespace
