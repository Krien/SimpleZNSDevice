#include "szd/szd_device.hpp"
#include "szd/szd_namespace.h"

#include <cstring>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {

// Necessary when SZDDevice is initialised, then deleted and a new SZDDevice is
// created. This makes sure that the device is aware that DPDK is already
// initialised
static bool dpdk_initialised = false;

SZDDevice::SZDDevice(const std::string &application_name)
    : application_name_(application_name), initialised_device_(false),
      device_opened_(false), manager_(new EngineManager *), opened_device_() {}

SZDDevice::~SZDDevice() {
  if (initialised_device_ || device_opened_) {
    Destroy();
  }
  delete manager_;
}

SZDStatus SZDDevice::Init() {
  DeviceOptions opts = {.name = application_name_.data(),
                        .setup_spdk = !dpdk_initialised, 
                        .iouring_sqthread = false,
                        .iouring_fixed = false,
  };
  SZDStatus s = FromStatus(szd_init(manager_, &opts, SZD::SZD_IO_BACKEND_IO_URING));
  if (s == SZDStatus::Success) {
    initialised_device_ = true;
    dpdk_initialised = true;
  }
  return s;
}

SZDStatus SZDDevice::Reinit() {
  if (initialised_device_ != true) {
    SZD_LOG_ERROR("SZD: Device: Reinit: Not initialised\n");
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_reinit(manager_));
  if (s == SZDStatus::Success) {
    initialised_device_ = true;
  }
  return s;
}

typedef struct {
  char **traddr; /**< transport ids of all probed devices.*/
  bool *zns;     /**< Foreach probed device, is it a ZNS device?*/
  struct spdk_nvme_ctrlr **ctrlr; /**< The controller(s) of the devices.*/
  uint8_t devices;                /**< Used to identify global device count.*/
  pthread_mutex_t *mut; /**< Ensures that probe information is thread safe.*/
} ProbeInformation;

SZDStatus SZDDevice::Probe(std::vector<DeviceOpenInfo> &info) {
  if (!initialised_device_) {
    SZD_LOG_ERROR("SZD: Device: Probe: Invalid args\n");
    return SZDStatus::InvalidArguments;
  }
  ProbeInformation **prober = new ProbeInformation *;
  SZDStatus s = FromStatus(szd_probe(*manager_, (void**)prober));
  if (s != SZDStatus::Success) {
    SZD_LOG_ERROR("SZD: Device: Probe: Failed probing\n");
    return s;
  }
  for (uint8_t dev = 0; dev < (*prober)->devices; dev++) {
    std::string trid;
    trid.assign((*prober)->traddr[dev], strlen((*prober)->traddr[dev]));
    info.push_back(
        DeviceOpenInfo{.traddr = trid, .is_zns = (*prober)->zns[dev]});
  }
  szd_free_probe_information(*manager_, *prober);
  delete prober;
  // Probe can leave SZD in a weird attached state (zombie devices).
  s = Reinit();
  return s;
}

SZDStatus SZDDevice::Open(const std::string &device_name, uint64_t min_zone,
                          uint64_t max_zone) {
  if (!initialised_device_ || device_opened_) {
    SZD_LOG_ERROR("SZD: Device: Open: Invalid args/state\n");
    return SZDStatus::InvalidArguments;
  }
  opened_device_.assign(device_name);
  DeviceOpenOptions oopts = {.min_zone = min_zone, .max_zone = max_zone};
  SZDStatus s = FromStatus(szd_open(*manager_, opened_device_.data(), &oopts));
  if (s == SZDStatus::Success) {
    device_opened_ = true;
  }
  return s;
}

SZDStatus SZDDevice::Open(const std::string &device_name) {
  return Open(device_name, 0, 0);
}

SZDStatus SZDDevice::Close() {
  if (!initialised_device_ || !device_opened_) {
    SZD_LOG_ERROR("SZD: Device: Close: Nothing to close\n");
    return SZDStatus::InvalidArguments;
  }
  device_opened_ = false;
  return FromStatus(szd_close(*manager_));
}

SZDStatus SZDDevice::GetInfo(DeviceInfo *info) const {
  if (!device_opened_) {
    SZD_LOG_ERROR("SZD: Device: GetInfo: Not initialised\n");
    return SZDStatus::InvalidArguments;
  }
  *info = (*manager_)->manager_->info;
  return SZDStatus::Success;
}

SZDStatus SZDDevice::Destroy() {
  if (!initialised_device_) {
    SZD_LOG_ERROR("SZD: Device: Destroy: Not initialised\n");
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_destroy(*manager_));
  device_opened_ = false;
  initialised_device_ = false;
  return s;
}
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
