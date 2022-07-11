#include "szd/szd_channel_factory.hpp"

#include "szd/szd.h"
#include "szd/szd_channel.hpp"
#include "szd/szd_status.hpp"

#include <cassert>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDChannelFactory::SZDChannelFactory(DeviceManager *device_manager,
                                     size_t max_channel_count)
    : max_channel_count_(max_channel_count), channel_count_(0),
      device_manager_(device_manager), refs_(0) {}
SZDChannelFactory::~SZDChannelFactory() {}

SZDStatus SZDChannelFactory::register_raw_qpair(QPair **qpair) {
  if (channel_count_ >= max_channel_count_ || qpair == nullptr) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_create_qpair(device_manager_, qpair));
  if (s == SZDStatus::Success) {
    channel_count_++;
  }
  return s;
}

SZDStatus SZDChannelFactory::unregister_raw_qpair(QPair *qpair) {
  SZDStatus s = FromStatus(szd_destroy_qpair(qpair));
  if (s == SZDStatus::Success) {
    channel_count_--;
  }
  return s;
}

SZDStatus SZDChannelFactory::register_channel(SZDChannel **channel,
                                              uint64_t min_zone_nr,
                                              uint64_t max_zone_nr,
                                              bool preserve_async_buffer) {
  if (channel_count_ >= max_channel_count_) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s;
  QPair **qpair = new QPair *;
  if ((s = FromStatus(szd_create_qpair(device_manager_, qpair))) !=
      SZDStatus::Success) {
    return s;
  }
  *channel = new SZDChannel(
      std::unique_ptr<QPair>(*qpair), device_manager_->info,
      min_zone_nr * device_manager_->info.zone_size,
      max_zone_nr * device_manager_->info.zone_size, preserve_async_buffer);

  channel_count_++;
  delete qpair;
  return SZDStatus::Success;
}

SZDStatus SZDChannelFactory::register_channel(SZDChannel **channel,
                                              bool preserve_async_buffer) {
  return register_channel(
      channel, device_manager_->info.min_lba / device_manager_->info.zone_size,
      device_manager_->info.max_lba / device_manager_->info.zone_size,
      preserve_async_buffer);
}

SZDStatus SZDChannelFactory::unregister_channel(SZDChannel *channel) {
  delete channel;
  channel_count_--;
  return SZDStatus::Success;
}
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE