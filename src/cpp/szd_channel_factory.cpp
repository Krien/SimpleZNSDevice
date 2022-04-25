#include "szd/cpp/szd_channel_factory.h"

#include "szd/cpp/szd_channel.h"
#include "szd/cpp/szd_status.h"
#include "szd/szd.h"
#include "szd/szd_utils.h"

#include <cassert>
#include <string>

namespace SimpleZNSDeviceNamespace {
SZDChannelFactory::SZDChannelFactory(DeviceManager *device_manager,
                                     size_t max_channel_count)
    : max_channel_count_(max_channel_count), channel_count_(0),
      device_manager_(device_manager), refs_(0) {}
SZDChannelFactory::~SZDChannelFactory() { assert(channel_count_ == 0); }

SZDStatus SZDChannelFactory::register_raw_qpair(QPair **qpair) {
  if (channel_count_ >= max_channel_count_) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(z_create_qpair(device_manager_, qpair));
  if (s == SZDStatus::Success) {
    channel_count_++;
  }
  return s;
}

SZDStatus SZDChannelFactory::unregister_raw_qpair(QPair *qpair) {
  SZDStatus s = FromStatus(SZD::z_destroy_qpair(qpair));
  if (s == SZDStatus::Success) {
    channel_count_--;
  }
  return s;
}

SZDStatus SZDChannelFactory::register_channel(SZDChannel **channel) {
  if (channel_count_ >= max_channel_count_) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s;
  QPair **qpair = new QPair *;
  if ((s = FromStatus(z_create_qpair(device_manager_, qpair))) !=
      SZDStatus::Success) {
    return s;
  }
  *channel =
      new SZDChannel(std::unique_ptr<QPair>(*qpair), device_manager_->info);
  channel_count_++;
  delete qpair;
  return SZDStatus::Success;
}

SZDStatus SZDChannelFactory::register_channel(SZDChannel **channel,
                                              uint64_t min_zone_head,
                                              uint64_t max_zone_head) {
  if (channel_count_ >= max_channel_count_) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s;
  QPair **qpair = new QPair *;
  if ((s = FromStatus(z_create_qpair(device_manager_, qpair))) !=
      SZDStatus::Success) {
    return s;
  }
  *channel =
      new SZDChannel(std::unique_ptr<QPair>(*qpair), device_manager_->info,
                     min_zone_head, max_zone_head);
  channel_count_++;
  delete qpair;
  return SZDStatus::Success;
}

SZDStatus SZDChannelFactory::unregister_channel(SZDChannel *channel) {
  delete channel;
  channel_count_++;
  return SZDStatus::Success;
}
} // namespace SimpleZNSDeviceNamespace