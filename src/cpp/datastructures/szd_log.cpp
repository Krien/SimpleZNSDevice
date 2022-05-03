#include "szd/cpp/datastructures/szd_log.h"
#include "szd/cpp/szd_channel_factory.h"
#include "szd/szd.h"

namespace SimpleZNSDeviceNamespace {
SZDLog::SZDLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
               const uint64_t min_zone_head, const uint64_t max_zone_head)
    : min_zone_head_(std::max(min_zone_head, info.min_lba)),
      max_zone_head_(std::min(max_zone_head, info.max_lba)),
      zone_size_(info.zone_size), lba_size_(info.lba_size),
      write_head_(min_zone_head), write_tail_(min_zone_head),
      channel_factory_(channel_factory) {}
} // namespace SimpleZNSDeviceNamespace
