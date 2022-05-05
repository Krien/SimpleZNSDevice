#include "szd/cpp/datastructures/szd_log.h"
#include "szd/cpp/szd_channel_factory.h"
#include "szd/szd.h"

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDLog::SZDLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
               const uint64_t min_zone_nr, const uint64_t max_zone_nr)
    : min_zone_head_(std::max(min_zone_nr * info.zone_size, info.min_lba)),
      max_zone_head_(std::min(max_zone_nr * info.zone_size, info.max_lba)),
      zone_size_(info.zone_size), lba_size_(info.lba_size),
      write_head_(min_zone_nr * info.zone_size),
      write_tail_(min_zone_nr * info.zone_size),
      channel_factory_(channel_factory) {}
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
