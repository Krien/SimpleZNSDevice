#include "szd/datastructures/szd_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDLog::SZDLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
               const uint64_t min_zone_nr, const uint64_t max_zone_nr,
               const uint8_t number_of_readers)
    : min_zone_head_(std::max(min_zone_nr * info.zone_cap,
                              (info.min_lba / info.zone_size) * info.zone_cap)),
      max_zone_head_(std::min(max_zone_nr * info.zone_cap,
                              (info.max_lba / info.zone_size) * info.zone_cap)),
      zone_size_(info.zone_size), zone_cap_(info.zone_cap),
      lba_size_(info.lba_size), number_of_readers_(number_of_readers),
      channel_factory_(channel_factory) {}
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
