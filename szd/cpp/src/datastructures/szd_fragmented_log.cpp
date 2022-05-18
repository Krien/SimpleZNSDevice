#include "szd/datastructures/szd_fragmented_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDFragmentedLog::SZDFragmentedLog(SZDChannelFactory *channel_factory,
                                   const DeviceInfo &info,
                                   const uint64_t min_zone_nr,
                                   const uint64_t max_zone_nr)
    : min_zone_head_(min_zone_nr * info.zone_size),
      max_zone_head_(max_zone_nr * info.zone_size), zone_size_(info.zone_size),
      lba_size_(info.lba_size), zone_bytes_(info.zone_size * info.lba_size),
      channel_factory_(channel_factory) {}
SZDFragmentedLog::~SZDFragmentedLog() {}

SZDStatus SZDFragmentedLog::Append(const SZDBuffer &buffer, size_t addr,
                                   size_t size, uint64_t *lbas, bool alligned) {
  (void)buffer;
  (void)addr;
  (void)size;
  (void)lbas;
  (void)alligned;
  return SZDStatus::Success;
}

SZDStatus SZDFragmentedLog::Read(uint64_t *lbas, char *data, uint64_t size,
                                 bool alligned) {
  (void)lbas;
  (void)data;
  (void)size;
  (void)alligned;
  return SZDStatus::Success;
}
SZDStatus SZDFragmentedLog::Reset(uint64_t &lbas) {
  (void)lbas;
  return SZDStatus::Success;
}
SZDStatus SZDFragmentedLog::ResetAll() { return SZDStatus::Success; }
SZDStatus SZDFragmentedLog::Recover() { return SZDStatus::Success; }

bool SZDFragmentedLog::Empty() const { return true; }
uint64_t SZDFragmentedLog::SpaceAvailable() const { return 0; }
bool SZDFragmentedLog::SpaceLeft(const size_t size, bool alligned) const {
  (void)size;
  (void)alligned;
  return true;
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE