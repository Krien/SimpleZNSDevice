/** \file
 * Interface for fragmented log structures
 * alligned on ZONE level.
 * */
#pragma once
#ifndef SZD_FRAGMENTED_LOG_H
#define SZD_FRAGMENTED_LOG_H

#include "szd/datastructures/szd_buffer.hpp"
#include "szd/datastructures/szd_freezone_list.hpp"
#include "szd/szd.h"
#include "szd/szd_channel.hpp"
#include "szd/szd_channel_factory.hpp"
#include "szd/szd_status.hpp"

#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
class SZDFragmentedLog {
public:
  SZDFragmentedLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
                   const uint64_t min_zone_nr, const uint64_t max_zone_nr);
  SZDFragmentedLog(const SZDFragmentedLog &) = delete;
  SZDFragmentedLog &operator=(const SZDFragmentedLog &) = delete;
  ~SZDFragmentedLog();

  SZDStatus Append(const char *buffer, size_t size,
                   std::vector<std::pair<uint64_t, uint64_t>> &regions,
                   bool alligned = true);
  SZDStatus Append(const SZDBuffer &buffer, size_t addr, size_t size,
                   std::vector<std::pair<uint64_t, uint64_t>> &regions,
                   bool alligned = true);
  SZDStatus Read(const std::vector<std::pair<uint64_t, uint64_t>> &regions,
                 char *data, uint64_t size, bool alligned = true);
  SZDStatus Reset(std::vector<std::pair<uint64_t, uint64_t>> &regions);
  SZDStatus ResetAll();
  SZDStatus Recover();

  bool Empty() const;
  uint64_t SpaceAvailable() const;
  bool SpaceLeft(const size_t size, bool alligned = true) const;

private:
  // const after initialisation
  const uint64_t min_zone_head_;
  const uint64_t max_zone_head_;
  const uint64_t zone_size_;
  const uint64_t lba_size_;
  const uint64_t zone_bytes_;
  // Log
  SZDFreeList *freelist_;
  SZDFreeList *seeker_;
  uint64_t zones_left_;
  // references
  SZDChannelFactory *channel_factory_;
  SZDChannel *write_channel_;
  SZDChannel *read_channel_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif