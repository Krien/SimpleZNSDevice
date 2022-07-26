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

#include <functional>
#include <mutex>
#include <string>

// Disabled, hampered maintainability and performance gain where ~1ns
//#define ALLOW_ASYNC_APPEND

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
class SZDFragmentedLog {
public:
  // TODO: Add support for multiple readers...
  SZDFragmentedLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
                   const uint64_t min_zone_nr, const uint64_t max_zone_nr,
                   const uint8_t number_of_readers,
                   const uint8_t number_of_writers);
  SZDFragmentedLog(const SZDFragmentedLog &) = delete;
  SZDFragmentedLog &operator=(const SZDFragmentedLog &) = delete;
  ~SZDFragmentedLog();

  SZDStatus Append(const char *buffer, size_t size,
                   std::vector<std::pair<uint64_t, uint64_t>> &regions,
                   bool alligned = true, uint8_t writer = 0);
  SZDStatus Append(const SZDBuffer &buffer, size_t addr, size_t size,
                   std::vector<std::pair<uint64_t, uint64_t>> &regions,
                   bool alligned = true, uint8_t writer = 0);
  SZDStatus Read(const std::vector<std::pair<uint64_t, uint64_t>> &regions,
                 char *data, uint64_t size, bool alligned = true,
                 uint8_t reader = 0);
  SZDStatus Reset(std::vector<std::pair<uint64_t, uint64_t>> &regions);
  SZDStatus ResetAll();
  SZDStatus Recover();

  bool Empty() const;
  uint64_t SpaceAvailable() const;
  bool SpaceLeft(const size_t size, bool alligned = true) const;

  std::string Encode();
  SZDStatus DecodeFrom(const char *data, const size_t size);

  inline uint64_t GetBytesWritten() const {
    uint64_t written = 0;
    for (size_t i = 0; i < number_of_writers_; i++) {
      written += write_channel_[i]->GetBytesWritten();
    }
    return written;
  };
  inline uint64_t GetAppendOperationsCounter() const {
    uint64_t appends = 0;
    for (size_t i = 0; i < number_of_writers_; i++) {
      appends += write_channel_[i]->GetAppendOperationsCounter();
    }
    return appends;
  };
  inline uint64_t GetBytesRead() const {
    uint64_t read = 0;
    for (size_t i = 0; i < number_of_readers_; i++) {
      read += read_channel_[i]->GetBytesRead();
    }
    return read;
  };
  inline uint64_t GetReadOperationsCounter() const {
    uint64_t read = 0;
    for (size_t i = 0; i < number_of_readers_; i++) {
      read += read_channel_[i]->GetReadOperationsCounter();
    }
    return read;
  };
  inline uint64_t GetZonesResetCounter() const {
    return write_channel_[1]->GetZonesResetCounter();
  };
  inline std::vector<uint64_t> GetZonesReset() const {
    return write_channel_[1]->GetZonesReset();
  };
  inline std::vector<uint64_t> GetAppendOperations() const {
    std::vector<uint64_t> resets = write_channel_[0]->GetAppendOperations();
    for (uint8_t i = 1; i < number_of_writers_; i++) {
      std::vector<uint64_t> tmp = write_channel_[i]->GetAppendOperations();
      std::transform(resets.begin(), resets.end(), tmp.begin(), tmp.begin(),
                     std::plus<uint64_t>());
    }
    return resets;
  }

  bool TESTEncodingDecoding() const;

private:
#ifdef ALLOW_ASYNC_APPEND
  SZDStatus AsyncAppend(const char *buffer, size_t size,
                        std::vector<std::pair<uint64_t, uint64_t>> &regions,
                        bool alligned = true);
#endif
  // const after initialisation
  const uint64_t min_zone_head_;
  const uint64_t max_zone_head_;
  const uint64_t zone_size_;
  const uint64_t zone_cap_;
  const uint64_t lba_size_;
  const uint64_t zasl_;
  const uint64_t zone_bytes_;
  const uint8_t number_of_readers_;
  const uint8_t number_of_writers_;
  std::mutex mut_;
  // Log
  SZDFreeList *freelist_;
  SZDFreeList *seeker_;
  uint64_t zones_left_;
  // references
  SZDChannelFactory *channel_factory_;
  SZDChannel **write_channel_;
  SZDChannel **read_channel_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif