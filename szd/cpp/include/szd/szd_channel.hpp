/** \file
 * C++ Wrapper around SZD QPairs that aids in interacting with the device.
 * */
#pragma once
#ifndef SZD_CPP_CHANNEL_H
#define SZD_CPP_CHANNEL_H

#include "szd/datastructures/szd_buffer.hpp"
#include "szd/szd.h"
#include "szd/szd_status.hpp"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
/**
 * @brief
 * *Comes with helper functions and performance optimisations.* /
 */
class SZDChannel {
public:
  SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
             uint64_t min_lba, uint64_t max_lba, bool keep_async_buffer = false,
             uint32_t queue_depth = 1);
  SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
             bool keep_async_buffer = false, uint32_t queue_depth = 1);
  // No copying or implicits
  SZDChannel(const SZDChannel &) = delete;
  SZDChannel &operator=(const SZDChannel &) = delete;
  ~SZDChannel();

  inline uint8_t msb(uint64_t lba_size) const {
    for (uint8_t bit = 0; bit < 64; bit++) {
      if ((2 << bit) & lba_size) {
        return bit + 1;
      }
    }
    // Illegal, lba_size is always a power of 2 right?
    return 0;
  }

  /**
   * @brief Get block-alligned size (ceiling).
   */
  inline uint64_t allign_size(uint64_t size) const {
    // Probably not a performance concern, but it sure is fun to program.
    // If it does not work, use ((size + lba_size_-1)/lba_size_)*lba_size_))
    uint64_t alligned = (size >> lba_msb_) << lba_msb_;
    return alligned + !!(alligned ^ size) * lba_size_;
  }

  // Buffer I/O Operations
  SZDStatus FlushBuffer(uint64_t *lba, const SZDBuffer &buffer);
  SZDStatus FlushBufferSection(uint64_t *lba, const SZDBuffer &buffer,
                               uint64_t section_addr, uint64_t section_size,
                               bool alligned = true);
  SZDStatus ReadIntoBuffer(uint64_t lba, SZDBuffer *buffer, size_t section_addr,
                           size_t section_size, bool alligned = true);

  // Direct I/O Operations
  SZDStatus DirectAppend(uint64_t *lba, void *buffer, const uint64_t size,
                         bool alligned = true);
  SZDStatus DirectRead(uint64_t lba, void *buffer, uint64_t size,
                       bool alligned = true);

  // Async I/O operations. Currently only supports Direct
  // writes < ZASL to ONE zone at a time. WARNINGS:
  //  1. next write operation should be a sync, as we can not write again till
  //  synced.
  //  2. writing more than 1 ZASL is undefined behaviour.
  //  3. CHECK that the write does not cross zones, this will break.
  SZDStatus AsyncAppend(uint64_t *lba, void *buffer, const uint64_t size,
                        uint32_t writer);
  bool PollOnce(uint32_t writer);
  // Pick any writer, if available
  bool PollOnce(uint32_t *writer);
  SZDStatus Sync();
  inline uint32_t GetQueueDepth() { return queue_depth_; }

  // Management of zones
  SZDStatus ResetZone(uint64_t slba);
  SZDStatus ResetAllZones();
  SZDStatus ZoneHead(uint64_t slba, uint64_t *zone_head);
  SZDStatus FinishZone(uint64_t slba);

  // Used to aid with the fact that zonecap != zonesize
  uint64_t TranslateLbaToPba(uint64_t lba);
  uint64_t TranslatePbaToLba(uint64_t lba);

  // diagnostics counters (will return empty values when disabled)
  uint64_t GetBytesWritten() const;
  uint64_t GetAppendOperationsCounter() const;
  uint64_t GetBytesRead() const;
  uint64_t GetReadOperationsCounter() const;
  uint64_t GetZonesResetCounter() const;

  // diagnostics for each zone
  std::vector<uint64_t> GetZonesReset() const;
  std::vector<uint64_t> GetAppendOperations() const;

private:
  QPair *qpair_;
  uint64_t lba_size_;
  uint64_t zasl_;
  uint64_t mdts_;
  uint64_t zone_size_;
  uint64_t zone_cap_;
  uint64_t min_lba_;
  uint64_t max_lba_;
  bool can_access_all_;
  void *backed_memory_spill_;
  uint64_t lba_msb_;
  // async IO
  uint32_t queue_depth_;
  Completion **completion_;
  void **async_buffer_;
  bool keep_async_buffer_;
  size_t *async_buffer_size_;
  // diagnostics counters
#ifdef SZD_PERF_COUNTERS
  std::atomic<uint64_t> bytes_written_;
  std::atomic<uint64_t> append_operations_counter_;
  std::atomic<uint64_t> bytes_read_;
  std::atomic<uint64_t> read_operations_;
  std::atomic<uint64_t> zones_reset_counter_;
#endif
  // diagnostics for heat zones
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  std::vector<uint64_t> zones_reset_;
  std::vector<uint64_t> append_operations_;
#endif
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif
