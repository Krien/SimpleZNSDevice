/** \file
 * Circular log that allows appending, reading and partial resets.
 * Threadsafe only when there is 1 reader and 1 writer max.
 * Also do not consume tail when data is being read in this part, external
 * synchronisation...
 * */
#pragma once
#ifndef SZD_CIRCULAR_LOG_H
#define SZD_CIRCULAR_LOG_H

#include "szd/datastructures/szd_buffer.hpp"
#include "szd/datastructures/szd_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel.hpp"
#include "szd/szd_channel_factory.hpp"
#include "szd/szd_status.hpp"

#include <atomic>
#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
class SZDCircularLog : public SZDLog {
public:
  SZDCircularLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
                 const uint64_t min_zone_nr, const uint64_t max_zone_nr);
  ~SZDCircularLog() override;

  SZDStatus Append(const std::string string, uint64_t *lbas = nullptr,
                   bool alligned = true) override;
  SZDStatus Append(const char *data, const size_t size,
                   uint64_t *lbas = nullptr, bool alligned = true) override;
  SZDStatus Append(const SZDBuffer &buffer, uint64_t *lbas = nullptr) override;
  SZDStatus Append(const SZDBuffer &buffer, size_t addr, size_t size,
                   uint64_t *lbas = nullptr, bool alligned = true) override;
  SZDStatus Read(uint64_t lba, char *data, uint64_t size,
                 bool alligned = true) override;
  SZDStatus Read(uint64_t lba, SZDBuffer *buffer, uint64_t size,
                 bool alligned = true) override;
  SZDStatus Read(uint64_t lba, SZDBuffer *buffer, size_t addr, size_t size,
                 bool alligned = true) override;
  SZDStatus ConsumeTail(uint64_t begin_lba, uint64_t end_lba);
  SZDStatus ResetAll() override;
  SZDStatus RecoverPointers() override;

  inline bool Empty() const override { return write_head_ == min_zone_head_; }
  inline uint64_t SpaceAvailable() const override { return space_left_; }
  inline bool SpaceLeft(const size_t size,
                        bool alligned = true) const override {
    uint64_t alligned_size =
        alligned ? size : write_channel_->allign_size(size);
    return alligned_size <= SpaceAvailable();
  }

  inline uint64_t GetWriteHead() const override { return write_head_; }
  inline uint64_t GetWriteTail() const override { return write_tail_; }

  bool IsValidReadAddress(const uint64_t addr, const uint64_t lbas) const;

private:
  void RecalculateSpaceLeft();

  // log
  std::atomic<uint64_t> write_head_;
  std::atomic<uint64_t> write_tail_;
  uint64_t zone_tail_; // only used by writer
  uint64_t space_left_;
  // references
  SZDChannel *read_channel_;
  SZDChannel *write_channel_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif
