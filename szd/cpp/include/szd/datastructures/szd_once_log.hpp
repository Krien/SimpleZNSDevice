/** \file
 * Log that only allows appending, reading an complete resets.
 * */
#pragma once
#ifndef SZD_ONCE_LOG_H
#define SZD_ONCE_LOG_H

#include "szd/datastructures/szd_buffer.hpp"
#include "szd/datastructures/szd_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel.hpp"
#include "szd/szd_channel_factory.hpp"
#include "szd/szd_status.hpp"

#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
class SZDOnceLog : public SZDLog {
public:
  SZDOnceLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
             const uint64_t min_zone_nr, const uint64_t max_zone_nr);
  ~SZDOnceLog() override;

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
  SZDStatus ResetAll() override;
  SZDStatus RecoverPointers() override;

  inline bool Empty() const override { return write_head_ == min_zone_head_; }
  inline uint64_t SpaceAvailable() const override { return space_left_; }
  inline bool SpaceLeft(const size_t size,
                        bool alligned = true) const override {
    uint64_t alligned_size = alligned ? size : channel_->allign_size(size);
    return alligned_size <= space_left_;
  }

  inline uint64_t GetWriteHead() const override { return write_head_; }
  inline uint64_t GetWriteTail() const override { return min_zone_head_; }

private:
  bool IsValidAddress(uint64_t lba, uint64_t lbas);
  // log
  const uint64_t block_range_;
  uint64_t space_left_;
  uint64_t write_head_;
  // stall writes
  // SZDBuffer write_buffer_;
  // references
  SZDChannel *channel_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif
