/** \file
 * Log that only allows appending, reading an complete resets.
 * */
#pragma once
#ifndef SZD_ONCE_LOG_H
#define SZD_ONCE_LOG_H

#include "szd/cpp/datastructures/szd_buffer.h"
#include "szd/cpp/datastructures/szd_log.h"
#include "szd/cpp/szd_channel.h"
#include "szd/cpp/szd_channel_factory.h"
#include "szd/cpp/szd_status.h"
#include "szd/szd.h"

#include <string>

namespace SimpleZNSDeviceNamespace {
class SZDOnceLog : public SZDLog {
public:
  SZDOnceLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
             const uint64_t min_zone_head, const uint64_t max_zone_head);
  ~SZDOnceLog() override;
  SZDStatus Append(const std::string string, uint64_t *lbas = nullptr) override;
  SZDStatus Append(const char *data, const size_t size,
                   uint64_t *lbas = nullptr, bool alligned = true) override;
  SZDStatus Append(const SZDBuffer &buffer, uint64_t *lbas = nullptr) override;
  SZDStatus Append(const SZDBuffer &buffer, size_t addr, size_t size,
                   uint64_t *lbas = nullptr, bool alligned = true) override;
  SZDStatus Read(char *data, uint64_t lba, uint64_t size,
                 bool alligned = true) override;
  SZDStatus Read(SZDBuffer *buffer, uint64_t lba, uint64_t size,
                 bool alligned = true) override;
  SZDStatus Read(SZDBuffer *buffer, size_t addr, size_t size, uint64_t lba,
                 bool alligned = true) override;
  SZDStatus ResetAll() override;
  SZDStatus RecoverPointers() override;
  bool Empty() const override { return write_head_ == min_zone_head_; }
  bool SpaceLeft(const size_t size) const override {
    return write_head_ + size / lba_size_ < max_zone_head_;
  }

private:
  // log
  uint64_t zone_head_;
  // references
  SZDChannel *channel_;
};
} // namespace SimpleZNSDeviceNamespace

#endif
