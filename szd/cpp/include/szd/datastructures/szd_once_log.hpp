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

#include <algorithm>
#include <deque>
#include <functional>
#include <string>
#include <variant>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {

typedef std::variant<uint32_t, SZDChannel *> queue_depth_or_external_channel;

class SZDOnceLog : public SZDLog {
public:
  // Either specify an external channel (borrowed by once log), or a queue depth
  SZDOnceLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
             const uint64_t min_zone_nr, const uint64_t max_zone_nr,
             const queue_depth_or_external_channel channel_definition);
  ~SZDOnceLog() override;

  // Direct IO
  SZDStatus Append(const std::string string, uint64_t *lbas = nullptr,
                   bool alligned = true) override;
  SZDStatus Append(const char *data, const size_t size,
                   uint64_t *lbas = nullptr, bool alligned = true) override;
  SZDStatus Append(const SZDBuffer &buffer, uint64_t *lbas = nullptr) override;
  SZDStatus Append(const SZDBuffer &buffer, size_t addr, size_t size,
                   uint64_t *lbas = nullptr, bool alligned = true) override;

  // Async IO (do NOT mix with normal Appends)
  SZDStatus AsyncAppend(const char *data, const size_t size,
                        uint64_t *lbas = nullptr, bool alligned = true);
  SZDStatus Sync();

  SZDStatus Read(uint64_t lba, char *data, uint64_t size, bool alligned = true,
                 uint8_t reader = 0) override;
  SZDStatus Read(uint64_t lba, SZDBuffer *buffer, uint64_t size,
                 bool alligned = true, uint8_t reader = 0) override;
  SZDStatus Read(uint64_t lba, SZDBuffer *buffer, size_t addr, size_t size,
                 bool alligned = true, uint8_t reader = 0) override;
  SZDStatus ReadAll(std::string &out);

  SZDStatus ResetAll() override;
  SZDStatus ResetAllForce();
  SZDStatus RecoverPointers() override;
  SZDStatus MarkInactive();

  inline bool Empty() const override { return write_head_ == min_zone_head_; }
  inline uint64_t SpaceAvailable() const override { return space_left_; }
  inline bool SpaceLeft(const size_t size,
                        bool alligned = true) const override {
    uint64_t alligned_size =
        alligned ? size : write_channel_->allign_size(size);
    return alligned_size <= space_left_;
  }

  inline uint64_t GetWriteHead() const override { return write_head_; }
  inline uint64_t GetWriteTail() const override { return min_zone_head_; }
  inline uint8_t GetNumberOfReaders() const override { return 1; };

  inline uint64_t GetBytesWritten() const override {
    return write_channel_->GetBytesWritten();
  };
  inline uint64_t GetAppendOperationsCounter() const override {
    return write_channel_->GetAppendOperationsCounter();
  };
  inline uint64_t GetBytesRead() const override {
    return read_reset_channel_->GetBytesRead();
  };
  inline uint64_t GetReadOperationsCounter() const override {
    return read_reset_channel_->GetReadOperationsCounter();
  };
  inline uint64_t GetZonesResetCounter() const override {
    return read_reset_channel_->GetZonesResetCounter();
  };
  inline std::vector<uint64_t> GetZonesReset() const override {
    return read_reset_channel_->GetZonesReset();
  };
  inline std::vector<uint64_t> GetAppendOperations() const override {
    return write_channel_->GetAppendOperations();
  };

private:
  bool IsValidAddress(uint64_t lba, uint64_t lbas);
  // log
  const uint64_t block_range_;
  uint32_t max_write_depth_;
  uint64_t space_left_;
  uint64_t write_head_;
  uint64_t zasl_;
  // channels used
  SZDChannel *write_channel_;
  SZDChannel *read_reset_channel_;
  bool write_channels_owned_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif
