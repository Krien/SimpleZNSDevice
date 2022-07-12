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

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
class SZDOnceLog : public SZDLog {
public:
  SZDOnceLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
             const uint64_t min_zone_nr, const uint64_t max_zone_nr,
             const uint8_t number_of_writers,
             SZDChannel **write_channel = nullptr);
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
  SZDStatus RecoverPointers() override;
  SZDStatus MarkInactive();

  inline bool Empty() const override { return write_head_ == min_zone_head_; }
  inline uint64_t SpaceAvailable() const override { return space_left_; }
  inline bool SpaceLeft(const size_t size,
                        bool alligned = true) const override {
    uint64_t alligned_size =
        alligned ? size : write_channel_[0]->allign_size(size);
    return alligned_size <= space_left_;
  }

  inline uint64_t GetWriteHead() const override { return write_head_; }
  inline uint64_t GetWriteTail() const override { return min_zone_head_; }
  inline uint8_t GetNumberOfReaders() const override { return 1; };

  inline uint64_t GetBytesWritten() const override {
    uint64_t bytes_written = 0;
    for (uint8_t i = 0; i < number_of_writers_; i++) {
      bytes_written += write_channel_[i]->GetBytesWritten();
    }
    return bytes_written;
  };
  inline uint64_t GetAppendOperationsCounter() const override {
    uint64_t append_operations = 0;
    for (uint8_t i = 0; i < number_of_writers_; i++) {
      append_operations += write_channel_[i]->GetAppendOperationsCounter();
    }
    return append_operations;
  };
  inline uint64_t GetBytesRead() const override {
    return read_channel_->GetBytesRead();
  };
  inline uint64_t GetReadOperationsCounter() const override {
    return read_channel_->GetReadOperationsCounter();
  };
  inline uint64_t GetZonesResetCounter() const override {
    uint64_t reset_operations = 0;
    reset_operations += read_channel_->GetZonesResetCounter();
    return reset_operations;
  };
  inline std::vector<uint64_t> GetZonesReset() const override {
    std::vector<uint64_t> resets = read_channel_->GetZonesReset();
    return resets;
  };
  inline std::vector<uint64_t> GetAppendOperations() const override {
    std::vector<uint64_t> resets = write_channel_[0]->GetAppendOperations();
    for (uint8_t i = 1; i < number_of_writers_; i++) {
      std::vector<uint64_t> tmp = write_channel_[i]->GetAppendOperations();
      std::transform(resets.begin(), resets.end(), tmp.begin(), tmp.begin(),
                     std::plus<uint64_t>());
    }
    return resets;
  };

private:
  bool IsValidAddress(uint64_t lba, uint64_t lbas);
  // log
  const uint8_t number_of_writers_;
  const uint64_t block_range_;
  uint64_t space_left_;
  uint64_t write_head_;
  uint64_t zasl_;
  // stall writes
  // SZDBuffer write_buffer_;
  // references
  // TODO: cleanup, we only use 1 channel at most, we have moved logic to queue
  // pairs
  SZDChannel **write_channel_;
  uint32_t qpair_depth_;
  SZDChannel *read_channel_;
  bool write_channels_owned_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif
