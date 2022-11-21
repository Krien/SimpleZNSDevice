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
                 const uint64_t min_zone_nr, const uint64_t max_zone_nr,
                 const uint8_t number_of_readers);
  ~SZDCircularLog() override;

  SZDStatus Append(const std::string string, uint64_t *lbas = nullptr,
                   bool alligned = true) override;
  SZDStatus Append(const char *data, const size_t size,
                   uint64_t *lbas = nullptr, bool alligned = true) override;
  SZDStatus Append(const SZDBuffer &buffer, uint64_t *lbas = nullptr) override;
  SZDStatus Append(const SZDBuffer &buffer, size_t addr, size_t size,
                   uint64_t *lbas = nullptr, bool alligned = true) override;
  SZDStatus Read(uint64_t lba, char *data, uint64_t size, bool alligned = true,
                 uint8_t reader = 0) override;
  SZDStatus Read(uint64_t lba, SZDBuffer *buffer, uint64_t size,
                 bool alligned = true, uint8_t reader = 0) override;
  SZDStatus Read(uint64_t lba, SZDBuffer *buffer, size_t addr, size_t size,
                 bool alligned = true, uint8_t reader = 0) override;
  SZDStatus ConsumeTail(uint64_t begin_lba, uint64_t end_lba);
  SZDStatus ResetAll() override;
  SZDStatus RecoverPointers() override;

  uint64_t wrapped_addr(uint64_t addr);

  inline bool Empty() const override { return write_head_ == min_zone_head_; }
  inline uint64_t SpaceAvailable() const override { return space_left_; }
  inline bool SpaceLeft(const size_t size,
                        bool alligned = true) const override {
    uint64_t alligned_size =
        alligned ? size : write_channel_->allign_size(size);
    return alligned_size <= SpaceAvailable();
  }

  inline uint64_t GetWriteHead() const override {
    return write_head_.load(std::memory_order_acquire);
  }
  inline uint64_t GetWriteTail() const override {
    return write_tail_.load(std::memory_order_acquire);
  }
  inline uint8_t GetNumberOfReaders() const override {
    return number_of_readers_;
  };

  inline uint64_t GetBytesWritten() const override {
    return write_channel_->GetBytesWritten();
  };
  inline uint64_t GetAppendOperationsCounter() const {
    return write_channel_->GetAppendOperationsCounter();
  }
  inline uint64_t GetBytesRead() const override {
    uint64_t read = 0;
    for (size_t i = 0; i < number_of_readers_; i++) {
      read += read_channel_[i]->GetBytesRead();
    }
    return read;
  };
  inline uint64_t GetReadOperationsCounter() const override {
    uint64_t read = 0;
    for (size_t i = 0; i < number_of_readers_; i++) {
      read += read_channel_[i]->GetReadOperationsCounter();
    }
    return read;
  };
  inline uint64_t GetZonesResetCounter() const override {
    return reset_channel_->GetZonesResetCounter();
  };
  inline std::vector<uint64_t> GetZonesReset() const override {
    return reset_channel_->GetZonesReset();
  };
  inline std::vector<uint64_t> GetAppendOperations() const override {
    return write_channel_->GetAppendOperations();
  };

  bool IsValidReadAddress(const uint64_t addr, const uint64_t lbas) const;

private:
  void RecalculateSpaceLeft();

  // log
  const uint8_t number_of_readers_;
  std::atomic<uint64_t> write_head_;
  std::atomic<uint64_t> write_tail_;
  uint64_t zone_tail_; // only used by writer
  std::atomic<uint64_t> space_left_;
  // references
  SZDChannel **read_channel_;
  SZDChannel *reset_channel_;
  SZDChannel *write_channel_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif
