/** \file
 * Interface for simple log structures
 * */
#pragma once
#ifndef SZD_LOG_H
#define SZD_LOG_H

#include "szd/datastructures/szd_buffer.hpp"
#include "szd/szd.h"
#include "szd/szd_channel.hpp"
#include "szd/szd_channel_factory.hpp"
#include "szd/szd_status.hpp"

#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
class SZDLog {
public:
  SZDLog(SZDChannelFactory *channel_factory, const DeviceInfo &info,
         const uint64_t min_zone_nr, const uint64_t max_zone_nr,
         const uint8_t number_of_readers);
  SZDLog(const SZDLog &) = delete;
  SZDLog &operator=(const SZDLog &) = delete;
  virtual ~SZDLog() = default;

  virtual SZDStatus Append(const std::string string, uint64_t *lbas = nullptr,
                           bool alligned = true) = 0;
  virtual SZDStatus Append(const char *data, const size_t size,
                           uint64_t *lbas = nullptr, bool alligned = true) = 0;
  virtual SZDStatus Append(const SZDBuffer &buffer,
                           uint64_t *lbas = nullptr) = 0;
  virtual SZDStatus Append(const SZDBuffer &buffer, size_t addr, size_t size,
                           uint64_t *lbas = nullptr, bool alligned = true) = 0;
  virtual SZDStatus Read(uint64_t lba, char *data, uint64_t size,
                         bool alligned = true, uint8_t reader = 0) = 0;
  virtual SZDStatus Read(uint64_t lba, SZDBuffer *buffer, uint64_t size,
                         bool alligned = true, uint8_t reader = 0) = 0;
  virtual SZDStatus Read(uint64_t lba, SZDBuffer *buffer, size_t addr,
                         size_t size, bool alligned = true,
                         uint8_t reader = 0) = 0;
  virtual SZDStatus ResetAll() = 0;
  virtual SZDStatus RecoverPointers() = 0;

  virtual bool Empty() const = 0;
  virtual uint64_t SpaceAvailable() const = 0;
  virtual bool SpaceLeft(const size_t size, bool alligned = true) const = 0;

  virtual uint64_t GetWriteHead() const = 0;
  virtual uint64_t GetWriteTail() const = 0;

  inline uint8_t GetNumberOfReaders() const { return number_of_readers_; }

  // diagnostics (NOT threadsafe, meant to be called at the END)
  virtual uint64_t GetBytesWritten() const = 0;
  virtual uint64_t GetBytesRead() const = 0;
  virtual uint64_t GetZonesReset() const = 0;

protected:
  // const after initialisation
  const uint64_t min_zone_head_;
  const uint64_t max_zone_head_;
  const uint64_t zone_size_;
  const uint64_t lba_size_;
  const uint8_t number_of_readers_;
  // references
  SZD::SZDChannelFactory *channel_factory_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif
