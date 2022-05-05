#include "szd/szd_channel.hpp"
#include "szd/szd.h"
#include "szd/szd_status.hpp"

#include <cassert>
#include <cstring>
#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {

SZDChannel::SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
                       uint64_t min_lba, uint64_t max_lba)
    : qpair_(qpair.release()), lba_size_(info.lba_size),
      zone_size_(info.zone_size), min_lba_(min_lba), max_lba_(max_lba),
      can_access_all_(false), backed_memory_spill_(nullptr),
      lba_msb_(msb(info.lba_size)) {
  assert(min_lba_ <= max_lba_);
  // If true, there is a creeping bug not catched during debug? block all IO.
  if (min_lba_ > max_lba) {
    min_lba_ = max_lba_;
  }
  backed_memory_spill_ = szd_calloc(lba_size_, 1, lba_size_);
}

SZDChannel::SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info)
    : SZDChannel(std::move(qpair), info, 0, info.lba_cap) {
  can_access_all_ = true;
}

SZDChannel::~SZDChannel() {
  if (backed_memory_spill_ != nullptr) {
    szd_free(backed_memory_spill_);
    backed_memory_spill_ = nullptr;
  }
  if (qpair_ != nullptr) {
    szd_destroy_qpair(qpair_);
  }
}

SZDStatus SZDChannel::FlushBufferSection(uint64_t *lba, const SZDBuffer &buffer,
                                         uint64_t addr, uint64_t size,
                                         bool alligned) {
  uint64_t alligned_size = alligned ? size : allign_size(size);
  uint64_t available_size = buffer.GetBufferSize();
  if (addr + alligned_size > available_size ||
      *lba + alligned_size / lba_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
    return SZDStatus::InvalidArguments;
  }
  void *cbuffer;
  SZDStatus s = SZDStatus::Success;
  if ((s = buffer.GetBuffer(&cbuffer)) != SZDStatus::Success) {
    return s;
  }
  if (alligned_size != size) {
    if (backed_memory_spill_ == nullptr) {
      return SZDStatus::IOError;
    }
    uint64_t postfix_size = lba_size_ - (alligned_size - size);
    alligned_size -= lba_size_;
    int rc = 0;
    if (alligned_size > 0) {
      mutex_.lock();
      rc = szd_append(qpair_, lba, (char *)cbuffer + addr, alligned_size);
      mutex_.unlock();
    }
    memset((char *)backed_memory_spill_ + postfix_size, '\0',
           lba_size_ - postfix_size);
    memcpy(backed_memory_spill_, (char *)cbuffer + addr + alligned_size,
           postfix_size);
    mutex_.lock();
    rc = rc | szd_append(qpair_, lba, backed_memory_spill_, lba_size_);
    mutex_.unlock();
    return FromStatus(rc);
  } else {
    mutex_.lock();
    s = FromStatus(
        szd_append(qpair_, lba, (char *)cbuffer + addr, alligned_size));
    mutex_.unlock();
    return s;
  }
}

SZDStatus SZDChannel::FlushBuffer(uint64_t *lba, const SZDBuffer &buffer) {
  return FlushBufferSection(lba, buffer, 0, buffer.GetBufferSize(), true);
}

SZDStatus SZDChannel::ReadIntoBuffer(uint64_t lba, SZDBuffer *buffer,
                                     size_t addr, size_t size, bool alligned) {
  uint64_t alligned_size = alligned ? size : allign_size(size);
  uint64_t available_size = buffer->GetBufferSize();
  if (addr + alligned_size > available_size ||
      lba + alligned_size / lba_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
    return SZDStatus::InvalidArguments;
  }
  void *cbuffer;
  SZDStatus s = SZDStatus::Success;
  if ((s = buffer->GetBuffer(&cbuffer)) != SZDStatus::Success) {
    return s;
  }
  if (alligned_size != size) {
    if (backed_memory_spill_ == nullptr) {
      return SZDStatus::IOError;
    }
    uint64_t postfix_size = lba_size_ - (alligned_size - size);
    alligned_size -= lba_size_;
    int rc = 0;
    if (alligned_size > 0) {
      mutex_.lock();
      rc = szd_read(qpair_, lba, (char *)cbuffer + addr, alligned_size);
      mutex_.unlock();
    }
    mutex_.lock();
    rc = rc | szd_read(qpair_, lba + alligned_size / lba_size_,
                       (char *)backed_memory_spill_, lba_size_);
    mutex_.unlock();
    s = FromStatus(rc);
    if (s == SZDStatus::Success) {
      memcpy((char *)cbuffer + addr + alligned_size, backed_memory_spill_,
             postfix_size);
    }
    return s;
  } else {
    mutex_.lock();
    s = FromStatus(
        szd_read(qpair_, lba, (char *)cbuffer + addr, alligned_size));
    mutex_.unlock();
    return s;
  }
}

SZDStatus SZDChannel::DirectAppend(uint64_t *lba, void *buffer,
                                   const uint64_t size, bool alligned) {
  uint64_t alligned_size = alligned ? size : allign_size(size);
  if (*lba + alligned_size / lba_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
    return SZDStatus::InvalidArguments;
  }
  void *dma_buffer = szd_calloc(lba_size_, 1, alligned_size);
  if (dma_buffer == nullptr) {
    return SZDStatus::IOError;
  }
  memcpy(dma_buffer, buffer, size);
  mutex_.lock();
  SZDStatus s = FromStatus(szd_append(qpair_, lba, dma_buffer, alligned_size));
  mutex_.unlock();
  szd_free(dma_buffer);
  return s;
}

SZDStatus SZDChannel::DirectRead(uint64_t lba, void *buffer, uint64_t size,
                                 bool alligned) {
  uint64_t alligned_size = alligned ? size : allign_size(size);
  if (lba + alligned_size / lba_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
    return SZDStatus::InvalidArguments;
  }
  void *buffer_dma = szd_calloc(lba_size_, 1, alligned_size);
  if (buffer_dma == nullptr) {
    return SZDStatus::IOError;
  }
  mutex_.lock();
  SZDStatus s = FromStatus(szd_read(qpair_, lba, buffer_dma, alligned_size));
  mutex_.unlock();
  if (s == SZDStatus::Success) {
    memcpy(buffer, buffer_dma, size);
  }
  szd_free(buffer_dma);
  return s;
}

SZDStatus SZDChannel::ResetZone(uint64_t slba) {
  if (slba < min_lba_ || slba > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  mutex_.lock();
  SZDStatus s = FromStatus(szd_reset(qpair_, slba));
  mutex_.unlock();
  return s;
}

SZDStatus SZDChannel::ResetAllZones() {
  SZDStatus s = SZDStatus::Success;
  // There is no partial reset, reset the partial zones one by one.
  if (!can_access_all_) {
    for (uint64_t slba = min_lba_; slba != max_lba_; slba += zone_size_) {
      if ((s = ResetZone(slba)) != SZDStatus::Success) {
        return s;
      }
    }
    return s;
  } else {
    s = FromStatus(szd_reset_all(qpair_));
    return s;
  }
}

SZDStatus SZDChannel::ZoneHead(uint64_t slba, uint64_t *zone_head) {
  if (slba < min_lba_ || slba > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  mutex_.lock();
  SZDStatus s = FromStatus(szd_get_zone_head(qpair_, slba, zone_head));
  mutex_.unlock();
  return s;
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
