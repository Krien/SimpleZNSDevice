#include "szd_channel.h"
#include "szd.h"
#include "szd_utils.h"

#include <string>

namespace SimpleZNSDeviceNamespace {

SZDChannel::SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
                       uint64_t min_lba, uint64_t max_lba)
    : qpair_(qpair.release()), lba_size_(info.lba_size),
      zone_size_(info.zone_size), min_lba_(min_lba), max_lba_(max_lba),
      can_access_all_(true), backed_memory_(nullptr), backed_memory_size_(0) {
  assert(min_lba_ <= max_lba_);
  // If true, there is a creeping bug not catched during debug? block all IO.
  if (min_lba_ > max_lba) {
    min_lba_ = max_lba_;
  }
}
SZDChannel::SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info)
    : SZDChannel(std::move(qpair), info, 0, info.lba_cap) {
  can_access_all_ = false;
}

SZDChannel::~SZDChannel() {
  if (backed_memory_size_ > 0 && backed_memory_ != nullptr) {
    z_free(qpair_, backed_memory_);
    backed_memory_ = nullptr;
  }
  if (qpair_ != nullptr) {
    z_destroy_qpair(qpair_);
  }
}

SZDStatus SZDChannel::GetBuffer(void **buffer) {
  if (backed_memory_size_ == 0 || backed_memory_ == nullptr) {
    return SZDStatus::InvalidArguments;
  }
  *buffer = backed_memory_;
  return SZDStatus::Success;
}

SZDStatus SZDChannel::FreeBuffer() {
  if (backed_memory_size_ == 0) {
    return SZDStatus::InvalidArguments;
  }
  z_free(qpair_, backed_memory_);
  backed_memory_ = nullptr;
  return SZDStatus::Success;
}

SZDStatus SZDChannel::ReserveBuffer(uint64_t size) {
  SZDStatus s = SZDStatus::Success;
  uint64_t alligned_size = allign_size(size);
  // nothing to do (if you want to reduce memory of the buffer, instead clean
  // first)
  if (backed_memory_size_ > 0 && backed_memory_size_ > alligned_size) {
    return s;
  }
  // realloc, we need more space
  if (backed_memory_size_ > 0) {
    if ((s = FreeBuffer()) != SZDStatus::Success) {
      return s;
    }
  }
  backed_memory_ = z_calloc(qpair_, alligned_size, sizeof(char));
  if (backed_memory_ == nullptr) {
    return SZDStatus::IOError;
  }
  backed_memory_size_ = alligned_size;
  return SZDStatus::Success;
}

SZDStatus SZDChannel::Append(void *data, size_t size, size_t *write_head) {
  if (*write_head + size > backed_memory_size_) {
    return SZDStatus::InvalidArguments;
  }
  memcpy((char *)backed_memory_ + *write_head, data, size);
  *write_head += size;
  return SZDStatus::Success;
}

SZDStatus SZDChannel::Write(void *data, size_t size, size_t addr) {
  if (addr + size > backed_memory_size_) {
    return SZDStatus::InvalidArguments;
  }
  memcpy((char *)backed_memory_ + addr, data, size);
  return SZDStatus::Success;
}

SZDStatus SZDChannel::FlushBuffer(uint64_t *lba) {
  if (backed_memory_size_ == 0 ||
      *lba + backed_memory_size_ / lba_size_ > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  return FromStatus(z_append(qpair_, lba, backed_memory_, backed_memory_size_));
}

SZDStatus SZDChannel::ReadIntoBuffer(uint64_t lba, size_t size, size_t addr,
                                     bool alligned) {
  uint64_t alligned_size = alligned ? size : allign_size(size);
  if (addr + alligned_size > backed_memory_size_ ||
      lba + size / lba_size_ > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  return FromStatus(z_read(qpair_, lba, backed_memory_, alligned_size));
}

std::string SZDChannel::DebugBufferString() {
  return std::string((const char *)backed_memory_, backed_memory_size_);
}

SZDStatus SZDChannel::DirectAppend(uint64_t *lba, void *buffer,
                                   const uint64_t size, bool alligned) const {
  uint64_t alligned_size = alligned ? size : allign_size(size);
  if (*lba + alligned_size / lba_size_ > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  void *dma_buffer = z_calloc(qpair_, 1, alligned_size);
  if (dma_buffer == nullptr) {
    return SZDStatus::IOError;
  }
  memcpy(dma_buffer, buffer, size);
  SZDStatus s = FromStatus(z_append(qpair_, lba, dma_buffer, alligned_size));
  z_free(qpair_, dma_buffer);
  return s;
}

SZDStatus SZDChannel::DirectRead(void *buffer, uint64_t lba, uint64_t size,
                                 bool alligned) const {
  uint64_t alligned_size = alligned ? size : allign_size(size);
  void *buffer_dma = z_calloc(qpair_, 1, alligned_size);
  if (buffer_dma == nullptr) {
    return SZDStatus::IOError;
  }
  SZDStatus s = FromStatus(z_read(qpair_, lba, buffer_dma, alligned_size));
  if (s == SZDStatus::Success) {
    memcpy(buffer, buffer_dma, size);
  }
  z_free(qpair_, buffer_dma);
  return s;
}

SZDStatus SZDChannel::ResetZone(uint64_t slba) const {
  if (slba < min_lba_ || slba > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  return FromStatus(SZD::z_reset(qpair_, slba, false));
}

SZDStatus SZDChannel::ResetAllZones() const {
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
    return FromStatus(SZD::z_reset(qpair_, 0, true));
  }
}

} // namespace SimpleZNSDeviceNamespace
