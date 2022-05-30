#include "szd/datastructures/szd_once_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

#include <cassert>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDOnceLog::SZDOnceLog(SZDChannelFactory *channel_factory,
                       const DeviceInfo &info, const uint64_t min_zone_nr,
                       const uint64_t max_zone_nr)
    : SZDLog(channel_factory, info, min_zone_nr, max_zone_nr, 1),
      block_range_((max_zone_nr - min_zone_nr) * info.zone_cap),
      space_left_(block_range_ * info.lba_size), write_head_(min_zone_head_) {
  channel_factory_->Ref();
  channel_factory_->register_channel(&channel_, min_zone_nr, max_zone_nr);
}

SZDOnceLog::~SZDOnceLog() {
  if (channel_ != nullptr) {
    channel_factory_->unregister_channel(channel_);
  }
  channel_factory_->Unref();
}

SZDStatus SZDOnceLog::Append(const char *data, const size_t size,
                             uint64_t *lbas, bool alligned) {
  SZDStatus s;
  if (!SpaceLeft(size, alligned)) {
    if (lbas != nullptr) {
      *lbas = 0;
    }
    return SZDStatus::IOError;
  }
  uint64_t blocks = channel_->allign_size(size) / lba_size_;
  s = channel_->DirectAppend(&write_head_, (void *)data, size, alligned);
  if (lbas != nullptr) {
    *lbas = blocks;
  }
  space_left_ -= blocks * lba_size_;
  return s;
}

SZDStatus SZDOnceLog::Append(const std::string string, uint64_t *lbas,
                             bool alligned) {
  return Append(string.data(), string.size(), lbas, alligned);
}

SZDStatus SZDOnceLog::Append(const SZDBuffer &buffer, size_t addr, size_t size,
                             uint64_t *lbas, bool alligned) {
  SZDStatus s;
  if (!SpaceLeft(size, alligned)) {
    if (lbas != nullptr) {
      *lbas = 0;
    }
    return SZDStatus::IOError;
  }
  uint64_t blocks = channel_->allign_size(size) / lba_size_;
  s = channel_->FlushBufferSection(&write_head_, buffer, addr, size, alligned);
  if (lbas != nullptr) {
    *lbas = blocks;
  }
  space_left_ -= blocks * lba_size_;
  return s;
}

SZDStatus SZDOnceLog::Append(const SZDBuffer &buffer, uint64_t *lbas) {
  SZDStatus s;
  size_t size = buffer.GetBufferSize();
  if (!SpaceLeft(size)) {
    if (lbas != nullptr) {
      *lbas = 0;
    }
    return SZDStatus::IOError;
  }
  uint64_t blocks = channel_->allign_size(size) / lba_size_;
  s = channel_->FlushBuffer(&write_head_, buffer);
  if (lbas != nullptr) {
    *lbas = blocks;
  }
  space_left_ -= blocks * lba_size_;
  return s;
}

bool SZDOnceLog::IsValidAddress(uint64_t lba, uint64_t lbas) {
  return lba >= min_zone_head_ && lba + lbas <= write_head_;
}

SZDStatus SZDOnceLog::Read(uint64_t lba, char *data, uint64_t size,
                           bool alligned, uint8_t /*reader*/) {
  if (!IsValidAddress(lba, channel_->allign_size(size) / lba_size_)) {
    return SZDStatus::InvalidArguments;
  }
  return channel_->DirectRead(lba, data, size, alligned);
}

SZDStatus SZDOnceLog::Read(uint64_t lba, SZDBuffer *buffer, uint64_t size,
                           bool alligned, uint8_t /*reader*/) {
  if (!IsValidAddress(lba, channel_->allign_size(size) / lba_size_)) {
    return SZDStatus::InvalidArguments;
  }
  return channel_->ReadIntoBuffer(lba, buffer, 0, size, alligned);
}

SZDStatus SZDOnceLog::Read(uint64_t lba, SZDBuffer *buffer, size_t addr,
                           size_t size, bool alligned, uint8_t /*reader*/) {
  if (!IsValidAddress(lba, channel_->allign_size(size) / lba_size_)) {
    return SZDStatus::InvalidArguments;
  }
  return channel_->ReadIntoBuffer(lba, buffer, addr, size, alligned);
}

SZDStatus SZDOnceLog::ResetAll() {
  SZDStatus s;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_cap_) {
    s = channel_->ResetZone(slba);
    if (s != SZDStatus::Success) {
      return s;
    }
  }
  s = SZDStatus::Success;
  write_head_ = min_zone_head_;
  space_left_ = block_range_ * lba_size_;
  return s;
}

SZDStatus SZDOnceLog::RecoverPointers() {
  SZDStatus s;
  uint64_t write_head = min_zone_head_;
  uint64_t zone_head = min_zone_head_;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_cap_) {
    s = channel_->ZoneHead(slba, &zone_head);
    if (s != SZDStatus::Success) {
      return s;
    }
    // head is at last zone that is not empty
    if (zone_head > slba) {
      write_head = zone_head;
    }
    // end has been reached.
    if (write_head > min_zone_head_ && zone_head == slba) {
      break;
    }
  }
  write_head_ = write_head;
  space_left_ = (max_zone_head_ - write_head_) * lba_size_;
  return SZDStatus::Success;
}

SZDStatus SZDOnceLog::MarkInactive() {
  SZDStatus s = SZDStatus::Success;
  if ((write_head_ / zone_size_) * zone_size_ != write_head_) {
    s = channel_->FinishZone((write_head_ / zone_size_) * zone_size_);
  }
  return s;
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
