#include "szd/datastructures/szd_once_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

#include <cassert>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDOnceLog::SZDOnceLog(SZDChannelFactory *channel_factory,
                       const DeviceInfo &info, const uint64_t min_zone_nr,
                       const uint64_t max_zone_nr,
                       const uint8_t number_of_writers,
                       SZDChannel **write_channel)
    : SZDLog(channel_factory, info, min_zone_nr, max_zone_nr),
      number_of_writers_(number_of_writers),
      block_range_((max_zone_nr - min_zone_nr) * info.zone_cap),
      space_left_(block_range_ * info.lba_size), write_head_(0),
      zasl_(info.zasl), write_channel_(write_channel),
      write_channels_owned_(false) {
  write_head_ = min_zone_head_;
  channel_factory_->Ref();
  if (write_channel_ == nullptr) {
    write_channel_ = new SZD::SZDChannel *[number_of_writers_];
    for (uint8_t i = 0; i < number_of_writers_; i++) {
      channel_factory_->register_channel(&write_channel_[i], min_zone_nr,
                                         max_zone_nr);
    }
  } else {
    write_channels_owned_ = true;
  }
  qpair_depth_ = write_channel_[0]->GetQueueDepth();

#ifdef EstimatedQueue
  // Create free queue
  for (uint8_t i = 0; i < number_of_writers_; i++) {
    frees.push_back(i);
  }
#endif
  channel_factory_->register_channel(&read_channel_, min_zone_nr, max_zone_nr);
}

SZDOnceLog::~SZDOnceLog() {
  Sync();
  if (!write_channels_owned_ && write_channel_ != nullptr) {
    for (uint8_t i = 0; i < number_of_writers_; i++) {
      if (write_channel_[i]) {
        channel_factory_->unregister_channel(write_channel_[i]);
      }
    }
    delete[] write_channel_;
  }
  if (read_channel_ != nullptr) {
    channel_factory_->unregister_channel(read_channel_);
  }
  channel_factory_->Unref();
}

SZDStatus SZDOnceLog::Append(const char *data, const size_t size,
                             uint64_t *lbas, bool alligned) {
  SZDStatus s;
  if (szd_unlikely(!SpaceLeft(size, alligned))) {
    if (lbas != nullptr) {
      *lbas = 0;
    }
    SZD_LOG_ERROR("SZD: Once log: Append: No space left\n");
    return SZDStatus::IOError;
  }
  uint64_t write_head_old = write_head_;
  s = write_channel_[0]->DirectAppend(&write_head_, (void *)data, size,
                                      alligned);
  uint64_t blocks = write_head_ - write_head_old;
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
  if (szd_unlikely(!SpaceLeft(size, alligned))) {
    if (lbas != nullptr) {
      *lbas = 0;
    }
    SZD_LOG_ERROR("SZD: Once log: Append: No space left\n");
    return SZDStatus::IOError;
  }
  uint64_t write_head_old = write_head_;
  s = write_channel_[0]->FlushBufferSection(&write_head_, buffer, addr, size,
                                            alligned);
  uint64_t blocks = write_head_ - write_head_old;
  if (lbas != nullptr) {
    *lbas = blocks;
  }
  space_left_ -= blocks * lba_size_;
  return s;
}

SZDStatus SZDOnceLog::Append(const SZDBuffer &buffer, uint64_t *lbas) {
  SZDStatus s;
  size_t size = buffer.GetBufferSize();
  if (szd_unlikely(!SpaceLeft(size))) {
    if (lbas != nullptr) {
      *lbas = 0;
    }
    SZD_LOG_ERROR("SZD: Once log: Append: No space left\n");
    return SZDStatus::IOError;
  }
  uint64_t write_head_old = write_head_;
  s = write_channel_[0]->FlushBuffer(&write_head_, buffer);
  uint64_t blocks = write_head_ - write_head_old;
  if (lbas != nullptr) {
    *lbas = blocks;
  }
  space_left_ -= blocks * lba_size_;
  return s;
}

SZDStatus SZDOnceLog::AsyncAppend(const char *data, const size_t size,
                                  uint64_t *lbas, bool alligned) {
  SZDStatus s;
  if (szd_unlikely(!SpaceLeft(size, alligned))) {
    if (lbas != nullptr) {
      *lbas = 0;
    }
    SZD_LOG_ERROR("SZD: Once log: Async Append: No space left\n");
    return SZDStatus::IOError;
  }
  uint64_t zone_end = (write_head_ / zone_cap_) * zone_cap_ + zone_cap_;
  // Check if possible to even do async
  uint64_t alligned_size = write_channel_[0]->allign_size(size);
  uint64_t blocks_needed = alligned_size / lba_size_;
  bool can_do_async = blocks_needed <= zasl_ / lba_size_ &&
                      write_head_ + blocks_needed < zone_end;
  // We need to sync all previous writes first, then do a direct append
  // Try to claim a channel
  uint32_t claimed_nr = 0;
  if (!can_do_async) {
    s = Sync();
    claimed_nr = 0;
    s = write_channel_[0]->DirectAppend(&write_head_, (void *)data, size,
                                        alligned);
  } else {
    // Spinlock-like, but over all queues one by one each time.
    uint64_t waiting = 0;
    while (true) {
      if (write_channel_[0]->FindFreeWriter(&claimed_nr)) {
        break;
      }
      waiting++;
    }
    s = write_channel_[0]->AsyncAppend(&write_head_, (void *)data, size,
                                       claimed_nr);
  }
  if (lbas != nullptr) {
    *lbas = blocks_needed;
  }
  space_left_ -= blocks_needed * lba_size_;
  return s;
}

SZDStatus SZDOnceLog::Sync() {
  SZDStatus s = SZDStatus::Success;
#ifdef EstimatedQueue
  frees.clear();
  waits.clear();
#endif
  for (uint8_t i = 0; i < number_of_writers_; i++) {
    s = write_channel_[i]->Sync();
#ifdef EstimatedQueue
    frees.push_back(i);
#endif
  }
  return s;
}

bool SZDOnceLog::IsValidAddress(uint64_t lba, uint64_t lbas) {
  return lba >= min_zone_head_ && lba + lbas <= write_head_;
}

SZDStatus SZDOnceLog::Read(uint64_t lba, char *data, uint64_t size,
                           bool alligned, uint8_t /*reader*/) {
  if (szd_unlikely(
          !IsValidAddress(lba, read_channel_->allign_size(size) / lba_size_))) {
    SZD_LOG_ERROR("SZD: Once log: Read: Invalid args\n");
    return SZDStatus::InvalidArguments;
  }
  return read_channel_->DirectRead(lba, data, size, alligned);
}

SZDStatus SZDOnceLog::Read(uint64_t lba, SZDBuffer *buffer, uint64_t size,
                           bool alligned, uint8_t /*reader*/) {
  if (szd_unlikely(
          !IsValidAddress(lba, read_channel_->allign_size(size) / lba_size_))) {
    SZD_LOG_ERROR("SZD: Once log: Read: Invalid args\n");
    return SZDStatus::InvalidArguments;
  }
  return read_channel_->ReadIntoBuffer(lba, buffer, 0, size, alligned);
}

SZDStatus SZDOnceLog::Read(uint64_t lba, SZDBuffer *buffer, size_t addr,
                           size_t size, bool alligned, uint8_t /*reader*/) {
  if (szd_unlikely(
          !IsValidAddress(lba, read_channel_->allign_size(size) / lba_size_))) {
    SZD_LOG_ERROR("SZD: Once log: Read: Invalid args\n");
    return SZDStatus::InvalidArguments;
  }
  return read_channel_->ReadIntoBuffer(lba, buffer, addr, size, alligned);
}

SZDStatus SZDOnceLog::ReadAll(std::string &out) {
  size_t size_needed = (GetWriteHead() - GetWriteTail()) * lba_size_;
  if (szd_unlikely(size_needed == 0)) {
    SZD_LOG_ERROR("SZD: Once log: ReadAll: Invalid args\n");
    return SZDStatus::Success;
  }
  char *dat = new char[size_needed + 1];
  SZDStatus s =
      read_channel_->DirectRead(GetWriteTail(), dat, size_needed, true);
  if (szd_unlikely(s != SZDStatus::Success)) {
    SZD_LOG_ERROR("SZD: Once log: ReadAll: Failed\n");
    return s;
  }
  out.append(dat, size_needed);
  delete[] dat;
  return s;
}

SZDStatus SZDOnceLog::ResetAll() {
  SZDStatus s;
  for (uint64_t slba = min_zone_head_;
       slba < max_zone_head_ && slba < write_head_; slba += zone_cap_) {
    s = read_channel_->ResetZone(slba);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Once log: ResetZone\n");
      return s;
    }
  }
  s = SZDStatus::Success;
  write_head_ = min_zone_head_;
  space_left_ = block_range_ * lba_size_;
  return s;
}

SZDStatus SZDOnceLog::ResetAllForce() { return read_channel_->ResetAllZones(); }

SZDStatus SZDOnceLog::RecoverPointers() {
  SZDStatus s;
  uint64_t write_head = min_zone_head_;
  uint64_t zone_head = min_zone_head_;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_cap_) {
    s = read_channel_->ZoneHead(slba, &zone_head);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Once log: Recover pointers\n");
      return s;
    }
    // head is at last zone that is not empty
    if (zone_head > slba) {
      write_head = zone_head;
    }
    // end has been reached.
    if (zone_head == slba) {
      break;
    }
  }
  write_head_ = write_head;
  space_left_ = (max_zone_head_ - write_head_) * lba_size_;
  return SZDStatus::Success;
}

SZDStatus SZDOnceLog::MarkInactive() {
  SZDStatus s = SZDStatus::Success;
  if ((write_head_ / zone_cap_) * zone_cap_ != write_head_) {
    uint64_t wasted_space =
        (write_head_ / zone_cap_) * zone_cap_ + zone_cap_ - write_head_;
    s = read_channel_->FinishZone((write_head_ / zone_cap_) * zone_cap_);
    space_left_ -= wasted_space * lba_size_;
    write_head_ += wasted_space;
  }
  return s;
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
