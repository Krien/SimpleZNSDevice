#include "szd/datastructures/szd_circular_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

#include <cassert>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDCircularLog::SZDCircularLog(SZDChannelFactory *channel_factory,
                               const DeviceInfo &info,
                               const uint64_t min_zone_nr,
                               const uint64_t max_zone_nr,
                               const uint8_t number_of_readers)
    : SZDLog(channel_factory, info, min_zone_nr, max_zone_nr),
      number_of_readers_(number_of_readers), write_head_(min_zone_head_),
      write_tail_(min_zone_head_), zone_tail_(min_zone_nr * info.zone_cap),
      space_left_((max_zone_nr - min_zone_nr) * info.zone_cap * info.lba_size) {
  channel_factory_->Ref();
  read_channel_ = new SZD::SZDChannel *[number_of_readers_];
  for (uint8_t i = 0; i < number_of_readers_; i++) {
    channel_factory_->register_channel(&read_channel_[i], min_zone_nr,
                                       max_zone_nr);
  }
  channel_factory_->register_channel(&write_channel_, min_zone_nr, max_zone_nr);
  channel_factory_->register_channel(&reset_channel_, min_zone_nr, max_zone_nr);
}

SZDCircularLog::~SZDCircularLog() {
  if (read_channel_ != nullptr) {
    for (uint8_t i = 0; i < number_of_readers_; i++) {
      if (read_channel_[i]) {
        channel_factory_->unregister_channel(read_channel_[i]);
      }
    }
    delete[] read_channel_;
  }
  if (write_channel_ != nullptr) {
    channel_factory_->unregister_channel(write_channel_);
  }
  if (reset_channel_ != nullptr) {
    channel_factory_->unregister_channel(reset_channel_);
  }
  channel_factory_->Unref();
}

uint64_t SZDCircularLog::wrapped_addr(uint64_t addr) {
  // TODO: Should error
  if (addr < min_zone_head_) {
    SZD_LOG_ERROR("SZD: Circular log: wrapped_addr OOB\n");
    return 0;
  }
  addr -= min_zone_head_;
  addr %= (max_zone_head_ - min_zone_head_);
  addr += min_zone_head_;
  return addr;
}

SZDStatus SZDCircularLog::Append(const char *data, const size_t size,
                                 uint64_t *lbas_, bool alligned) {
  SZDStatus s;
  size_t alligned_size = alligned ? size : write_channel_->allign_size(size);
  if (szd_unlikely(!SpaceLeft(alligned_size))) {
    if (lbas_ != nullptr) {
      *lbas_ = 0;
    }
    SZD_LOG_ERROR("SZD: Circular log: Append: Out of space\n");
    return SZDStatus::IOError;
  }
  uint64_t lbas = alligned_size / lba_size_;
  // 2 phase
  uint64_t new_write_head = write_head_;
  if (new_write_head + lbas > max_zone_head_ && write_tail_ > min_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - new_write_head) * lba_size_;
    s = write_channel_->DirectAppend(&new_write_head, (void *)data,
                                     first_phase_size, alligned);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR(
          "SZD: Circular log: Apppend: Wraparound (end->begin) failed\n");
      return s;
    }
    // Wraparound
    new_write_head = min_zone_head_;
    s = write_channel_->DirectAppend(&new_write_head,
                                     (void *)(data + first_phase_size),
                                     size - first_phase_size, alligned);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Circular log: Apppend: Wraparound (begin) failed\n");
      return s;
    }
  } else {
    s = write_channel_->DirectAppend(&new_write_head, (void *)data, size,
                                     alligned);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Circular log: Apppend: Failed\n");
      return s;
    }
  }
  space_left_ -= lbas * lba_size_;
  write_head_ = new_write_head; // atomic write
  if (lbas_ != nullptr) {
    *lbas_ = lbas;
  }
  return s;
}

SZDStatus SZDCircularLog::Append(const std::string string, uint64_t *lbas,
                                 bool alligned) {
  return Append(string.data(), string.size(), lbas, alligned);
}

SZDStatus SZDCircularLog::Append(const SZDBuffer &buffer, size_t addr,
                                 size_t size, uint64_t *lbas_, bool alligned) {
  SZDStatus s;
  size_t alligned_size = alligned ? size : write_channel_->allign_size(size);
  if (szd_unlikely(!SpaceLeft(alligned_size))) {
    if (lbas_ != nullptr) {
      *lbas_ = 0;
    }
    SZD_LOG_ERROR("SZD: Circular log: Append (buffered): No space\n");
    return SZDStatus::IOError;
  }
  uint64_t lbas = alligned_size / lba_size_;
  // 2 phase
  uint64_t new_write_head = write_head_.load();
  if (new_write_head + lbas > max_zone_head_ && write_tail_ > min_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - new_write_head) * lba_size_;
    s = write_channel_->FlushBufferSection(&new_write_head, buffer, addr,
                                           first_phase_size);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Circular log: Append (buffered) wraparound "
                    "(end->begin): Failed\n");
      return s;
    }
    // Wraparound
    new_write_head = min_zone_head_;
    s = write_channel_->FlushBufferSection(&new_write_head, buffer,
                                           addr + first_phase_size,
                                           size - first_phase_size, alligned);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR(
          "SZD: Circular log: Append (buffered) wraparound (begin): Failed\n");
      return s;
    }
  } else {
    s = write_channel_->FlushBufferSection(&new_write_head, buffer, addr, size,
                                           alligned);
    if (s != SZDStatus::Success) {
      SZD_LOG_ERROR("SZD: Circular log: Append (buffered): Failed\n");
      return s;
    }
  }
  space_left_ -= lbas * lba_size_;
  write_head_ = new_write_head; // atomic write
  if (lbas_ != nullptr) {
    *lbas_ = lbas;
  }
  return s;
}

SZDStatus SZDCircularLog::Append(const SZDBuffer &buffer, uint64_t *lbas_) {
  SZDStatus s;
  size_t size = buffer.GetBufferSize();
  if (szd_unlikely(!SpaceLeft(size))) {
    if (lbas_ != nullptr) {
      *lbas_ = 0;
    }
    SZD_LOG_ERROR("SZD: Circular log: Append (buffered): No space\n");
    return SZDStatus::IOError;
  }
  uint64_t lbas = size / lba_size_;
  // 2 phase
  uint64_t new_write_head = write_head_;
  if (new_write_head + lbas > max_zone_head_ && write_tail_ > min_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - new_write_head) * lba_size_;
    s = write_channel_->FlushBufferSection(&new_write_head, buffer, 0,
                                           first_phase_size);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Circular log: Append (buffered) wraparound "
                    "(end->begin): Failed\n");
      return s;
    }
    // Wraparound
    new_write_head = min_zone_head_;
    s = write_channel_->FlushBufferSection(
        &new_write_head, buffer, first_phase_size, size - first_phase_size);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR(
          "SZD: Circular log: Append (buffered) wraparound (begin): Failed\n");
      return s;
    }
  } else {
    s = write_channel_->FlushBuffer(&new_write_head, buffer);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Circular log: Append (buffered): Failed\n");
      return s;
    }
  }
  space_left_ -= lbas * lba_size_;
  write_head_ = new_write_head; // atomic write
  if (lbas_ != nullptr) {
    *lbas_ = lbas;
  }
  return s;
}

bool SZDCircularLog::IsValidReadAddress(const uint64_t addr,
                                        const uint64_t lbas) const {
  uint64_t write_head_snapshot = write_head_;
  uint64_t write_tail_snapshot = write_tail_;
  if (write_head_snapshot >= write_tail_snapshot) {
    // [---------------WTvvvvWH--]
    if (szd_unlikely(addr < write_tail_snapshot ||
                     addr + lbas > write_head_snapshot)) {
      SZD_LOG_ERROR("SZD: Circular log: Read: addr out of valid centre, %lu "
                    "%lu %lu %lu \n",
                    write_tail_snapshot, addr, addr + lbas,
                    write_head_snapshot);
      return false;
    }
  } else {
    // [vvvvvvvvvvvvvvvvWH---WTvv]
    if (szd_unlikely(
            (addr > write_head_snapshot && addr < write_tail_snapshot) ||
            (addr + lbas > write_head_snapshot &&
             addr + lbas < write_tail_snapshot))) {
      SZD_LOG_ERROR("SZD: Circular log: Read: addr in invalid centree, %lu %lu "
                    "%lu %lu \n",
                    write_tail_snapshot, addr, addr + lbas,
                    write_head_snapshot);
      return false;
    }
  }
  return true;
}

SZDStatus SZDCircularLog::Read(uint64_t lba, char *data, uint64_t size,
                               bool alligned, uint8_t reader) {
  // Wraparound
  if (lba > max_zone_head_ || reader >= number_of_readers_) {
    return Read(lba - max_zone_head_ + min_zone_head_, data, size, alligned,
                reader);
  }
  // Set up proper size
  uint64_t alligned_size =
      alligned ? size : read_channel_[reader]->allign_size(size);
  uint64_t lbas = alligned_size / lba_size_;
  // Ensure data is written
  if (szd_unlikely(!IsValidReadAddress(lba, lbas))) {
    SZD_LOG_ERROR("SZD: Circular log: Read: invalid circular log address\n");
    return SZDStatus::InvalidArguments;
  }
  // 2 phase (wraparound) or 1 phase read needed?
  if (write_head_ < write_tail_ && lba + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - lba) * lba_size_;
    SZDStatus s = read_channel_[reader]->DirectRead(lba, data, first_phase_size,
                                                    alligned);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Circular log: Read: Error during wraparound\n");
      return s;
    }
    s = read_channel_[reader]->DirectRead(
        min_zone_head_, data + first_phase_size,
        alligned_size - first_phase_size, alligned);
    return s;
  } else {
    return read_channel_[reader]->DirectRead(lba, data, alligned_size,
                                             alligned);
  }
}

SZDStatus SZDCircularLog::Read(uint64_t lba, SZDBuffer *buffer, size_t addr,
                               size_t size, bool alligned, uint8_t reader) {
  // Wraparound
  if (lba > max_zone_head_ || reader >= number_of_readers_) {
    return Read(lba - max_zone_head_ + min_zone_head_, buffer, addr, size,
                alligned, reader);
  }
  // Set up proper size
  uint64_t alligned_size =
      alligned ? size : read_channel_[reader]->allign_size(size);
  uint64_t lbas = alligned_size / lba_size_;
  // Ensure data is written
  if (szd_unlikely(!IsValidReadAddress(lba, lbas))) {
    SZD_LOG_ERROR("SZD: Circular log: Read: Invalid arguments\n");
    return SZDStatus::InvalidArguments;
  }
  // 2 phase (wraparound) or 1 phase read needed?
  if (write_head_ < write_tail_ && lba + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - lba) * lba_size_;
    SZDStatus s = read_channel_[reader]->ReadIntoBuffer(
        lba, buffer, addr, first_phase_size, alligned);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Circular log: Read wraparound: Failed\n");
      return s;
    }
    s = read_channel_[reader]->ReadIntoBuffer(
        min_zone_head_, buffer, addr + first_phase_size,
        size - first_phase_size, alligned);
    return s;
  } else {
    return read_channel_[reader]->ReadIntoBuffer(lba, buffer, addr, size,
                                                 alligned);
  }
}

SZDStatus SZDCircularLog::Read(uint64_t lba, SZDBuffer *buffer, uint64_t size,
                               bool alligned, uint8_t reader) {
  return Read(lba, buffer, 0, size, alligned, reader);
}

SZDStatus SZDCircularLog::ConsumeTail(uint64_t begin_lba, uint64_t end_lba) {
  if (szd_unlikely(begin_lba != write_tail_ || end_lba < min_zone_head_)) {
    return SZDStatus::InvalidArguments;
  }
  // We want to force wrap apparently, we do not allow this.
  // Therefore we move the head back "past" the end. We wrap later on
  // manually.
  if (end_lba < begin_lba) {
    end_lba = end_lba - min_zone_head_ + max_zone_head_;
  }
  // Manual wrapping. First up to max and then up from start.
  if (szd_unlikely(end_lba > max_zone_head_)) {
    SZDStatus s = ConsumeTail(begin_lba, max_zone_head_);
    if (s != SZDStatus::Success) {
      SZD_LOG_ERROR("SZD: Circular log: Consume tail: Internal Error\n");
      return s;
    }
    end_lba = (end_lba - max_zone_head_) + min_zone_head_;
    begin_lba = min_zone_head_;
  }

  // Nothing to consume.
  uint64_t write_head_snapshot = write_head_;
  uint64_t write_tail_snapshot = write_tail_;
  if (szd_unlikely((write_tail_snapshot <= write_head_snapshot &&
                    end_lba > write_head_snapshot) ||
                   (write_tail_snapshot > write_head_snapshot &&
                    end_lba > write_head_snapshot &&
                    end_lba < write_tail_snapshot))) {
    SZD_LOG_ERROR("SZD: Circular log: Consume Tail: Invalid args\n");
    return SZDStatus::InvalidArguments;
  }

  // Reset zones.
  write_tail_snapshot = end_lba;
  uint64_t cur_zone = (write_tail_snapshot / zone_cap_) * zone_cap_;
  SZDStatus s;
  for (uint64_t slba = zone_tail_; slba != cur_zone; slba += zone_cap_) {
    if ((s = reset_channel_->ResetZone(slba)) != SZDStatus::Success) {
      SZD_LOG_ERROR("SZD: Circular log: Consume tail: Failed resetting zone\n");
      return s;
    }
    space_left_ += zone_cap_ * lba_size_;
  }
  zone_tail_ = cur_zone;

  // Wraparound of the actual tail.
  if (write_tail_snapshot == max_zone_head_) {
    zone_tail_ = write_tail_snapshot = min_zone_head_;
  }
  write_tail_ = write_tail_snapshot; // atomic write
  return SZDStatus::Success;
}

SZDStatus SZDCircularLog::ResetAll() {
  SZDStatus s;
  // We never own all zones for a circular log (I hope), therefore we need
  // individual resetting.
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_cap_) {
    s = reset_channel_->ResetZone(slba);
    if (s != SZDStatus::Success) {
      SZD_LOG_ERROR("SZD: Circular log: Reset all failed\n");
      return s;
    }
  }
  s = SZDStatus::Success;
  // Clean state
  write_head_ = zone_tail_ = write_tail_ = min_zone_head_;
  space_left_ = (max_zone_head_ - min_zone_head_) * lba_size_;
  return s;
}

void SZDCircularLog::RecalculateSpaceLeft() {
  uint64_t space;
  // head got ahead of tail :)
  if (write_head_ >= write_tail_) {
    // [vvvvTZ-WT----------WZ-WHvvvvv]
    uint64_t space_end = max_zone_head_ - write_head_;
    uint64_t space_begin = zone_tail_ - min_zone_head_;
    space = space_begin + space_end;
  } else {
    // [--WZ--WHvvvvvvvvTZ----WT---]
    space = zone_tail_ - write_head_;
  }
  space_left_ = space * lba_size_;
}

SZDStatus SZDCircularLog::RecoverPointers() {
  SZDStatus s;

  // Retrieve zone heads from the device
  std::vector<uint64_t> zone_heads;
  s = reset_channel_->ZoneHeads(min_zone_head_, max_zone_head_ - zone_cap_, &zone_heads);
  if (szd_unlikely(s != SZDStatus::Success)) {
    SZD_LOG_ERROR("SZD: Once log: Recover pointers\n");
    return s;
  }
  if (zone_heads.size() !=
      ((max_zone_head_ - min_zone_head_ - zone_cap_) / zone_cap_) + 1) {
    SZD_LOG_ERROR("SZD: Once log: ZoneHeads did not return all heads\n");
    return SZDStatus::Unknown;
  }

  uint64_t log_tail = min_zone_head_, log_head = min_zone_head_;
  // Scan for tail
  uint64_t slba;
  uint64_t zone_head = min_zone_head_, old_zone_head = min_zone_head_;
  for (slba = min_zone_head_; slba < max_zone_head_; slba += zone_cap_) {
    zone_head = zone_heads[(slba - min_zone_head_) / zone_cap_];
    old_zone_head = zone_head;
    // tail is at first zone that is not empty
    if (zone_head > slba) {
      log_tail = slba;
      // Head might be here if exactly 1 zone is filled...
      log_head = zone_head;
      break;
    }
  }
  // Scan for head
  for (; slba < max_zone_head_; slba += zone_cap_) {
    zone_head = zone_heads[(slba - min_zone_head_) / zone_cap_];
    // The first zone with a head more than 0 and less than max_zone, holds the
    // head of the manifest.
    if (zone_head > slba && zone_head < slba + zone_cap_) {
      log_head = zone_head;
      break;
    }
    // Or the last zone that is completely filled.
    if (zone_head < slba + zone_cap_ && slba > zone_cap_ &&
        old_zone_head > slba - zone_cap_) {
      log_head = slba;
      break;
    }
    old_zone_head = zone_head;
  }
  // if head < end and tail == 0, we need to be sure that the tail does not
  // start AFTER head.
  if (log_head > min_zone_head_ && log_tail == min_zone_head_) {
    for (slba += zone_cap_; slba < max_zone_head_; slba += zone_cap_) {
      zone_head = zone_heads[(slba - min_zone_head_) / zone_cap_];
      if (zone_head > slba) {
        log_tail = slba;
        break;
      }
    }
  }
  write_head_ = log_head;
  zone_tail_ = write_tail_ = log_tail;
  RecalculateSpaceLeft();
  return SZDStatus::Success;
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
