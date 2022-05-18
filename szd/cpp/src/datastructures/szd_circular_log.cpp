#include "szd/datastructures/szd_circular_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

#include <cassert>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDCircularLog::SZDCircularLog(SZDChannelFactory *channel_factory,
                               const DeviceInfo &info,
                               const uint64_t min_zone_nr,
                               const uint64_t max_zone_nr)
    : SZDLog(channel_factory, info, min_zone_nr, max_zone_nr),
      write_head_(min_zone_head_), write_tail_(min_zone_head_),
      zone_tail_(min_zone_nr * info.zone_size),
      space_left_((max_zone_nr - min_zone_nr) * info.zone_size *
                  info.lba_size) {
  channel_factory_->Ref();
  channel_factory_->register_channel(&read_channel_, min_zone_nr, max_zone_nr);
  channel_factory_->register_channel(&write_channel_, min_zone_nr, max_zone_nr);
}

SZDCircularLog::~SZDCircularLog() {
  if (read_channel_ != nullptr) {
    channel_factory_->unregister_channel(read_channel_);
  }
  if (write_channel_ != nullptr) {
    channel_factory_->unregister_channel(write_channel_);
  }
  channel_factory_->Unref();
}

SZDStatus SZDCircularLog::Append(const char *data, const size_t size,
                                 uint64_t *lbas_, bool alligned) {
  SZDStatus s;
  size_t alligned_size = alligned ? size : write_channel_->allign_size(size);
  if (!SpaceLeft(alligned_size)) {
    if (lbas_ != nullptr) {
      *lbas_ = 0;
    }
    return SZDStatus::IOError;
  }
  uint64_t lbas = alligned_size / lba_size_;
  // 2 phase
  uint64_t new_write_head = write_head_;
  if (new_write_head + lbas > max_zone_head_ && write_tail_ > min_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - new_write_head) * lba_size_;
    s = write_channel_->DirectAppend(&new_write_head, (void *)data,
                                     first_phase_size, alligned);
    if (s != SZDStatus::Success) {
      return s;
    }
    // Wraparound
    new_write_head = min_zone_head_;
    s = write_channel_->DirectAppend(&new_write_head,
                                     (void *)(data + first_phase_size),
                                     size - first_phase_size, alligned);
  } else {
    s = write_channel_->DirectAppend(&new_write_head, (void *)data, size,
                                     alligned);
  }
  space_left_ -= lbas * lba_size_;
  write_head_ = new_write_head; // atomic write
  if (s == SZDStatus::Success && lbas_ != nullptr) {
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
  if (!SpaceLeft(size)) {
    if (lbas_ != nullptr) {
      *lbas_ = 0;
    }
    return SZDStatus::IOError;
  }
  uint64_t lbas = alligned_size / lba_size_;
  // 2 phase
  uint64_t new_write_head = write_head_;
  if (new_write_head + lbas > max_zone_head_ && write_tail_ > min_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - new_write_head) * lba_size_;
    s = write_channel_->FlushBufferSection(&new_write_head, buffer, addr,
                                           first_phase_size);
    if (s != SZDStatus::Success) {
      return s;
    }
    // Wraparound
    new_write_head = min_zone_head_;
    s = write_channel_->FlushBufferSection(&new_write_head, buffer,
                                           addr + first_phase_size,
                                           size - first_phase_size, alligned);
  } else {
    s = write_channel_->FlushBufferSection(&new_write_head, buffer, addr, size,
                                           alligned);
  }
  space_left_ -= lbas * lba_size_;
  write_head_ = new_write_head; // atomic write
  if (s == SZDStatus::Success && lbas_ != nullptr) {
    *lbas_ = lbas;
  }
  return s;
}

SZDStatus SZDCircularLog::Append(const SZDBuffer &buffer, uint64_t *lbas_) {
  SZDStatus s;
  size_t size = buffer.GetBufferSize();
  if (!SpaceLeft(size)) {
    if (lbas_ != nullptr) {
      *lbas_ = 0;
    }
    return SZDStatus::IOError;
  }
  uint64_t lbas = size / lba_size_;
  // 2 phase
  uint64_t new_write_head = write_head_;
  if (new_write_head + lbas > max_zone_head_ && write_tail_ > min_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - new_write_head) * lba_size_;
    s = write_channel_->FlushBufferSection(&new_write_head, buffer, 0,
                                           first_phase_size);
    if (s != SZDStatus::Success) {
      return s;
    }
    // Wraparound
    new_write_head = min_zone_head_;
    s = write_channel_->FlushBufferSection(
        &new_write_head, buffer, first_phase_size, size - first_phase_size);
  } else {
    s = write_channel_->FlushBuffer(&new_write_head, buffer);
  }
  space_left_ -= lbas * lba_size_;
  write_head_ = new_write_head; // atomic write
  if (s == SZDStatus::Success && lbas_ != nullptr) {
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
    if (addr < write_tail_snapshot || addr + lbas > write_head_snapshot) {
      return false;
    }
  } else {
    // [vvvvvvvvvvvvvvvvWH---WTvv]
    if ((addr > write_head_snapshot && addr < write_tail_snapshot) ||
        (addr + lbas > write_head_snapshot &&
         addr + lbas < write_tail_snapshot)) {
      return false;
    }
  }
  return true;
}

SZDStatus SZDCircularLog::Read(uint64_t lba, char *data, uint64_t size,
                               bool alligned) {
  // Wraparound
  if (lba > max_zone_head_) {
    return Read(lba - max_zone_head_ + min_zone_head_, data, size, alligned);
  }
  // Set up proper size
  uint64_t alligned_size = alligned ? size : read_channel_->allign_size(size);
  uint64_t lbas = alligned_size / lba_size_;
  // Ensure data is written
  if (!IsValidReadAddress(lba, lbas)) {
    return SZDStatus::InvalidArguments;
  }
  // 2 phase (wraparound) or 1 phase read needed?
  if (write_head_ < write_tail_ && lba + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - lba) * lba_size_;
    SZDStatus s =
        read_channel_->DirectRead(lba, data, first_phase_size, alligned);
    if (s != SZDStatus::Success) {
      return s;
    }
    s = read_channel_->DirectRead(min_zone_head_, data,
                                  alligned_size - first_phase_size, alligned);
    return s;
  } else {
    return read_channel_->DirectRead(lba, data, alligned_size, alligned);
  }
}

SZDStatus SZDCircularLog::Read(uint64_t lba, SZDBuffer *buffer, size_t addr,
                               size_t size, bool alligned) {
  // Wraparound
  if (lba > max_zone_head_) {
    return Read(lba - max_zone_head_ + min_zone_head_, buffer, addr, size,
                alligned);
  }
  // Set up proper size
  uint64_t alligned_size = alligned ? size : read_channel_->allign_size(size);
  uint64_t lbas = alligned_size / lba_size_;
  // Ensure data is written
  if (!IsValidReadAddress(lba, lbas)) {
    return SZDStatus::InvalidArguments;
  }
  // 2 phase (wraparound) or 1 phase read needed?
  if (write_head_ < write_tail_ && lba + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - lba) * lba_size_;
    SZDStatus s = read_channel_->ReadIntoBuffer(lba, buffer, addr,
                                                first_phase_size, alligned);
    if (s != SZDStatus::Success) {
      return s;
    }
    s = read_channel_->ReadIntoBuffer(min_zone_head_, buffer,
                                      addr + first_phase_size,
                                      size - first_phase_size, alligned);
    return s;
  } else {
    return read_channel_->ReadIntoBuffer(lba, buffer, addr, size, alligned);
  }
}

SZDStatus SZDCircularLog::Read(uint64_t lba, SZDBuffer *buffer, uint64_t size,
                               bool alligned) {
  return Read(lba, buffer, 0, size, alligned);
}

SZDStatus SZDCircularLog::ConsumeTail(uint64_t begin_lba, uint64_t end_lba) {
  if (begin_lba != write_tail_ || end_lba < min_zone_head_) {
    return SZDStatus::InvalidArguments;
  }
  // We want to force wrap apparently, we do not allow this.
  // Therefore we move the head back "past" the end. We wrap later on
  // manually.
  if (end_lba < begin_lba) {
    end_lba = end_lba - min_zone_head_ + max_zone_head_;
  }
  // Manual wrapping. First up to max and then up from start.
  if (end_lba > max_zone_head_) {
    SZDStatus s = ConsumeTail(begin_lba, max_zone_head_);
    if (s != SZDStatus::Success) {
      return s;
    }
    end_lba = end_lba - max_zone_head_ + min_zone_head_;
    begin_lba = min_zone_head_;
  }

  // Nothing to consume.
  uint64_t write_head_snapshot = write_head_;
  uint64_t write_tail_snapshot = write_tail_;
  if ((write_tail_snapshot <= write_head_snapshot &&
       end_lba > write_head_snapshot) ||
      (write_tail_snapshot > write_head_snapshot &&
       end_lba > write_head_snapshot && end_lba < write_tail_snapshot)) {
    return SZDStatus::InvalidArguments;
  }

  // Reset zones.
  write_tail_snapshot = end_lba;
  uint64_t cur_zone = (write_tail_snapshot / zone_size_) * zone_size_;
  SZDStatus s;
  for (uint64_t slba = zone_tail_; slba != cur_zone; slba += zone_size_) {
    if ((s = write_channel_->ResetZone(slba)) != SZDStatus::Success) {
      return s;
    }
    space_left_ += zone_size_ * lba_size_;
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
       slba += zone_size_) {
    s = write_channel_->ResetZone(slba);
    if (s != SZDStatus::Success) {
      return s;
    }
  }
  s = SZDStatus::Success;
  // Clean state
  write_head_ = zone_tail_ = write_tail_ = min_zone_head_;
  space_left_ = (max_zone_head_ - write_head_) * lba_size_;
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
  uint64_t log_tail = min_zone_head_, log_head = min_zone_head_;
  // Scan for tail
  uint64_t slba;
  uint64_t zone_head = min_zone_head_, old_zone_head = min_zone_head_;
  for (slba = min_zone_head_; slba < max_zone_head_; slba += zone_size_) {
    if ((s = read_channel_->ZoneHead(slba, &zone_head)) != SZDStatus::Success) {
      return s;
    }
    old_zone_head = zone_head;
    // tail is at first zone that is not empty
    if (zone_head > slba) {
      log_tail = slba;
      break;
    }
  }
  // Scan for head
  for (; slba < max_zone_head_; slba += zone_size_) {
    if ((s = read_channel_->ZoneHead(slba, &zone_head)) != SZDStatus::Success) {
      return s;
    }
    // The first zone with a head more than 0 and less than max_zone, holds the
    // head of the manifest.
    if (zone_head > slba && zone_head < slba + zone_size_) {
      log_head = zone_head;
      break;
    }
    // Or the last zone that is completely filled.
    if (zone_head < slba + zone_size_ && slba > zone_size_ &&
        old_zone_head > slba - zone_size_) {
      log_head = slba;
      break;
    }
    old_zone_head = zone_head;
  }
  // if head < end and tail == 0, we need to be sure that the tail does not
  // start AFTER head.
  if (log_head > min_zone_head_ && log_tail == min_zone_head_) {
    for (slba += zone_size_; slba < max_zone_head_; slba += zone_size_) {
      if ((s = read_channel_->ZoneHead(slba, &zone_head)) !=
          SZDStatus::Success) {
        return s;
      }
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
