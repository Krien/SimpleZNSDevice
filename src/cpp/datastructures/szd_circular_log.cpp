#include "szd/cpp/datastructures/szd_circular_log.h"
#include "szd/cpp/szd_channel_factory.h"
#include "szd/szd.h"

#include <cassert>

namespace SimpleZNSDeviceNamespace {
SZDCircularLog::SZDCircularLog(SZDChannelFactory *channel_factory,
                               const DeviceInfo &info,
                               const uint64_t min_zone_head,
                               const uint64_t max_zone_head)
    : SZDLog(channel_factory, info, min_zone_head, max_zone_head),
      zone_head_(min_zone_head), zone_tail_(min_zone_head) {
  channel_factory_->Ref();
  channel_factory_->register_channel(&channel_, min_zone_head_, max_zone_head_);
}

SZDCircularLog::~SZDCircularLog() {
  if (channel_ != nullptr) {
    channel_factory_->unregister_channel(channel_);
  }
  channel_factory_->Unref();
}

SZDStatus SZDCircularLog::Append(const char *data, const size_t size,
                                 uint64_t *lbas_, bool alligned) {
  SZDStatus s;
  size_t alligned_size = alligned ? size : channel_->allign_size(size);
  if (!SpaceLeft(alligned_size)) {
    return SZDStatus::IOError;
  }
  uint64_t lbas = size / lba_size_;
  // 2 phase
  if (write_head_ < write_tail_ && write_head_ + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - write_head_) * lba_size_;
    s = channel_->DirectAppend(&write_head_, (void *)data, first_phase_size,
                               alligned);
    if (s != SZDStatus::Success) {
      return s;
    }
    // Wraparound
    write_head_ = min_zone_head_;
    s = channel_->DirectAppend(&write_head_, (void *)(data + first_phase_size),
                               first_phase_size, alligned);
    zone_head_ = (write_head_ / zone_size_) * zone_size_;
  } else {
    s = channel_->DirectAppend(&write_head_, (void *)data, size, alligned);
    zone_head_ = (write_head_ / zone_size_) * zone_size_;
  }
  if (lbas_ != nullptr) {
    *lbas_ = lbas;
  }
  return s;
}

SZDStatus SZDCircularLog::Append(const std::string string, uint64_t *lbas) {
  return Append(string.data(), string.size());
}

SZDStatus SZDCircularLog::Append(const SZDBuffer &buffer, size_t addr,
                                 size_t size, uint64_t *lbas_, bool alligned) {
  SZDStatus s;
  size_t alligned_size = alligned ? size : channel_->allign_size(size);
  if (!SpaceLeft(size)) {
    return SZDStatus::IOError;
  }
  uint64_t lbas = alligned_size / lba_size_;
  // 2 phase
  if (write_head_ < write_tail_ && write_head_ + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - write_head_) * lba_size_;
    s = channel_->FlushBufferSection(buffer, &write_head_, addr,
                                     first_phase_size);
    if (s != SZDStatus::Success) {
      return s;
    }
    // Wraparound
    write_head_ = min_zone_head_;
    s = channel_->FlushBufferSection(
        buffer, &write_head_, addr + first_phase_size,
        alligned_size - first_phase_size, alligned);
    zone_head_ = (write_head_ / zone_size_) * zone_size_;
  } else {
    s = channel_->FlushBufferSection(buffer, &write_head_, addr, size,
                                     alligned);
    zone_head_ = (write_head_ / zone_size_) * zone_size_;
  }
  if (lbas_ != nullptr) {
    *lbas_ = lbas;
  }
  return s;
}

SZDStatus SZDCircularLog::Append(const SZDBuffer &buffer, uint64_t *lbas_) {
  SZDStatus s;
  size_t size = buffer.GetBufferSize();
  if (!SpaceLeft(size)) {
    return SZDStatus::IOError;
  }
  uint64_t lbas = size / lba_size_;
  // 2 phase
  if (write_head_ < write_tail_ && write_head_ + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - write_head_) * lba_size_;
    s = channel_->FlushBufferSection(buffer, &write_head_, 0, first_phase_size);
    if (s != SZDStatus::Success) {
      return s;
    }
    // Wraparound
    write_head_ = min_zone_head_;
    s = channel_->FlushBufferSection(buffer, &write_head_, first_phase_size,
                                     size - first_phase_size);
    zone_head_ = (write_head_ / zone_size_) * zone_size_;
  } else {
    s = channel_->FlushBuffer(buffer, &write_head_);
    zone_head_ = (write_head_ / zone_size_) * zone_size_;
  }
  if (lbas_ != nullptr) {
    *lbas_ = lbas;
  }
  return s;
}

bool SZDCircularLog::IsValidReadAddress(const uint64_t addr,
                                        const uint64_t lbas) const {
  if (write_head_ >= write_tail_) {
    // [---------------WTvvvvWH--]
    if (addr < write_tail_ || addr + lbas > write_head_) {
      return false;
    }
  } else {
    // [vvvvvvvvvvvvvvvvWH---WTvv]
    if ((addr > write_head_ && addr < write_tail_) ||
        (addr + lbas > write_head_ && addr + lbas < write_tail_)) {
      return false;
    }
  }
  return true;
}

SZDStatus SZDCircularLog::Read(char *data, uint64_t lba, uint64_t size,
                               bool alligned) {
  uint64_t alligned_size = alligned ? size : channel_->allign_size(size);
  uint64_t lbas = alligned_size / lba_size_;
  if (!IsValidReadAddress(lba, lbas)) {
    return SZDStatus::InvalidArguments;
  }
  // 2 phase
  if (write_head_ < write_tail_ && lba + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - lba) * lba_size_;
    SZDStatus s = channel_->DirectRead(data, lba, first_phase_size, alligned);
    if (s != SZDStatus::Success) {
      return s;
    }
    s = channel_->DirectRead(data, min_zone_head_,
                             alligned_size - first_phase_size, alligned);
    return s;
  } else {
    return channel_->DirectRead(data, lba, alligned_size, alligned);
  }
}

SZDStatus SZDCircularLog::Read(SZDBuffer *buffer, size_t addr, size_t size,
                               uint64_t lba, bool alligned) {
  uint64_t alligned_size = alligned ? size : channel_->allign_size(size);
  uint64_t lbas = alligned_size / lba_size_;
  if (!IsValidReadAddress(lba, lbas)) {
    return SZDStatus::InvalidArguments;
  }
  // 2 phase
  if (write_head_ < write_tail_ && lba + lbas > max_zone_head_) {
    uint64_t first_phase_size = (max_zone_head_ - lba) * lba_size_;
    SZDStatus s =
        channel_->ReadIntoBuffer(buffer, lba, addr, first_phase_size, alligned);
    if (s != SZDStatus::Success) {
      return s;
    }
    s = channel_->ReadIntoBuffer(buffer, min_zone_head_,
                                 addr + first_phase_size,
                                 alligned_size - first_phase_size, alligned);
    return s;
  } else {
    return channel_->ReadIntoBuffer(buffer, lba, addr, size, alligned);
  }
}

SZDStatus SZDCircularLog::Read(SZDBuffer *buffer, uint64_t lba, uint64_t size,
                               bool alligned) {
  return Read(buffer, 0, size, lba, alligned);
}

SZDStatus SZDCircularLog::ConsumeTail(uint64_t begin_lba, uint64_t end_lba) {
  if (begin_lba != write_tail_ || end_lba > max_zone_head_ ||
      end_lba < min_zone_head_) {
    return SZDStatus::InvalidArguments;
  }
  write_tail_ = begin_lba < end_lba ? end_lba : max_zone_head_;
  uint64_t cur_zone = (write_tail_ / zone_size_) * zone_size_;
  SZDStatus s;
  for (uint64_t slba = zone_tail_; slba < cur_zone; slba += zone_size_) {
    if ((s = channel_->ResetZone(slba)) != SZDStatus::Success) {
      return s;
    }
  }
  zone_tail_ = cur_zone;
  // Wraparound
  if (zone_tail_ == max_zone_head_) {
    zone_tail_ = write_tail_ = min_zone_head_;
  }
  if (begin_lba > end_lba) {
    return ConsumeTail(min_zone_head_, end_lba);
  } else {
    return SZDStatus::Success;
  }
}

SZDStatus SZDCircularLog::ResetAll() {
  SZDStatus s;
  for (uint64_t slba = min_zone_head_; slba < max_zone_head_;
       slba += zone_size_) {
    s = channel_->ResetZone(slba);
    if (s != SZDStatus::Success) {
      return s;
    }
  }
  s = SZDStatus::Success;
  write_head_ = zone_head_ = zone_tail_ = write_tail_ = min_zone_head_;
  return s;
}

SZDStatus SZDCircularLog::RecoverPointers() {
  SZDStatus s;
  uint64_t log_tail = min_zone_head_, log_head = min_zone_head_;
  // Scan for tail
  uint64_t slba;
  uint64_t zone_head = min_zone_head_, old_zone_head = min_zone_head_;
  for (slba = min_zone_head_; slba < max_zone_head_; slba += zone_size_) {
    if ((s = channel_->ZoneHead(slba, &zone_head)) != SZDStatus::Success) {
      return s;
    }
    // tail is at first zone that is not empty
    if (zone_head > slba) {
      log_tail = slba;
      break;
    }
    old_zone_head = zone_head;
  }
  // Scan for head
  for (; slba < max_zone_head_; slba += zone_size_) {
    if ((s = channel_->ZoneHead(slba, &zone_head)) != SZDStatus::Success) {
      return s;
    }
    // The first zone with a head more than 0 and less than max_zone, holds the
    // head of the manifest.
    if (zone_head > slba && zone_head < slba + zone_size_) {
      log_head = zone_head;
      break;
    }
    // Or the last zone that is completely filled.
    if (zone_head < slba + zone_size_ && old_zone_head > slba - zone_size_) {
      if (zone_head == old_zone_head) {
        continue;
      }
      log_head = slba;
      break;
    }
    old_zone_head = zone_head;
  }
  // if head < end and tail == 0, we need to be sure that the tail does not
  // start AFTER head.
  if (log_head > 0 && log_tail == 0) {
    for (slba += zone_size_; slba < max_zone_head_; slba += zone_size_) {
      if ((s = channel_->ZoneHead(slba, &zone_head)) != SZDStatus::Success) {
        return s;
      }
      if (zone_head > slba) {
        log_tail = slba;
        break;
      }
    }
  }
  write_head_ = log_head;
  zone_head_ = (log_head / zone_size_) * zone_size_;
  zone_tail_ = write_tail_ = log_tail;
  return SZDStatus::Success;
}

} // namespace SimpleZNSDeviceNamespace
