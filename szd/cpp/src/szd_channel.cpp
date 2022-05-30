#include "szd/szd_channel.hpp"
#include "szd/szd.h"
#include "szd/szd_status.hpp"

#include <cassert>
#include <cstring>
#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {

Zone *SZDChannel::GetZone(QPair *qpair, uint64_t slba) {
  uint64_t wp = 0;
  uint64_t zone_cap = 0;
  szd_get_zone_head(qpair, slba, &wp);
  szd_get_zone_cap(qpair, slba, &zone_cap);
  Zone *z = new Zone{.slba = slba, .wp = wp, .zone_cap = zone_cap};
  // printf("New zone of %lu %lu %lu\n", slba, wp, zone_cap);
  return z;
}

SZDChannel::SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
                       uint64_t min_lba, uint64_t max_lba)
    : qpair_(qpair.release()), lba_size_(info.lba_size),
      zone_size_(info.zone_size), zone_cap_(info.zone_cap), min_lba_(min_lba),
      max_lba_(max_lba), can_access_all_(false), backed_memory_spill_(nullptr),
      lba_msb_(msb(info.lba_size)), bytes_written_(0), append_operations_(0),
      bytes_read_(0), read_operations_(0), zones_reset_(0) {
  assert(min_lba_ <= max_lba_);
  // If true, there is a creeping bug not catched during debug? block all IO.
  if (min_lba_ > max_lba) {
    min_lba_ = max_lba_;
  }
  // Reserve a small buffer
  backed_memory_spill_ = szd_calloc(lba_size_, 1, lba_size_);
  // Get zone states
  for (uint64_t slba = min_lba; slba <= max_lba; slba += zone_size_) {
    zones_.push_back(GetZone(qpair_, slba));
  }
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
  for (size_t i = 0; i < zones_.size(); i++) {
    delete zones_[i];
  }
}

uint64_t SZDChannel::TranslateLbaToPba(uint64_t lba) {
  // determine lba by going to actual zone offset and readding offset.
  uint64_t slba = (lba / zone_cap_) * zone_size_;
  uint64_t slba_offset = lba % zone_cap_;
  // printf("translate lba to pba %lu %lu %lu\n", lba, slba, slba_offset);
  return slba + slba_offset;
}

uint64_t SZDChannel::TranslatePbaToLba(uint64_t lba) {
  // determine lba by going to fake zone offset and readding offset.
  uint64_t slba = (lba / zone_size_) * zone_cap_;
  uint64_t slba_offset = lba % zone_size_;
  // printf("translate pba to lba %lu %lu %lu\n", lba, slba, slba_offset);
  return slba + slba_offset;
}

SZDStatus SZDChannel::FlushBufferSection(uint64_t *lba, const SZDBuffer &buffer,
                                         uint64_t addr, uint64_t size,
                                         bool alligned) {
  // Translate lba
  uint64_t new_lba = TranslateLbaToPba(*lba);
  // Allign
  uint64_t alligned_size = alligned ? size : allign_size(size);
  uint64_t available_size = buffer.GetBufferSize();
  // Check if in bounds...
  uint64_t zone_begin = (new_lba / zone_size_) * zone_size_;
  uint64_t zone_index = (zone_begin - min_lba_) / zone_size_;
  uint64_t zone_offset = new_lba - zone_begin;
  if (zone_begin < min_lba_ || zone_begin > max_lba_ ||
      (alligned && size != allign_size(size)) ||
      addr + alligned_size > available_size) {
    return SZDStatus::InvalidArguments;
  }
  // Get zone
  Zone *current_zone_ = zones_[zone_index];
  // Trying to read in protected memory
  if (zone_offset > current_zone_->zone_cap) {
    return SZDStatus::InvalidArguments;
  }
  // Get buffer to flush
  void *cbuffer;
  SZDStatus s = SZDStatus::Success;
  if ((s = buffer.GetBuffer(&cbuffer)) != SZDStatus::Success) {
    return s;
  }
  // We need two steps because it will not work with one buffer.
  if (alligned_size != size) {
    if (backed_memory_spill_ == nullptr) {
      return SZDStatus::IOError;
    }
    uint64_t postfix_size = lba_size_ - (alligned_size - size);
    alligned_size -= lba_size_;
    if (alligned_size > 0) {
      s = FlushBufferSection(lba, buffer, addr, alligned_size, true);
      new_lba = TranslateLbaToPba(*lba);
    }
    if (s != SZDStatus::Success) {
      return s;
    }
    memset((char *)backed_memory_spill_ + postfix_size, '\0',
           lba_size_ - postfix_size);
    memcpy(backed_memory_spill_, (char *)cbuffer + addr + alligned_size,
           postfix_size);
    int rc = szd_append_with_diag(qpair_, &new_lba, backed_memory_spill_,
                                  lba_size_, &append_operations_);
    bytes_written_ += lba_size_;
    s = FromStatus(rc);
  } else {
    // Append in steps that make sense for the zone
    uint64_t left = alligned_size;
    uint64_t written = 0;
    uint64_t mock_lba = new_lba;
    SZDStatus s;
    while (left > 0) {
      uint64_t avail =
          (current_zone_->slba + current_zone_->zone_cap - current_zone_->wp) *
          lba_size_;
      uint64_t step = left < avail ? left : avail;
      s = FromStatus(szd_append_with_diag(qpair_, &mock_lba,
                                          (char *)cbuffer + addr + written,
                                          step, &append_operations_));
      bytes_written_ += step;
      new_lba += step / lba_size_;
      if (s != SZDStatus::Success) {
        break;
      }
      current_zone_->wp += step / lba_size_;
      written += step;
      left -= step;
      if (left == 0) {
        break;
      }
      if (new_lba > max_lba_) {
        s = SZDStatus::InvalidArguments;
        break;
      }
      zone_index++;
      current_zone_ = zones_[zone_index];
      // This means that we are going into an already partially filled zone?
      if (current_zone_->wp > current_zone_->slba) {
        s = SZDStatus::InvalidArguments;
        break;
      }
      mock_lba = new_lba = current_zone_->slba;
    }
  }
  *lba = TranslatePbaToLba(new_lba);
  return s;
}

SZDStatus SZDChannel::FlushBuffer(uint64_t *lba, const SZDBuffer &buffer) {
  return FlushBufferSection(lba, buffer, 0, buffer.GetBufferSize(), true);
}

SZDStatus SZDChannel::ReadIntoBuffer(uint64_t lba, SZDBuffer *buffer,
                                     size_t addr, size_t size, bool alligned) {
  lba = TranslateLbaToPba(lba);
  // Allign
  uint64_t alligned_size = alligned ? size : allign_size(size);
  uint64_t available_size = buffer->GetBufferSize();
  // Check if in bounds...
  uint64_t zone_begin = (lba / zone_size_) * zone_size_;
  uint64_t zone_index = (zone_begin - min_lba_) / zone_size_;
  uint64_t zone_offset = lba - zone_begin;
  if (zone_begin < min_lba_ || zone_begin > max_lba_ ||
      (alligned && size != allign_size(size)) ||
      addr + alligned_size > available_size) {
    return SZDStatus::InvalidArguments;
  }
  // Get zone
  Zone *current_zone_ = zones_[zone_index];
  // Trying to read in protected memory
  if (zone_offset > current_zone_->zone_cap) {
    return SZDStatus::InvalidArguments;
  }
  // Get buffer to read into
  void *cbuffer;
  SZDStatus s = SZDStatus::Success;
  if ((s = buffer->GetBuffer(&cbuffer)) != SZDStatus::Success) {
    return s;
  }
  // We need two steps because it will not work with one buffer.
  if (alligned_size != size) {
    if (backed_memory_spill_ == nullptr) {
      return SZDStatus::IOError;
    }
    uint64_t postfix_size = lba_size_ - (alligned_size - size);
    alligned_size -= lba_size_;
    // Read in steps that make sense for the zone
    uint64_t left = alligned_size;
    uint64_t read = 0;
    SZDStatus s;
    while (left > 0) {
      uint64_t avail =
          (current_zone_->slba + current_zone_->zone_cap - lba) * lba_size_;
      uint64_t step = left < avail ? left : avail;
      s = FromStatus(szd_read_with_diag(
          qpair_, lba, (char *)cbuffer + addr + read, step, &read_operations_));
      bytes_read_ += step;
      if (s != SZDStatus::Success) {
        break;
      }
      read += step;
      left -= step;
      lba += step / lba_size_;
      if (lba < current_zone_->slba + current_zone_->zone_cap) {
        break;
      }
      lba = current_zone_->slba + zone_size_;
      if (lba > max_lba_) {
        s = SZDStatus::InvalidArguments;
        break;
      }
      zone_index++;
      current_zone_ = zones_[zone_index];
    }
    if (s == SZDStatus::Success) {
      s = FromStatus(szd_read_with_diag(qpair_, lba,
                                        (char *)backed_memory_spill_, lba_size_,
                                        &read_operations_));
      bytes_read_ += lba_size_;
    }
    if (s == SZDStatus::Success) {
      memcpy((char *)cbuffer + addr + alligned_size, backed_memory_spill_,
             postfix_size);
    }
    return s;
  } else {
    // Read in steps that make sense for the zone
    uint64_t left = alligned_size;
    uint64_t read = 0;
    SZDStatus s;
    while (left > 0) {
      uint64_t avail =
          (current_zone_->slba + current_zone_->zone_cap - lba) * lba_size_;
      uint64_t step = left < avail ? left : avail;
      s = FromStatus(szd_read_with_diag(
          qpair_, lba, (char *)cbuffer + addr + read, step, &read_operations_));
      bytes_read_ += step;
      if (s != SZDStatus::Success) {
        printf("tyf op\n");
        break;
      }
      read += step;
      left -= step;
      lba = current_zone_->slba + zone_size_;
      if (lba > max_lba_) {
        s = SZDStatus::InvalidArguments;
        break;
      }
      zone_index++;
      current_zone_ = zones_[zone_index];
    }
    return s;
  }
}

SZDStatus SZDChannel::DirectAppend(uint64_t *lba, void *buffer,
                                   const uint64_t size, bool alligned) {
  // Translate lba
  uint64_t new_lba = TranslateLbaToPba(*lba);
  // Allign
  uint64_t alligned_size = alligned ? size : allign_size(size);
  // Check if in bounds...
  uint64_t zone_begin = (new_lba / zone_size_) * zone_size_;
  uint64_t zone_index = (zone_begin - min_lba_) / zone_size_;
  uint64_t zone_offset = new_lba - zone_begin;
  if (zone_begin < min_lba_ || zone_begin > max_lba_ ||
      (alligned && size != allign_size(size))) {
    return SZDStatus::InvalidArguments;
  }
  // Get zone
  Zone *current_zone_ = zones_[zone_index];
  // Trying to write in protected memory. The pointers used to write are
  // corrupt. Better to abort.
  if (zone_offset > current_zone_->zone_cap) {
    return SZDStatus::InvalidArguments;
  }
  // Create temporary DMA buffer and copy normal buffer to DMA.
  void *dma_buffer = szd_calloc(lba_size_, 1, alligned_size);
  if (dma_buffer == nullptr) {
    return SZDStatus::IOError;
  }
  memcpy(dma_buffer, buffer, size);
  // Append in steps that make sense for the zone
  uint64_t left = alligned_size;
  uint64_t written = 0;
  uint64_t mock_lba = new_lba;
  SZDStatus s;
  while (left > 0) {
    uint64_t avail =
        (current_zone_->slba + current_zone_->zone_cap - current_zone_->wp) *
        lba_size_;
    uint64_t step = left < avail ? left : avail;
    s = FromStatus(szd_append_with_diag(qpair_, &mock_lba,
                                        (char *)dma_buffer + written, step,
                                        &append_operations_));
    bytes_written_ += step;
    new_lba += step / lba_size_;
    if (s != SZDStatus::Success) {
      break;
    }
    current_zone_->wp += step / lba_size_;
    written += step;
    left -= step;
    if (left == 0) {
      break;
    }
    if (new_lba > max_lba_) {
      s = SZDStatus::InvalidArguments;
      break;
    }
    zone_index++;
    current_zone_ = zones_[zone_index];
    // This means that we are going into an already partially filled zone?
    if (current_zone_->wp > current_zone_->slba) {
      printf("INVALID %lu \n", current_zone_->slba);
      s = SZDStatus::InvalidArguments;
      break;
    }
    mock_lba = new_lba = current_zone_->slba;
  }
  // Remove temporary buffer.
  szd_free(dma_buffer);
  *lba = TranslatePbaToLba(new_lba);
  return s;
}

SZDStatus SZDChannel::DirectRead(uint64_t lba, void *buffer, uint64_t size,
                                 bool alligned) {
  lba = TranslateLbaToPba(lba);
  // Allign
  uint64_t alligned_size = alligned ? size : allign_size(size);
  // Check if in bounds...
  uint64_t zone_begin = (lba / zone_size_) * zone_size_;
  uint64_t zone_index = (zone_begin - min_lba_) / zone_size_;
  uint64_t zone_offset = lba - zone_begin;
  if (zone_begin < min_lba_ || zone_begin > max_lba_ ||
      (alligned && size != allign_size(size))) {
    return SZDStatus::InvalidArguments;
  }
  // Get zone
  Zone *current_zone_ = zones_[zone_index];
  // Trying to read in protected memory
  if (zone_offset > current_zone_->zone_cap) {
    return SZDStatus::InvalidArguments;
  }
  // Create temporary DMA buffer to copy other DMA buffer data into.
  void *buffer_dma = szd_calloc(lba_size_, 1, alligned_size);
  if (buffer_dma == NULL) {
    return SZDStatus::IOError;
  }
  // Read in steps that make sense for the zone
  uint64_t left = alligned_size;
  uint64_t read = 0;
  (void)buffer;
  SZDStatus s = SZDStatus::Success;
  while (left > 0) {
    uint64_t avail =
        (current_zone_->slba + current_zone_->zone_cap - lba) * lba_size_;
    uint64_t step = left < avail ? left : avail;
    s = FromStatus(szd_read_with_diag(qpair_, lba, (char *)buffer_dma + read,
                                      step, &read_operations_));
    bytes_read_ += step;
    read += step;
    left -= step;
    if (s != SZDStatus::Success || left == 0) {
      break;
    }
    lba = current_zone_->slba + zone_size_;
    if (lba > max_lba_) {
      s = SZDStatus::InvalidArguments;
      break;
    }
    zone_index++;
    current_zone_ = zones_[zone_index];
  }
  memcpy(buffer, buffer_dma, size);
  // Remove temporary buffer.
  szd_free(buffer_dma);
  return s;
}

SZDStatus SZDChannel::ResetZone(uint64_t slba) {
  slba = TranslateLbaToPba(slba);
  if (slba < min_lba_ || slba > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_reset(qpair_, slba));
  uint64_t zone_index = (slba - min_lba_) / zone_size_;
  zones_[zone_index]->wp = zones_[zone_index]->slba;
  zones_reset_++;
  return s;
}

SZDStatus SZDChannel::ResetAllZones() {
  SZDStatus s = SZDStatus::Success;
  // There is no partial reset, reset the partial zones one by one.
  if (!can_access_all_) {
    int zones_i_ = 0;
    for (uint64_t slba = min_lba_; slba != max_lba_; slba += zone_size_) {
      if ((s = FromStatus(szd_reset(qpair_, slba))) != SZDStatus::Success) {
        return s;
      }
      zones_[zones_i_]->wp = zones_[zones_i_]->slba;
      zones_reset_++;
      zones_i_++;
    }
    return s;
  } else {
    s = FromStatus(szd_reset_all(qpair_));
    zones_reset_ += (max_lba_ - min_lba_) / zone_size_;
    int zones_i_ = 0;
    for (uint64_t slba = min_lba_; slba != max_lba_; slba += zone_size_) {
      zones_[zones_i_]->wp = zones_[zones_i_]->slba;
      zones_i_++;
    }
    return s;
  }
}

SZDStatus SZDChannel::ZoneHead(uint64_t slba, uint64_t *zone_head) {
  slba = TranslateLbaToPba(slba);
  if (slba < min_lba_ || slba > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_get_zone_head(qpair_, slba, zone_head));
  *zone_head = TranslatePbaToLba(*zone_head);
  return s;
}

SZDStatus SZDChannel::FinishZone(uint64_t slba) {
  slba = TranslateLbaToPba(slba);
  if (slba < min_lba_ || slba > max_lba_) {
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_finish_zone(qpair_, slba));
  return s;
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
