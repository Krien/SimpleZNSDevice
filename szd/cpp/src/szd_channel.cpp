#include "szd/szd_channel.hpp"
#include "szd/szd.h"
#include "szd/szd_status.hpp"

#include <cassert>
#include <cstring>
#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {

SZDChannel::SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
                       uint64_t min_lba, uint64_t max_lba,
                       bool keep_async_buffer, uint32_t queue_depth)
    : qpair_(qpair.release()), lba_size_(info.lba_size), zasl_(info.zasl),
      mdts_(info.mdts), zone_size_(info.zone_size), zone_cap_(info.zone_cap),
      min_lba_(min_lba), max_lba_(max_lba), can_access_all_(false),
      backed_memory_spill_(nullptr), lba_msb_(msb(info.lba_size)),
      queue_depth_(queue_depth), completion_(nullptr), async_buffer_(nullptr),
      keep_async_buffer_(keep_async_buffer), async_buffer_size_(0) {
  assert(min_lba_ <= max_lba_);
  // If true, there is a creeping bug not catched during debug? block all IO.
  if (min_lba_ > max_lba) {
    min_lba_ = max_lba_;
  }
  backed_memory_spill_ = szd_calloc(lba_size_, 1, lba_size_);
  // diagnostics
  bytes_written_.store(0);
  append_operations_counter_.store(0);
  bytes_read_.store(0);
  read_operations_.store(0);
  zones_reset_counter_.store(0);
  // Ensure that all diagnosed zones are
  zones_reset_.clear();
  for (size_t slba = min_lba; slba < max_lba_; slba += zone_size_) {
    zones_reset_.push_back(0);
    append_operations_.push_back(0);
  }
  completion_ = new Completion *[queue_depth_];
  async_buffer_ = (void **)(new char **[queue_depth_]);
  async_buffer_size_ = new size_t[queue_depth_];
  for (uint32_t i = 0; i < queue_depth_; i++) {
    async_buffer_[i] = nullptr;
    completion_[i] = nullptr;
    async_buffer_size_[i] = 0;
  }
}

SZDChannel::SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
                       bool keep_async_buffer, uint32_t queue_depth)
    : SZDChannel(std::move(qpair), info, 0, info.lba_cap, keep_async_buffer,
                 queue_depth) {
  can_access_all_ = true;
}

SZDChannel::~SZDChannel() {
  if (keep_async_buffer_ && async_buffer_ != nullptr) {
    for (uint32_t i = 0; i < queue_depth_; i++) {
      if (async_buffer_[i] != nullptr) {
        szd_free(async_buffer_[i]);
      }
    }
  }
  delete[] completion_;
  delete[] async_buffer_;
  delete[] async_buffer_size_;
  if (backed_memory_spill_ != nullptr) {
    szd_free(backed_memory_spill_);
    backed_memory_spill_ = nullptr;
  }
  if (qpair_ != nullptr) {
    szd_destroy_qpair(qpair_);
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
  uint64_t slba = (new_lba / zone_size_) * zone_size_;
  uint64_t zones_needed =
      (new_lba - slba + (alligned_size / lba_size_)) / zone_cap_;
  if (addr + alligned_size > available_size ||
      slba + zones_needed * zone_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
    return SZDStatus::InvalidArguments;
  }
  // Get buffer to flush
  void *cbuffer;
  SZDStatus s = SZDStatus::Success;
  if ((s = buffer.GetBuffer(&cbuffer)) != SZDStatus::Success) {
    return s;
  }
  // Diag
  uint64_t append_ops = 0;
  // We need two steps because it will not work with one buffer.
  if (alligned_size != size) {
    if (backed_memory_spill_ == nullptr) {
      return SZDStatus::IOError;
    }
    uint64_t postfix_size = lba_size_ - (alligned_size - size);
    alligned_size -= lba_size_;
    int rc = 0;
    if (alligned_size > 0) {
      rc = szd_append_with_diag(qpair_, &new_lba, (char *)cbuffer + addr,
                                alligned_size, &append_ops);
      bytes_written_.fetch_add(alligned_size, std::memory_order_relaxed);
    }
    memset((char *)backed_memory_spill_ + postfix_size, '\0',
           lba_size_ - postfix_size);
    memcpy(backed_memory_spill_, (char *)cbuffer + addr + alligned_size,
           postfix_size);
    rc = rc | szd_append_with_diag(qpair_, &new_lba, backed_memory_spill_,
                                   lba_size_, &append_ops);
    bytes_written_.fetch_add(lba_size_, std::memory_order_relaxed);
    s = FromStatus(rc);
  } else {
    s = FromStatus(szd_append_with_diag(
        qpair_, &new_lba, (char *)cbuffer + addr, alligned_size, &append_ops));
    bytes_written_.fetch_add(alligned_size, std::memory_order_relaxed);
  }
  // Diag
  uint64_t left = alligned_size / lba_size_;
  for (slba = TranslateLbaToPba(*lba); left != 0 && slba <= new_lba;
       slba += zone_size_) {
    uint64_t step = left > zone_cap_ ? zone_cap_ : left;
    append_operations_[(slba - min_lba_) / zone_size_] +=
        ((step * lba_size_ + zasl_ - 1) / zasl_);
    left -= step;
  }
  append_operations_counter_.fetch_add(append_ops, std::memory_order_relaxed);
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
  uint64_t slba = (lba / zone_size_) * zone_size_;
  uint64_t zones_needed =
      (lba - slba + (alligned_size / lba_size_)) / zone_cap_;
  if (addr + alligned_size > available_size ||
      slba + zones_needed * zone_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
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
    int rc = 0;
    if (alligned_size > 0) {
      uint64_t read_ops = 0;
      rc = szd_read_with_diag(qpair_, lba, (char *)cbuffer + addr,
                              alligned_size, &read_ops);
      bytes_read_.fetch_add(alligned_size, std::memory_order_relaxed);
      read_operations_.fetch_add(read_ops, std::memory_order_relaxed);
    }
    uint64_t read_ops = 0;
    rc = rc | szd_read_with_diag(qpair_, lba + alligned_size / lba_size_,
                                 (char *)backed_memory_spill_, lba_size_,
                                 &read_ops);
    bytes_read_.fetch_add(lba_size_, std::memory_order_relaxed);
    read_operations_.fetch_add(read_ops, std::memory_order_relaxed);
    s = FromStatus(rc);
    if (s == SZDStatus::Success) {
      memcpy((char *)cbuffer + addr + alligned_size, backed_memory_spill_,
             postfix_size);
    }
    return s;
  } else {
    uint64_t read_ops = 0;
    s = FromStatus(szd_read_with_diag(qpair_, lba, (char *)cbuffer + addr,
                                      alligned_size, &read_ops));
    bytes_read_.fetch_add(alligned_size, std::memory_order_relaxed);
    read_operations_.fetch_add(read_ops, std::memory_order_relaxed);
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
  uint64_t slba = (new_lba / zone_size_) * zone_size_;
  uint64_t zones_needed =
      (new_lba - slba + (alligned_size / lba_size_)) / zone_cap_;
  if (slba + zones_needed * zone_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
    printf("Invalid arguments for DirectAppend\n");
    return SZDStatus::InvalidArguments;
  }
  // Create temporary DMA buffer of ZASL size
  void *dma_buffer = szd_calloc(lba_size_, 1, zasl_);
  if (dma_buffer == nullptr) {
    printf("No DMA memory left\n");
    return SZDStatus::IOError;
  }
  // Write in steps of ZASL
  uint64_t begin = 0;
  uint64_t stepsize = zasl_;
  SZDStatus s;
  while (begin < size) {
    if (begin + zasl_ > alligned_size) {
      stepsize = alligned_size - begin;
      memset(dma_buffer, 0, zasl_);
      memcpy(dma_buffer, (char *)buffer + begin, size - begin);
    } else {
      stepsize = zasl_;
      memcpy(dma_buffer, (char *)buffer + begin, stepsize);
    }
    uint64_t append_ops = 0;
    uint64_t prev_lba = new_lba;
    s = FromStatus(szd_append_with_diag(qpair_, &new_lba, dma_buffer, stepsize,
                                        &append_ops));
    // Diag
    if ((prev_lba / zone_size_) * zone_size_ !=
        (new_lba / zone_size_) * zone_size_) {
      append_operations_[(prev_lba - min_lba_) / zone_size_] += 1;
    }
    append_operations_[(new_lba - min_lba_) / zone_size_] += 1;

    if (s != SZDStatus::Success) {
      printf("DirectWrite error \n");
      break;
    }
    begin += stepsize;
    // Diag
    bytes_written_.fetch_add(stepsize, std::memory_order_relaxed);
    append_operations_counter_.fetch_add(append_ops, std::memory_order_relaxed);
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
  uint64_t slba = (lba / zone_size_) * zone_size_;
  uint64_t zones_needed =
      (lba - slba + (alligned_size / lba_size_)) / zone_cap_;
  if (slba + zones_needed * zone_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
    printf("Directread invalid arguments \n");
    return SZDStatus::InvalidArguments;
  }
  // Create temporary DMA buffer to copy other DMA buffer data into.
  void *buffer_dma = szd_calloc(lba_size_, 1, mdts_);
  if (buffer_dma == nullptr) {
    printf("No DMA memory left for reading\n");
    return SZDStatus::IOError;
  }
  // Read in steps of MDTS
  uint64_t begin = 0;
  uint64_t lba_to_read = lba;
  slba = (lba_to_read / zone_size_) * zone_size_;
  uint64_t current_zone_end = slba + zone_cap_;
  uint64_t stepsize = mdts_;
  uint64_t alligned_step = mdts_;
  SZDStatus s;
  while (begin < size) {
    if (begin + mdts_ > alligned_size) {
      alligned_step = size - begin;
      stepsize = alligned_size - begin;
    } else {
      stepsize = mdts_;
      alligned_step = begin + mdts_ > size ? size - begin : mdts_;
    }
    uint64_t read_ops = 0;
    s = FromStatus(szd_read_with_diag(qpair_, lba_to_read, buffer_dma, stepsize,
                                      &read_ops));
    read_operations_.fetch_add(read_ops, std::memory_order_relaxed);
    bytes_read_.fetch_add(stepsize, std::memory_order_relaxed);
    if (s == SZDStatus::Success) {
      memcpy((char *)buffer + begin, buffer_dma, alligned_step);
    } else {
      printf("Error in DirectRead\n");
      break;
    }
    begin += stepsize;
    lba_to_read += stepsize / lba_size_;
    if (lba_to_read >= current_zone_end) {
      slba += zone_size_;
      lba_to_read = slba + lba_to_read - current_zone_end;
      current_zone_end = slba + zone_cap_;
    }
  }
  // Remove temporary buffer.
  szd_free(buffer_dma);
  return s;
}

SZDStatus SZDChannel::AsyncAppend(uint64_t *lba, void *buffer,
                                  const uint64_t size, uint32_t writer) {
  if (writer > queue_depth_) {
    return SZDStatus::InvalidArguments;
  }
  // Translate lba
  uint64_t new_lba = TranslateLbaToPba(*lba);
  // Allign
  uint64_t alligned_size = allign_size(size);
  // Check if in bounds...
  uint64_t slba = (new_lba / zone_size_) * zone_size_;
  uint64_t zones_needed =
      (new_lba - slba + (alligned_size / lba_size_)) / zone_cap_;
  if (zones_needed > 1) {
    printf("Invalid arguments for DirectAsyncAppend\n");
    return SZDStatus::InvalidArguments;
  }
  // Create temporary DMA buffer and copy normal buffer to DMA.
  if (keep_async_buffer_ && async_buffer_size_[writer] < alligned_size) {
    if (async_buffer_[writer] != nullptr) {
      szd_free(async_buffer_[writer]);
    }
    async_buffer_[writer] = szd_calloc(lba_size_, 1, alligned_size);
    async_buffer_size_[writer] = alligned_size;
  } else if (!keep_async_buffer_) {
    async_buffer_[writer] = szd_calloc(lba_size_, 1, alligned_size);
  } else {
    memset(async_buffer_[writer], 0, async_buffer_size_[writer]);
  }
  if (async_buffer_[writer] == nullptr) {
    printf("No DMA memory left\n");
    return SZDStatus::IOError;
  }
  memcpy(async_buffer_[writer], buffer, size);
  uint64_t append_ops = 0;
  if (completion_[writer] != nullptr) {
    delete completion_[writer];
  }
  completion_[writer] = new Completion;
  SZDStatus s = FromStatus(szd_append_async_with_diag(
      qpair_, &new_lba, async_buffer_[writer], alligned_size, &append_ops,
      completion_[writer]));
  // Diag
  uint64_t left = alligned_size / lba_size_;
  for (slba = TranslateLbaToPba(*lba); left != 0 && slba <= new_lba;
       slba += zone_size_) {
    uint64_t step = left > zone_cap_ ? zone_cap_ : left;
    append_operations_[(slba - min_lba_) / zone_size_] +=
        ((step * lba_size_ + zasl_ - 1) / zasl_);
    left -= step;
  }
  bytes_written_.fetch_add(alligned_size, std::memory_order_relaxed);
  append_operations_counter_.fetch_add(append_ops, std::memory_order_relaxed);

  *lba = TranslatePbaToLba(new_lba);
  return s;
}

bool SZDChannel::PollOnce(uint32_t writer) {
  if (writer > queue_depth_) {
    return false;
  }
  if (completion_[writer] == nullptr) {
    return true;
  }
  szd_poll_once(qpair_, completion_[writer]);
  bool status = completion_[writer]->done || completion_[writer]->err != 0;
  if (status) {
    // Remove temporary buffer.
    if (!keep_async_buffer_) {
      szd_free(async_buffer_[writer]);
      async_buffer_[writer] = nullptr;
    }
    delete completion_[writer];
    completion_[writer] = nullptr;
  }
  return status;
}

bool SZDChannel::PollOnce(uint32_t *writer) {
  szd_poll_once_raw(qpair_);
  for (uint32_t i = 0; i < queue_depth_; i++) {
    if (completion_[i] == nullptr) {
      *writer = i;
      return true;
    }
    if (completion_[i]->err != 0x0 || completion_[i]->done) {
      bool status = completion_[i]->done || completion_[i]->err != 0;
      if (status) {
        // Remove temporary buffer.
        if (!keep_async_buffer_) {
          szd_free(async_buffer_[i]);
          async_buffer_[i] = nullptr;
        }
        delete completion_[i];
        completion_[i] = nullptr;
      }
      *writer = i;
      return true;
    }
  }
  return false;
}

SZDStatus SZDChannel::Sync() {
  SZDStatus s = SZDStatus::Success;
  if (completion_ == nullptr) {
    return s;
  }
  // poll
  for (uint32_t i = 0; i < queue_depth_; i++) {
    if (completion_[i] == nullptr) {
      continue;
    }
    s = FromStatus(szd_poll_async(qpair_, completion_[i]));
    if (s != SZDStatus::Success) {
      break;
    }
    // Remove temporary buffer.
    if (!keep_async_buffer_) {
      szd_free(async_buffer_[i]);
      async_buffer_[i] = nullptr;
    }
    delete completion_[i];
    completion_[i] = nullptr;
  }
  return s;
}

SZDStatus SZDChannel::ResetZone(uint64_t slba) {
  slba = TranslateLbaToPba(slba);
  if (slba < min_lba_ || slba > max_lba_) {
    printf("Reset out of range %lu  - %lu -  %lu\n", min_lba_, slba, max_lba_);
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_reset(qpair_, slba));
  zones_reset_counter_.fetch_add(1, std::memory_order_relaxed);
  zones_reset_[(slba - min_lba_) / zone_size_]++;
  return s;
}

SZDStatus SZDChannel::ResetAllZones() {
  SZDStatus s = SZDStatus::Success;
  // There is no partial reset, reset the partial zones one by one.
  if (!can_access_all_) {
    for (uint64_t slba = min_lba_; slba != max_lba_; slba += zone_size_) {
      if ((s = FromStatus(szd_reset(qpair_, slba))) != SZDStatus::Success) {
        return s;
      }
      zones_reset_counter_.fetch_add(1, std::memory_order_relaxed);
      zones_reset_[(slba - min_lba_) / zone_size_]++;
    }
    return s;
  } else {
    s = FromStatus(szd_reset_all(qpair_));
    zones_reset_counter_.fetch_add((max_lba_ - min_lba_) / zone_size_,
                                   std::memory_order_relaxed);
    for (uint64_t &z : zones_reset_) {
      z++;
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
