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
      queue_depth_(queue_depth), outstanding_requests_(0), completion_(nullptr),
      async_buffer_(nullptr), keep_async_buffer_(keep_async_buffer),
      async_buffer_size_(0) {
  assert(min_lba_ <= max_lba_);
  // If true, there is a creeping bug not catched during debug? block all IO.
  if (min_lba_ > max_lba) {
    SZD_LOG_ERROR("SZD: Channel: Creation lba range incorrect %lu to %lu\n",
                  min_lba_, max_lba_);
    min_lba_ = max_lba_;
  }
  // Check if channel can access all blocks
  if (min_lba_ == 0 && max_lba_ == info.lba_cap) {
    can_access_all_ = true;
  }
  // Setup all buffers
  backed_memory_spill_ = szd_calloc(lba_size_, 1, lba_size_);
  completion_ = new Completion *[queue_depth_];
  async_buffer_ = (void **)(new char **[queue_depth_]);
  async_buffer_size_ = new size_t[queue_depth_];
  for (uint32_t i = 0; i < queue_depth_; i++) {
    async_buffer_[i] = nullptr;
    completion_[i] = nullptr;
    async_buffer_size_[i] = 0;
  }
  // setup diagnostic variables
#ifdef SZD_PERF_COUNTERS
  bytes_written_.store(0);
  append_operations_counter_.store(0);
  bytes_read_.store(0);
  read_operations_.store(0);
  zones_reset_counter_.store(0);
#endif
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  zones_reset_.clear();
  for (size_t slba = min_lba; slba < max_lba_; slba += zone_size_) {
    zones_reset_.push_back(0);
    append_operations_.push_back(0);
  }
#endif
}

SZDChannel::SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
                       bool keep_async_buffer, uint32_t queue_depth)
    : SZDChannel(std::move(qpair), info, 0, info.lba_cap, keep_async_buffer,
                 queue_depth) {}

SZDChannel::~SZDChannel() {
  if (outstanding_requests_ > 0) {
    SZD_LOG_ERROR("SZD Channel: channel with outstanding request destroyed");
  }
  // WARNING: we do not free completion buffers. You NEED to ensure all requests
  // completed before a delete. The destructor should not have to poll.
  if (keep_async_buffer_ && async_buffer_ != nullptr) {
    for (uint32_t i = 0; i < queue_depth_; i++) {
      if (async_buffer_[i] != nullptr) {
        szd_free(async_buffer_[i]);
      }
      if (completion_[i] != nullptr) {
        SZD_LOG_ERROR(
            "SZD Channel: queue %lu with outstanding request destroyed", i);
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
  return slba + slba_offset;
}

uint64_t SZDChannel::TranslatePbaToLba(uint64_t lba) {
  // determine lba by going to fake zone offset and readding offset.
  uint64_t slba = (lba / zone_size_) * zone_cap_;
  uint64_t slba_offset = lba % zone_size_;
  return slba + slba_offset;
}

SZDStatus SZDChannel::FlushBufferSection(uint64_t *lba, const SZDBuffer &buffer,
                                         uint64_t addr, uint64_t size,
                                         bool alligned) {
  // Translate lba
  uint64_t old_lba = TranslateLbaToPba(*lba);
  uint64_t new_lba = old_lba;
  // Allign
  uint64_t alligned_size = alligned ? size : allign_size(size);
  uint64_t available_size = buffer.GetBufferSize();
  // Check if in bounds...
  uint64_t slba = (new_lba / zone_size_) * zone_size_;
  uint64_t zones_needed =
      (new_lba - slba + (alligned_size / lba_size_)) / zone_cap_;
  if (szd_unlikely(addr + alligned_size > available_size || slba < min_lba_ ||
                   slba + zones_needed * zone_size_ > max_lba_ ||
                   (alligned && size != allign_size(size)))) {
    return SZDStatus::InvalidArguments;
  }
  // Get buffer to flush
  void *cbuffer;
  SZDStatus s = SZDStatus::Success;
  if ((s = buffer.GetBuffer(&cbuffer)) != SZDStatus::Success) {
    SZD_LOG_ERROR("SZD: Channel: FlushBufferSection: GetBuffer\n");
    return s;
  }
  // Diag
#ifdef SZD_PERF_COUNTERS
  uint64_t append_ops = 0;
#endif
  // We need two steps because it will not work with one buffer.
  if (alligned_size != size) {
    if (szd_unlikely(backed_memory_spill_ == nullptr)) {
      SZD_LOG_ERROR("SZD: Channel: FlushBufferSection: No spill buffer\n");
      return SZDStatus::MemoryError;
    }
    uint64_t postfix_size = lba_size_ - (alligned_size - size);
    uint64_t prefix_size = alligned_size - lba_size_;
    int rc = 0;
    if (prefix_size > 0) {
#ifdef SZD_PERF_COUNTERS
      rc = szd_append_with_diag(qpair_, &new_lba, (char *)cbuffer + addr,
                                prefix_size, &append_ops);
      bytes_written_.fetch_add(prefix_size, std::memory_order_relaxed);
#else
      rc = szd_append(qpair_, &new_lba, (char *)cbuffer + addr, prefix_size);
#endif
    }
    memset((char *)backed_memory_spill_ + postfix_size, 0,
           lba_size_ - postfix_size);
    memcpy(backed_memory_spill_, (char *)cbuffer + addr + prefix_size,
           postfix_size);
#ifdef SZD_PERF_COUNTERS
    rc = rc | szd_append_with_diag(qpair_, &new_lba, backed_memory_spill_,
                                   lba_size_, &append_ops);
    bytes_written_.fetch_add(lba_size_, std::memory_order_relaxed);
#else
    rc = rc | szd_append(qpair_, &new_lba, backed_memory_spill_, lba_size_);
#endif
    s = FromStatus(rc);
  } else {
#ifdef SZD_PERF_COUNTERS
    s = FromStatus(szd_append_with_diag(
        qpair_, &new_lba, (char *)cbuffer + addr, alligned_size, &append_ops));
    bytes_written_.fetch_add(alligned_size, std::memory_order_relaxed);
#else
    s = FromStatus(
        szd_append(qpair_, &new_lba, (char *)cbuffer + addr, alligned_size));
#endif
  }

  // Diag
#ifdef SZD_PERF_COUNTERS
  append_operations_counter_.fetch_add(append_ops, std::memory_order_relaxed);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  uint64_t left = alligned_size / lba_size_;
  uint64_t step = 0;
  for (slba = old_lba; left != 0 && slba <= new_lba; slba += step) {
    uint64_t step = left > zone_cap_ ? zone_cap_ : left;
    append_operations_[(slba - min_lba_) / zone_size_] +=
        ((step * lba_size_ + zasl_ - 1) / zasl_);
    left -= step;
  }
#endif
#endif

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
  if (addr + alligned_size > available_size || slba < min_lba_ ||
      slba + zones_needed * zone_size_ > max_lba_ ||
      (alligned && size != allign_size(size))) {
    return SZDStatus::InvalidArguments;
  }
  // Get buffer to read into
  void *cbuffer;
  SZDStatus s = SZDStatus::Success;
  if (szd_unlikely((s = buffer->GetBuffer(&cbuffer)) != SZDStatus::Success)) {
    SZD_LOG_ERROR("SZD: Channel: ReadIntoBuffer: GetBuffer\n");
    return s;
  }
  // We need two steps because it will not work with one buffer.
  if (alligned_size != size) {
    if (szd_unlikely(backed_memory_spill_ == nullptr)) {
      SZD_LOG_ERROR("SZD: Channel: ReadIntoBuffer: No spill buffer\n");
      return SZDStatus::MemoryError;
    }
    uint64_t postfix_size = lba_size_ - (alligned_size - size);
    alligned_size -= lba_size_;
    int rc = 0;
    if (alligned_size > 0) {
#ifdef SZD_PERF_COUNTERS
      uint64_t read_ops = 0;
      rc = szd_read_with_diag(qpair_, lba, (char *)cbuffer + addr,
                              alligned_size, &read_ops);
      bytes_read_.fetch_add(alligned_size, std::memory_order_relaxed);
      read_operations_.fetch_add(read_ops, std::memory_order_relaxed);
#else
      rc = szd_read(qpair_, lba, (char *)cbuffer + addr, alligned_size);
#endif
    }
#ifdef SZD_PERF_COUNTERS
    uint64_t read_ops = 0;
    rc = rc | szd_read_with_diag(qpair_, lba + alligned_size / lba_size_,
                                 (char *)backed_memory_spill_, lba_size_,
                                 &read_ops);
    bytes_read_.fetch_add(lba_size_, std::memory_order_relaxed);
    read_operations_.fetch_add(read_ops, std::memory_order_relaxed);
#else
    rc = rc | szd_read(qpair_, lba + alligned_size / lba_size_,
                       (char *)backed_memory_spill_, lba_size_);
#endif
    s = FromStatus(rc);
    if (s == SZDStatus::Success) {
      memcpy((char *)cbuffer + addr + alligned_size, backed_memory_spill_,
             postfix_size);
    }
  } else {
#ifdef SZD_PERF_COUNTERS
    uint64_t read_ops = 0;
    s = FromStatus(szd_read_with_diag(qpair_, lba, (char *)cbuffer + addr,
                                      alligned_size, &read_ops));
    bytes_read_.fetch_add(alligned_size, std::memory_order_relaxed);
    read_operations_.fetch_add(read_ops, std::memory_order_relaxed);
#else
    s = FromStatus(
        szd_read(qpair_, lba, (char *)cbuffer + addr, alligned_size));
#endif
  }
  return s;
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
  if (szd_unlikely(slba < min_lba_ ||
                   slba + zones_needed * zone_size_ > max_lba_ ||
                   (alligned && size != allign_size(size)))) {
    SZD_LOG_ERROR("SZD: Channel: DirectAppend: OOB\n");
    return SZDStatus::InvalidArguments;
  }
  // Create temporary DMA buffer of maximum ZASL size
  size_t dma_buffer_size = zasl_ > alligned_size ? alligned_size : zasl_;
  void *dma_buffer = szd_calloc(lba_size_, dma_buffer_size, 1);
  if (szd_unlikely(dma_buffer == nullptr)) {
    SZD_LOG_ERROR("SZD: Channel: DirectAppend: No DMA buffer\n");
    return SZDStatus::MemoryError;
  }
  // Write in steps of ZASL
  uint64_t begin = 0;
  uint64_t stepsize = dma_buffer_size;
  SZDStatus s;
  while (begin < size) {
    if (begin + dma_buffer_size >= alligned_size) {
      stepsize = alligned_size - begin;
      memset(dma_buffer, 0, dma_buffer_size);
      memcpy(dma_buffer, (char *)buffer + begin, size - begin);
    } else {
      stepsize = dma_buffer_size;
      memcpy(dma_buffer, (char *)buffer + begin, stepsize);
    }
#ifdef SZD_PERF_COUNTERS
    uint64_t append_ops = 0;
    uint64_t prev_lba = new_lba;
    s = FromStatus(szd_append_with_diag(qpair_, &new_lba, dma_buffer, stepsize,
                                        &append_ops));
    if (s == SZDStatus::Success) {
      bytes_written_.fetch_add(stepsize, std::memory_order_relaxed);
      append_operations_counter_.fetch_add(append_ops,
                                           std::memory_order_relaxed);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
      if ((prev_lba / zone_size_) * zone_size_ !=
          (new_lba / zone_size_) * zone_size_) {
        append_operations_[(prev_lba - min_lba_) / zone_size_] += 1;
      }
      if (new_lba % zone_size_ != 0 && new_lba < max_lba_) {
        append_operations_[(new_lba - min_lba_) / zone_size_] += 1;
      }
    }
#endif
#else
    s = FromStatus(szd_append(qpair_, &new_lba, dma_buffer, stepsize));
#endif

    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Channel: DirectAppend: Could not write\n");
      break;
    }
    begin += stepsize;
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
  if (szd_unlikely(slba < min_lba_ ||
                   slba + zones_needed * zone_size_ > max_lba_ ||
                   (alligned && size != allign_size(size)))) {
    SZD_LOG_ERROR("SZD: Channel: DirectRead: OOB\n");
    return SZDStatus::InvalidArguments;
  }
  // Create temporary DMA buffer to copy other DMA buffer data into.
  size_t dma_buffer_size = mdts_ > alligned_size ? alligned_size : mdts_;
  void *buffer_dma = szd_calloc(lba_size_, 1, dma_buffer_size);
  if (szd_unlikely(buffer_dma == nullptr)) {
    SZD_LOG_ERROR("SZD: Channel: DirectRead: OOM\n");
    return SZDStatus::MemoryError;
  }
  // Read in steps of MDTS
  uint64_t begin = 0;
  uint64_t lba_to_read = lba;
  slba = (lba_to_read / zone_size_) * zone_size_;
  uint64_t current_zone_end = slba + zone_cap_;
  uint64_t stepsize = dma_buffer_size;
  uint64_t alligned_step = dma_buffer_size;
  SZDStatus s;
  while (begin < size) {
    if (begin + dma_buffer_size > alligned_size) {
      alligned_step = size - begin;
      stepsize = alligned_size - begin;
    } else {
      stepsize = dma_buffer_size;
      alligned_step =
          begin + dma_buffer_size > size ? size - begin : dma_buffer_size;
    }
#ifdef SZD_PERF_COUNTERS
    uint64_t read_ops = 0;
    s = FromStatus(szd_read_with_diag(qpair_, lba_to_read, buffer_dma, stepsize,
                                      &read_ops));
    read_operations_.fetch_add(read_ops, std::memory_order_relaxed);
    bytes_read_.fetch_add(stepsize, std::memory_order_relaxed);
#else
    s = FromStatus(szd_read(qpair_, lba_to_read, buffer_dma, stepsize));
#endif
    if (szd_likely(s == SZDStatus::Success)) {
      memcpy((char *)buffer + begin, buffer_dma, alligned_step);
    } else {
      SZD_LOG_ERROR("SZD: Channel: DirectRead: Could not read\n");
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
  if (szd_unlikely(writer >= queue_depth_)) {
    SZD_LOG_ERROR("SZD: Channel: AsyncAppend: Invalid writer\n");
    return SZDStatus::InvalidArguments;
  }
  // Translate lba
  uint64_t new_lba = TranslateLbaToPba(*lba);
  // Allign
  uint64_t alligned_size = allign_size(size);
  if (alligned_size > zasl_) {
    SZD_LOG_ERROR(
        "SZD: Channel: AsyncAppend: Writes larger than ZASL not supported\n");
    return SZDStatus::InvalidArguments;
  }
  // Check if in bounds...
  uint64_t slba = (new_lba / zone_size_) * zone_size_;
  uint64_t zones_needed =
      (new_lba - slba + (alligned_size / lba_size_) + zone_cap_ - 1) /
      zone_cap_;
  if (szd_unlikely(zones_needed > 1 || slba < min_lba_ ||
                   slba + zones_needed * zone_size_ > max_lba_)) {
    SZD_LOG_ERROR("SZD: Channel: AsyncAppend: OOB\n");
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
  if (szd_unlikely(async_buffer_[writer] == nullptr)) {
    SZD_LOG_ERROR("SZD: Channel: AsyncAppend: OOM\n");
    return SZDStatus::MemoryError;
  }
  memcpy(async_buffer_[writer], buffer, size);
  if (completion_[writer] != nullptr) {
    delete completion_[writer];
  }
  completion_[writer] = new Completion;
  SZDStatus s = SZDStatus::Success;
#ifdef SZD_PERF_COUNTERS
  uint64_t append_ops = 0;
  s = FromStatus(szd_append_async_with_diag(
      qpair_, &new_lba, async_buffer_[writer], alligned_size, &append_ops,
      completion_[writer]));
  if (s == SZDStatus::Success) {
    // Diag register
    bytes_written_.fetch_add(alligned_size, std::memory_order_relaxed);
    append_operations_counter_.fetch_add(append_ops, std::memory_order_relaxed);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
    uint64_t left = alligned_size / lba_size_;
    for (slba = TranslateLbaToPba(*lba); left != 0 && slba <= new_lba;
         slba += zone_size_) {
      uint64_t step = left > zone_cap_ ? zone_cap_ : left;
      append_operations_[(slba - min_lba_) / zone_size_] +=
          ((step * lba_size_ + zasl_ - 1) / zasl_);
      left -= step;
    }
  }
#endif
#else
  s = FromStatus(szd_append_async(qpair_, &new_lba, async_buffer_[writer],
                                  alligned_size, completion_[writer]));
#endif
  outstanding_requests_++;

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
  if (completion_[writer]->done || completion_[writer]->err != 0) {
    // Remove temporary buffer.
    if (!keep_async_buffer_) {
      szd_free(async_buffer_[writer]);
      async_buffer_[writer] = nullptr;
    }
    delete completion_[writer];
    completion_[writer] = nullptr;
    outstanding_requests_--;
    return true;
  }
  return false;
}

bool SZDChannel::FindFreeWriter(uint32_t *any_writer) {
  szd_poll_once_raw(qpair_);
  for (uint32_t i = 0; i < queue_depth_; i++) {
    if (completion_[i] == nullptr) {
      *any_writer = i;
      return true;
    }
    if (completion_[i]->err != 0x0 || completion_[i]->done) {
      // Remove temporary buffer.
      if (!keep_async_buffer_) {
        szd_free(async_buffer_[i]);
        async_buffer_[i] = nullptr;
      }
      delete completion_[i];
      completion_[i] = nullptr;
      *any_writer = i;
      outstanding_requests_--;
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
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Channel: Sync: Failed a poll\n");
      break;
    }
    // Remove temporary buffer.
    if (!keep_async_buffer_) {
      szd_free(async_buffer_[i]);
      async_buffer_[i] = nullptr;
    }
    delete completion_[i];
    completion_[i] = nullptr;
    outstanding_requests_--;
  }
  return s;
}

SZDStatus SZDChannel::ResetZone(uint64_t slba) {
  slba = TranslateLbaToPba(slba);
  if (szd_unlikely(slba < min_lba_ || slba > max_lba_)) {
    SZD_LOG_ERROR("SZD: Channel: ResetZone: OOB\n");
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_reset(qpair_, slba));
#ifdef SZD_PERF_COUNTERS
  zones_reset_counter_.fetch_add(1, std::memory_order_relaxed);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  zones_reset_[(slba - min_lba_) / zone_size_]++;
#endif
#endif
  return s;
}

SZDStatus SZDChannel::ResetAllZones() {
  SZDStatus s = SZDStatus::Success;
  // If we can not access all, there is no partial reset; reset the partial
  // zones one by one.
  if (!can_access_all_) {
    for (uint64_t slba = min_lba_; slba != max_lba_; slba += zone_size_) {
      if ((s = FromStatus(szd_reset(qpair_, slba))) != SZDStatus::Success) {
        SZD_LOG_ERROR("SZD: Channel: ResetAllZones: OOB\n");
        return s;
      }
#ifdef SZD_PERF_COUNTERS
      zones_reset_counter_.fetch_add(1, std::memory_order_relaxed);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
      zones_reset_[(slba - min_lba_) / zone_size_]++;
#endif
#endif
    }
  } else {
    s = FromStatus(szd_reset_all(qpair_));
#ifdef SZD_PERF_COUNTERS
    zones_reset_counter_.fetch_add((max_lba_ - min_lba_) / zone_size_,
                                   std::memory_order_relaxed);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
    for (uint64_t &z : zones_reset_) {
      z++;
    }
#endif
#endif
  }
  return s;
}

SZDStatus SZDChannel::ZoneHead(uint64_t slba, uint64_t *zone_head) {
  slba = TranslateLbaToPba(slba);
  if (szd_unlikely(slba < min_lba_ || slba > max_lba_)) {
    SZD_LOG_ERROR("SZD: Channel: ZoneHead: OOB\n");
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_get_zone_head(qpair_, slba, zone_head));
  *zone_head = TranslatePbaToLba(*zone_head);
  return s;
}

SZDStatus SZDChannel::FinishZone(uint64_t slba) {
  slba = TranslateLbaToPba(slba);
  if (szd_unlikely(slba < min_lba_ || slba > max_lba_)) {
    SZD_LOG_ERROR("SZD: Channel: FinishZone: OOB\n");
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = FromStatus(szd_finish_zone(qpair_, slba));
  return s;
}

uint64_t SZDChannel::GetBytesWritten() const {
#ifndef SZD_PERF_COUNTERS
  SZD_LOG_ERROR(
      "SZD: Channel: perf counters not enabled. Info will be wrong.\n");
  return 0;
#else
  return bytes_written_.load(std::memory_order_relaxed);
#endif
}

uint64_t SZDChannel::GetAppendOperationsCounter() const {
#ifndef SZD_PERF_COUNTERS
  SZD_LOG_ERROR(
      "SZD: Channel: perf counters not enabled. Info will be wrong.\n");
  return 0;
#else
  return append_operations_counter_.load(std::memory_order_relaxed);
#endif
}

uint64_t SZDChannel::GetBytesRead() const {
#ifndef SZD_PERF_COUNTERS
  SZD_LOG_ERROR(
      "SZD: Channel: perf counters not enabled. Info will be wrong.\n");
  return 0;
#else
  return bytes_read_.load(std::memory_order_relaxed);
#endif
}

uint64_t SZDChannel::GetReadOperationsCounter() const {
#ifndef SZD_PERF_COUNTERS
  SZD_LOG_ERROR(
      "SZD: Channel: perf counters not enabled. Info will be wrong.\n");
  return 0;
#else
  return read_operations_.load(std::memory_order_relaxed);
#endif
}

uint64_t SZDChannel::GetZonesResetCounter() const {
#ifndef SZD_PERF_COUNTERS
  SZD_LOG_ERROR(
      "SZD: Channel: perf counters not enabled. Info will be wrong.\n");
  return 0;
#else
  return zones_reset_counter_.load(std::memory_order_relaxed);
#endif
}

std::vector<uint64_t> SZDChannel::GetZonesReset() const {
#ifndef SZD_PERF_PER_ZONE_COUNTERS
  SZD_LOG_ERROR(
      "SZD: Channel: perf zone counters not enabled. Info will be wrong.\n");
  return {};
#else
  return zones_reset_;
#endif
}

std::vector<uint64_t> SZDChannel::GetAppendOperations() const {
#ifndef SZD_PERF_PER_ZONE_COUNTERS
  SZD_LOG_ERROR(
      "SZD: Channel: perf zone counters not enabled. Info will be wrong.\n");
  return {};
#else
  return append_operations_;
#endif
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
