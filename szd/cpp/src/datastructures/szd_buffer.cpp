
#include "szd/datastructures/szd_buffer.hpp"
#include "szd/szd.h"
#include "szd/szd_status.hpp"

#include <cstring>
#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {

SZDBuffer::SZDBuffer(SZD::EngineManager *em, size_t size, uint64_t lba_size)
    : lba_size_(lba_size), backed_memory_(nullptr), backed_memory_size_(size), em_(em) {
  backed_memory_size_ =
      ((backed_memory_size_ + lba_size_ - 1) / lba_size_) * lba_size_;
  if (backed_memory_size_ != 0) {
    backed_memory_ = szd_calloc(em_, lba_size_, 1, backed_memory_size_);
  }
  // idle state (can also be because of bad malloc!)
  if (backed_memory_ == nullptr) {
    backed_memory_size_ = 0;
  }
}
SZDBuffer::~SZDBuffer() {
  if (backed_memory_ != nullptr && backed_memory_size_ > 0) {
    szd_free(em_, backed_memory_);
  }
}

SZDStatus SZDBuffer::GetBuffer(void **buffer) const {
  if (szd_unlikely(backed_memory_ == nullptr)) {
    SZD_LOG_ERROR("SZD: Buffer: GetBuffer: NULL\n");
    return SZDStatus::IOError;
  }
  *buffer = backed_memory_;
  return SZDStatus::Success;
}
SZDStatus SZDBuffer::AppendToBuffer(void *data, size_t *write_head,
                                    size_t size) {
  if (szd_unlikely(*write_head + size > backed_memory_size_)) {
    SZD_LOG_ERROR("SZD: Buffer: AppendToBuffer: OOB\n");
    return SZDStatus::InvalidArguments;
  }
  memmove((char *)backed_memory_ + *write_head, data, size);
  *write_head += size;
  return SZDStatus::Success;
}

SZDStatus SZDBuffer::WriteToBuffer(void *data, size_t addr, size_t size) {
  if (szd_unlikely(addr + size > backed_memory_size_)) {
    SZD_LOG_ERROR("SZD: Buffer: WriteToBuffer: OOB\n");
    return SZDStatus::InvalidArguments;
  }
  memmove((char *)backed_memory_ + addr, data, size);
  return SZDStatus::Success;
}

SZDStatus SZDBuffer::ReadFromBuffer(void *data, size_t addr,
                                    size_t size) const {
  if (szd_unlikely(addr + size > backed_memory_size_)) {
    SZD_LOG_ERROR("SZD: Buffer: ReadFromBuffer: OOB\n");
    return SZDStatus::InvalidArguments;
  }
  memmove(data, (char *)backed_memory_ + addr, size);
  return SZDStatus::Success;
}

SZDStatus SZDBuffer::ReallocBuffer(uint64_t size) {
  SZDStatus s = SZDStatus::Success;
  uint64_t alligned_size = ((size + lba_size_ - 1) / lba_size_) * lba_size_;
  /* nothing to do (if you want to reduce memory of the buffer, instead free
   first) */
  if (backed_memory_size_ > 0 && backed_memory_size_ > alligned_size) {
    return s;
  }
  // realloc, we need more space
  char tmp[backed_memory_size_];
  if (backed_memory_size_ > 0) {
    memcpy(tmp, backed_memory_, backed_memory_size_);
    if ((s = FreeBuffer()) != SZDStatus::Success) {
      SZD_LOG_ERROR("SZD: Buffer: ReallocBuffer: Failed free\n");
      return s;
    }
  }
  backed_memory_ = szd_calloc(em_, lba_size_, alligned_size, sizeof(char));
  if (szd_unlikely(backed_memory_ == nullptr)) {
    backed_memory_size_ = 0;
    SZD_LOG_ERROR("SZD: Buffer: ReallocBuffer: Failed allocating memory\n");
    return SZDStatus::IOError;
  }
  if (backed_memory_size_ > 0) {
    memcpy(backed_memory_, tmp, backed_memory_size_);
  }
  backed_memory_size_ = alligned_size;
  return SZDStatus::Success;
}

SZDStatus SZDBuffer::FreeBuffer() {
  if (backed_memory_size_ == 0) {
    return SZDStatus::Success;
  }
  szd_free(em_, backed_memory_);
  backed_memory_ = nullptr;
  backed_memory_size_ = 0;
  return SZDStatus::Success;
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE