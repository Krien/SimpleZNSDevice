/** \file
 * Buffer data structure that uses DMA tagged SPDK memory.
 * */
#pragma once
#ifndef SZD_CPP_BUFFER_H
#define SZD_CPP_BUFFER_H

#include "szd/szd.h"
#include "szd/szd_status.hpp"

#include <string>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
class SZDBuffer {
public:
  SZDBuffer(size_t size, uint64_t lba_size);
  // No copying or implicits
  SZDBuffer(const SZDBuffer &) = delete;
  SZDBuffer &operator=(const SZDBuffer &) = delete;
  ~SZDBuffer();

  inline size_t GetBufferSize() const { return backed_memory_size_; }
  inline std::string DebugBufferString() const {
    return std::string((const char *)backed_memory_, backed_memory_size_);
  }

  /**
   * @brief Get the backed buffer for direct manipulation, WARNING make sure to
   * reserve first and guarantee that the buffer does not outlive this channel.
   */
  SZDStatus GetBuffer(void **buffer) const;
  /**
   * @brief Easy and relatively safe abstractions to append and write to memory
   * buffer. Does involve memcpy so not preferred for tight loops.
   */
  SZDStatus AppendToBuffer(void *data, size_t *write_head, size_t size);
  SZDStatus WriteToBuffer(void *data, size_t addr, size_t size);
  SZDStatus ReadFromBuffer(void *data, size_t addr, size_t size) const;
  /**
   * @brief Increases the memory of the buffer if needed.
   */
  SZDStatus ReallocBuffer(uint64_t size);
  /**
   * @brief Frees the DMA bucked buffer if it exists.
   */
  SZDStatus FreeBuffer();

private:
  uint64_t lba_size_;
  void *backed_memory_;
  size_t backed_memory_size_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif