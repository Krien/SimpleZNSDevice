#ifndef SZD_CPP_CHANNEL_H
#define SZD_CPP_CHANNEL_H

#include "szd/cpp/szd_status.h"
#include "szd/szd.h"
#include "szd/szd_utils.h"

#include <memory>
#include <string>

namespace SimpleZNSDeviceNamespace {
/**
 * @brief Simple abstraction on top of a QPair to make the code more Cxx like.
 * Comes with helper functions and performance optimisations.
 */
class SZDChannel {
public:
  SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info,
             uint64_t min_lba, uint64_t max_lba);
  SZDChannel(std::unique_ptr<QPair> qpair, const DeviceInfo &info);
  // No copying or implicits
  SZDChannel(const SZDChannel &) = delete;
  SZDChannel &operator=(const SZDChannel &) = delete;
  ~SZDChannel();

  /**
   * @brief Reserve a DMA backed buffer for SPDK.
   * Reducing overhead of reallocating memory for each I/O operation.
   */
  SZDStatus ReserveBuffer(uint64_t size);
  /**
   * @brief Get the backed buffer, WARNING make sure to reserve first and
   * guarantee that the buffer does not outlive this channel.
   */
  SZDStatus GetBuffer(void **buffer);
  /**
   * @brief Frees the DMA bucked buffer if it exists.
   * @return SZDStatus
   */
  SZDStatus FreeBuffer();
  /**
   * @brief Easy and relatively safe abstractions to append and write to memory
   * buffer. Does involve memcpy so not preferred for tight loops.
   */
  SZDStatus AppendToBuffer(void *data, size_t *write_head, size_t size);
  SZDStatus WriteToBuffer(void *data, size_t addr, size_t size);
  std::string DebugBufferString();
  // Buffer I/O Operations
  SZDStatus FlushBuffer(uint64_t *lba);
  SZDStatus FlushBufferSection(uint64_t *lba, uint64_t addr, uint64_t size,
                               bool alligned = true);
  SZDStatus ReadIntoBuffer(uint64_t lba, size_t addr, size_t size,
                           bool alligned = true);
  // Direct I/O Operations
  SZDStatus DirectAppend(uint64_t *lba, void *buffer, const uint64_t size,
                         bool alligned = true) const;
  SZDStatus DirectRead(void *buffer, uint64_t lba, uint64_t size,
                       bool alligned = true) const;
  // Management of zones
  SZDStatus ResetZone(uint64_t slba) const;
  SZDStatus ResetAllZones() const;
  SZDStatus ZoneHead(uint64_t slba, uint64_t *zone_head) const;

private:
  constexpr uint64_t allign_size(uint64_t size) const {
    return ((size + lba_size_ - 1) / lba_size_) * lba_size_;
  }
  QPair *qpair_;
  uint64_t lba_size_;
  uint64_t zone_size_;
  uint64_t min_lba_;
  uint64_t max_lba_;
  bool can_access_all_;
  void *backed_memory_;
  size_t backed_memory_size_;
};
} // namespace SimpleZNSDeviceNamespace

#endif