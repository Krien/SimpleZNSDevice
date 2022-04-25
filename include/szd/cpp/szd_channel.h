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

  SZDStatus ReserveBuffer(uint64_t size);
  /**
   * @brief Get the backed buffer, WARNING make sure to reserve first and that
   * the buffer does not outlive this channel.
   */
  SZDStatus GetBuffer(void **buffer);
  /**
   * @brief Easy and relatively safe abstractions to append and write to memory
   * buffer. Does involve memcpy so not preferred for tight loops.
   */
  SZDStatus Append(void *data, size_t size, size_t *write_head);
  SZDStatus Write(void *data, size_t size, size_t addr);
  SZDStatus FreeBuffer();
  std::string DebugBufferString();
  SZDStatus FlushBuffer(uint64_t *lba);
  SZDStatus ReadIntoBuffer(uint64_t lba, size_t size, size_t addr,
                           bool alligned = true);

  SZDStatus DirectAppend(uint64_t *lba, void *buffer, const uint64_t size,
                         bool alligned = true) const;
  SZDStatus DirectRead(void *buffer, uint64_t lba, uint64_t size,
                       bool alligned = true) const;

  SZDStatus ResetZone(uint64_t slba) const;
  SZDStatus ResetAllZones() const;

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