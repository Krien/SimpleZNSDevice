#include "szd/datastructures/szd_fragmented_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

#include <mutex>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDFragmentedLog::SZDFragmentedLog(SZDChannelFactory *channel_factory,
                                   const DeviceInfo &info,
                                   const uint64_t min_zone_nr,
                                   const uint64_t max_zone_nr,
                                   const uint8_t number_of_readers,
                                   const uint8_t number_of_writers)
    : min_zone_head_(min_zone_nr * info.zone_cap),
      max_zone_head_(max_zone_nr * info.zone_cap), zone_size_(info.zone_size),
      zone_cap_(info.zone_cap), lba_size_(info.lba_size), zasl_(info.zasl),
      zone_bytes_(info.zone_cap * info.lba_size),
      number_of_readers_(number_of_readers),
      number_of_writers_(number_of_writers), freelist_(nullptr),
      seeker_(nullptr), zones_left_(max_zone_nr - min_zone_nr),
      channel_factory_(channel_factory), write_channel_(nullptr),
      read_channel_(nullptr) {
  SZDFreeListFunctions::Init(&freelist_, min_zone_nr, max_zone_nr);
  seeker_ = freelist_;
  channel_factory_->Ref();
  read_channel_ = new SZD::SZDChannel *[number_of_readers_];
  for (uint8_t i = 0; i < number_of_readers_; i++) {
    channel_factory_->register_channel(&read_channel_[i], min_zone_nr,
                                       max_zone_nr);
  }
  write_channel_ = new SZD::SZDChannel *[number_of_writers_];
  for (uint8_t i = 0; i < number_of_writers_; i++) {
    channel_factory_->register_channel(&write_channel_[i], min_zone_nr,
                                       max_zone_nr);
  }
}

SZDFragmentedLog::~SZDFragmentedLog() {
  if (write_channel_ != nullptr) {
    for (uint8_t i = 0; i < number_of_writers_; i++) {
      if (write_channel_[i]) {
        channel_factory_->unregister_channel(write_channel_[i]);
      }
    }
    delete[] write_channel_;
  }
  if (read_channel_ != nullptr) {
    for (uint8_t i = 0; i < number_of_readers_; i++) {
      if (read_channel_[i]) {
        channel_factory_->unregister_channel(read_channel_[i]);
      }
    }
    delete[] read_channel_;
  }
  channel_factory_->Unref();
  if (seeker_ != nullptr) {
    SZDFreeListFunctions::Destroy(seeker_);
  }
}

SZDStatus
SZDFragmentedLog::Append(const char *buffer, size_t size,
                         std::vector<std::pair<uint64_t, uint64_t>> &regions,
                         bool alligned, uint8_t writer) {
#ifdef ALLOW_ASYNC_APPEND
  return AsyncAppend(buffer, size, regions, alligned);
#endif
  if (szd_unlikely(writer > number_of_writers_)) {
    SZD_LOG_ERROR("SZD: Fragmented log: Append: Invalid writer\n");
    return SZDStatus::InvalidArguments;
  }
  // Check buffer space
  size_t alligned_size =
      alligned ? size : write_channel_[writer]->allign_size(size);
  // Check zone space
  size_t zones_needed =
      (alligned_size + zone_bytes_ - 1) / zone_cap_ / lba_size_;
  if (szd_unlikely(zones_needed > zones_left_)) {
    SZD_LOG_ERROR("SZD: Fragmented log: Append: No space left\n");
    return SZDStatus::InvalidArguments;
  }

  // Reserve zones
  if (number_of_writers_ > 1) {
    mut_.lock();
  }
  // 2 writers reserve at same time, but there is no space.
  if (zones_left_ < zones_needed) {
    mut_.unlock();
    return SZDStatus::IOError;
  }
  if (szd_unlikely(
          SZDFreeListFunctions::AllocZones(regions, &seeker_, zones_needed) !=
          SZDStatus::Success)) {
    if (number_of_writers_ > 1) {
      mut_.unlock();
    }
    SZD_LOG_ERROR("SZD: Fragmented log: Append: No space left\n");
    return SZDStatus::Unknown;
  }
  zones_left_ -= zones_needed;
  if (number_of_writers_ > 1) {
    mut_.unlock();
  }

  // Write to zones
  SZDStatus s = SZDStatus::Success;
  uint64_t offset = 0;
  uint64_t bytes_to_write = 0;
  uint64_t slba = 0;
  bool write_alligned = true;

  for (auto region : regions) {
    slba = region.first * zone_cap_;
    bytes_to_write = region.second * zone_cap_ * lba_size_;
    if (bytes_to_write > size - offset) {
      bytes_to_write = size - offset;
      write_alligned = alligned;
    }
    s = write_channel_[writer]->DirectAppend(&slba, (void *)(buffer + offset),
                                             bytes_to_write, write_alligned);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Fragmented log: Append: Invalid append\n");
      return s;
    }
    offset += bytes_to_write;
  }

  // Ensure that resources are released
  if ((slba / zone_cap_) * zone_cap_ != slba) {
    s = write_channel_[writer]->FinishZone((slba / zone_cap_) * zone_cap_);
    if (s != SZDStatus::Success) {
      SZD_LOG_ERROR("SZD: Fragmented log: Append: Failed to finish zone\n");
    }
  }

  return s;
}

SZDStatus
SZDFragmentedLog::Append(const SZDBuffer &buffer, size_t addr, size_t size,
                         std::vector<std::pair<uint64_t, uint64_t>> &regions,
                         bool alligned, uint8_t writer) {
  if (szd_unlikely(writer > number_of_writers_)) {
    SZD_LOG_ERROR("SZD: Fragmented log: Append: Invalid writer\n");
    return SZDStatus::InvalidArguments;
  }
  // Check buffer space
  size_t alligned_size =
      alligned ? size : write_channel_[writer]->allign_size(size);
  if (szd_unlikely(addr + size > buffer.GetBufferSize())) {
    SZD_LOG_ERROR("SZD: Fragmented log: Append: Invalid buffer\n");
    return SZDStatus::InvalidArguments;
  }

  // Check zone space
  size_t zones_needed =
      (alligned_size + zone_bytes_ - 1) / zone_cap_ / lba_size_;
  if (szd_unlikely(zones_needed > zones_left_)) {
    SZD_LOG_ERROR("SZD: Fragmented log: Append: out of space\n");
    return SZDStatus::InvalidArguments;
  }

  // Reserve zones
  if (number_of_writers_ > 1) {
    mut_.lock();
  }
  if (szd_unlikely(
          SZDFreeListFunctions::AllocZones(regions, &seeker_, zones_needed) !=
          SZDStatus::Success)) {
    if (number_of_writers_ > 1) {
      mut_.unlock();
    }
    SZD_LOG_ERROR("SZD: Fragmented log: Append: Failed allocation\n");
    return SZDStatus::Unknown;
  }
  zones_left_ -= zones_needed;
  if (number_of_writers_ > 1) {
    mut_.unlock();
  }

  // Write to zones
  SZDStatus s = SZDStatus::Success;
  uint64_t offset = 0;
  uint64_t bytes_to_write;
  uint64_t slba;
  bool write_alligned = true;
  for (auto region : regions) {
    slba = region.first * zone_cap_;
    bytes_to_write = region.second * zone_cap_;
    if (bytes_to_write > size - offset) {
      bytes_to_write = size - offset;
      write_alligned = alligned;
    }
    s = write_channel_[writer]->FlushBufferSection(
        &slba, buffer, addr + offset, bytes_to_write, write_alligned);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR(
          "SZD: Fragmented log: Append: Could not flush buffer section\n");
      return s;
    }
    offset += bytes_to_write;
  }

  // Ensure that resources are released
  if ((slba / zone_size_) * zone_size_ != slba) {
    s = write_channel_[writer]->FinishZone((slba / zone_size_) * zone_size_);
  }
  return s;
}

SZDStatus SZDFragmentedLog::Read(
    const std::vector<std::pair<uint64_t, uint64_t>> &regions, char *data,
    uint64_t size, bool alligned, uint8_t reader) {
  if (szd_unlikely(reader > number_of_readers_)) {
    SZD_LOG_ERROR("SZD: Fragmented log: Read: Invalid reader\n");
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = SZDStatus::Success;
  uint64_t read = 0;
  bool alligned_read = true;
  uint64_t size_to_read = 0;
  for (auto region : regions) {
    if (size - read < region.second * zone_cap_ * lba_size_) {
      size_to_read = size - read;
      alligned_read = alligned;
    } else {
      size_to_read = region.second * zone_cap_ * lba_size_;
    }
    s = read_channel_[reader]->DirectRead(region.first * zone_cap_, data + read,
                                          size_to_read, alligned_read);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Fragmented log: Read: Failed reading from storage\n");
      return s;
    }
    read += size_to_read;
  }
  return s;
}

SZDStatus
SZDFragmentedLog::Reset(std::vector<std::pair<uint64_t, uint64_t>> &regions,
                        uint8_t writer) {
  if (szd_unlikely(writer > number_of_writers_)) {
    SZD_LOG_ERROR("SZD: Fragmented log: Reset: Invalid writer\n");
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = SZDStatus::Success;
  // Erase data
  for (auto region : regions) {
    uint64_t begin = region.first * zone_cap_;
    uint64_t end = begin + region.second * zone_cap_;
    for (uint64_t slba = begin; slba < end; slba += zone_cap_) {
      s = write_channel_[writer]->ResetZone(slba);
      if (szd_unlikely(s != SZDStatus::Success)) {
        SZD_LOG_ERROR(
            "SZD: Fragmented log: Reset: Could not reset zone at %lu\n", slba);
        return s;
      }
      zones_left_++;
    }
    SZDFreeList *to_delete;
    if (number_of_writers_ > 1) {
      mut_.lock();
    }
    SZDFreeListFunctions::FindRegion(region.first, seeker_, &to_delete);
    SZDFreeListFunctions::FreeZones(to_delete, &seeker_);
    if (number_of_writers_ > 1) {
      mut_.unlock();
    }
  }
  return s;
}

SZDStatus SZDFragmentedLog::ResetAll(uint8_t writer) {
  if (szd_unlikely(writer > number_of_writers_)) {
    SZD_LOG_ERROR("SZD: Fragmented log: ResetAll: Not a valid writer\n");
    return SZDStatus::InvalidArguments;
  }
  SZDStatus s = SZDStatus::Success;
  for (uint64_t slba = min_zone_head_; slba != max_zone_head_;
       slba += zone_cap_) {
    s = write_channel_[writer]->ResetZone(slba);
    if (szd_unlikely(s != SZDStatus::Success)) {
      SZD_LOG_ERROR("SZD: Fragmented log: ResetAll: Could not reset zone\n");
      return s;
    }
  }
  // Reset list
  if (number_of_writers_ > 1) {
    mut_.lock();
  }
  SZDFreeListFunctions::Destroy(seeker_);
  SZDFreeListFunctions::Init(&freelist_, min_zone_head_ / zone_cap_,
                             max_zone_head_ / zone_cap_);
  seeker_ = freelist_;
  zones_left_ = (max_zone_head_ - min_zone_head_) / zone_cap_;
  if (number_of_writers_ > 1) {
    mut_.unlock();
  }
  return s;
}

SZDStatus SZDFragmentedLog::Recover() { return SZDStatus::Success; }

bool SZDFragmentedLog::Empty() const {
  return zones_left_ == (max_zone_head_ - min_zone_head_) / zone_cap_;
}

uint64_t SZDFragmentedLog::SpaceAvailable() const {
  return zones_left_ * zone_cap_ * lba_size_;
}
bool SZDFragmentedLog::SpaceLeft(const size_t size, bool alligned) const {
  size_t alligned_size = alligned ? size : write_channel_[0]->allign_size(size);
  size_t zones_needed =
      (alligned_size + zone_bytes_ - 1) / zone_cap_ / lba_size_;
  return zones_needed <= zones_left_;
}

std::string SZDFragmentedLog::Encode() {
  uint64_t encoded_size;
  char *encoded = SZDFreeListFunctions::EncodeFreelist(seeker_, &encoded_size);
  std::string encoded_str(encoded, encoded_size);
  return encoded_str;
}

SZDStatus SZDFragmentedLog::DecodeFrom(const char *data, const size_t size) {
  uint32_t new_zones_left;
  SZDFreeList **new_freelist = new SZDFreeList *;
  SZDStatus s = SZDFreeListFunctions::DecodeFreelist(data, size, new_freelist,
                                                     &new_zones_left);
  if (szd_unlikely(s != SZDStatus::Success)) {
    delete new_freelist;
    return s;
  }
  freelist_ = *new_freelist;
  seeker_ = freelist_;
  zones_left_ = new_zones_left;
  delete new_freelist;
  return s;
}

bool SZDFragmentedLog::TESTEncodingDecoding() const {
  uint64_t encoded_size;
  const char *encoded =
      SZDFreeListFunctions::EncodeFreelist(seeker_, &encoded_size);
  SZDFreeList *newlist;
  uint32_t zones_free = 0;
  SZDStatus s = SZDFreeListFunctions::DecodeFreelist(encoded, encoded_size,
                                                     &newlist, &zones_free);
  delete[] encoded;
  if (s != SZDStatus::Success) {
    return false;
  }
  if (!SZDFreeListFunctions::TESTFreeListsEqual(seeker_, newlist)) {
    SZDFreeListFunctions::Destroy(newlist);
    return false;
  }
  if (zones_free != zones_left_) {
    SZDFreeListFunctions::Destroy(newlist);
    return false;
  }
  SZDFreeListFunctions::Destroy(newlist);
  return true;
}

} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
