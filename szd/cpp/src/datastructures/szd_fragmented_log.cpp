#include "szd/datastructures/szd_fragmented_log.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"
namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
SZDFragmentedLog::SZDFragmentedLog(SZDChannelFactory *channel_factory,
                                   const DeviceInfo &info,
                                   const uint64_t min_zone_nr,
                                   const uint64_t max_zone_nr)
    : min_zone_head_(min_zone_nr * info.zone_cap),
      max_zone_head_(max_zone_nr * info.zone_cap), zone_size_(info.zone_size),
      zone_cap_(info.zone_cap), lba_size_(info.lba_size),
      zone_bytes_(info.zone_cap * info.lba_size), freelist_(nullptr),
      seeker_(nullptr), zones_left_(max_zone_nr - min_zone_nr),
      channel_factory_(channel_factory), write_channel_(nullptr),
      read_channel_(nullptr) {
  SZDFreeListFunctions::Init(&freelist_, min_zone_nr, max_zone_nr);
  seeker_ = freelist_;
  channel_factory_->Ref();
  channel_factory_->register_channel(&write_channel_, min_zone_nr, max_zone_nr);
  channel_factory_->register_channel(&read_channel_, min_zone_nr, max_zone_nr);
}

SZDFragmentedLog::~SZDFragmentedLog() {
  if (write_channel_ != nullptr) {
    channel_factory_->unregister_channel(write_channel_);
  }
  if (read_channel_ != nullptr) {
    channel_factory_->unregister_channel(read_channel_);
  }
  channel_factory_->Unref();
  if (seeker_ != nullptr) {
    SZDFreeListFunctions::Destroy(seeker_);
  }
}

SZDStatus
SZDFragmentedLog::Append(const char *buffer, size_t size,
                         std::vector<std::pair<uint64_t, uint64_t>> &regions,
                         bool alligned) {
  // Check buffer space
  size_t alligned_size = alligned ? size : write_channel_->allign_size(size);
  // Check zone space
  size_t zones_needed =
      (alligned_size + zone_bytes_ - 1) / zone_cap_ / lba_size_;
  if (zones_needed > zones_left_) {
    return SZDStatus::InvalidArguments;
  }

  // Reserve zones
  if (SZDFreeListFunctions::AllocZones(regions, &seeker_, zones_needed) !=
      SZDStatus::Success) {
    return SZDStatus::Unknown;
  }
  zones_left_ -= zones_needed;

  // Write to zones
  SZDStatus s = SZDStatus::Success;
  uint64_t offset = 0;
  uint64_t bytes_to_write;
  uint64_t slba;
  bool write_alligned = true;
  for (auto region : regions) {
    slba = region.first * zone_cap_;
    bytes_to_write = region.second * zone_cap_ * lba_size_;
    if (bytes_to_write > size - offset) {
      bytes_to_write = size - offset;
      write_alligned = alligned;
    }
    s = write_channel_->DirectAppend(&slba, (void *)(buffer + offset),
                                     bytes_to_write, write_alligned);
    if (s != SZDStatus::Success) {
      return s;
    }
    offset += bytes_to_write;
  }

  return s;
}

SZDStatus
SZDFragmentedLog::Append(const SZDBuffer &buffer, size_t addr, size_t size,
                         std::vector<std::pair<uint64_t, uint64_t>> &regions,
                         bool alligned) {
  // Check buffer space
  size_t alligned_size = alligned ? size : write_channel_->allign_size(size);
  if (addr + size > buffer.GetBufferSize()) {
    return SZDStatus::InvalidArguments;
  }

  // Check zone space
  size_t zones_needed =
      (alligned_size + zone_bytes_ - 1) / zone_cap_ / lba_size_;
  if (zones_needed > zones_left_) {
    return SZDStatus::InvalidArguments;
  }

  // Reserve zones
  if (SZDFreeListFunctions::AllocZones(regions, &seeker_, zones_needed) !=
      SZDStatus::Success) {
    return SZDStatus::Unknown;
  }
  zones_left_ -= zones_needed;

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
    s = write_channel_->FlushBufferSection(&slba, buffer, addr + offset,
                                           bytes_to_write, write_alligned);
    if (s != SZDStatus::Success) {
      return s;
    }
    offset += bytes_to_write;
  }

  return s;
}

SZDStatus SZDFragmentedLog::Read(
    const std::vector<std::pair<uint64_t, uint64_t>> &regions, char *data,
    uint64_t size, bool alligned) {
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
    s = read_channel_->DirectRead(region.first * zone_cap_, data + read,
                                  size_to_read, alligned_read);
    if (s != SZDStatus::Success) {
      return s;
    }
    read += size_to_read;
  }
  return s;
}

SZDStatus
SZDFragmentedLog::Reset(std::vector<std::pair<uint64_t, uint64_t>> &regions) {
  SZDStatus s = SZDStatus::Success;
  // Erase data
  for (auto region : regions) {
    uint64_t begin = region.first * zone_cap_;
    uint64_t end = begin + region.second * zone_cap_;
    for (uint64_t slba = begin; slba < end; slba += zone_cap_) {
      s = write_channel_->ResetZone(slba);
      if (s != SZDStatus::Success) {
        return s;
      }
      zones_left_++;
    }
    SZDFreeList *to_delete;
    SZDFreeListFunctions::FindRegion(region.first, seeker_, &to_delete);
    SZDFreeListFunctions::FreeZones(to_delete, &seeker_);
  }
  return s;
}

SZDStatus SZDFragmentedLog::ResetAll() {
  SZDStatus s;
  for (uint64_t slba = min_zone_head_; slba != max_zone_head_;
       slba += zone_cap_) {
    s = write_channel_->ResetZone(slba);
    if (s != SZDStatus::Success) {
      return s;
    }
  }
  // Reset list
  SZDFreeListFunctions::Destroy(seeker_);
  SZDFreeListFunctions::Init(&freelist_, min_zone_head_ / zone_cap_,
                             max_zone_head_ / zone_cap_);
  seeker_ = freelist_;
  zones_left_ = (max_zone_head_ - min_zone_head_) / zone_cap_;
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
  size_t alligned_size = alligned ? size : write_channel_->allign_size(size);
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
  if (s != SZDStatus::Success) {
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
