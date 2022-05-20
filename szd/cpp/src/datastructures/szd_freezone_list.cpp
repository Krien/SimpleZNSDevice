#include "szd/datastructures/szd_freezone_list.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

#include <string.h>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
namespace SZDFreeListFunctions {
void Init(SZDFreeList **freelist, uint64_t begin_zone, uint64_t max_zone) {
  *freelist = new SZDFreeList();
  (*freelist)->begin_zone_ = begin_zone;
  (*freelist)->zones_ = max_zone - begin_zone;
  (*freelist)->used_ = false;
  (*freelist)->prev_ = nullptr;
  (*freelist)->next_ = nullptr;
}

void Destroy(SZDFreeList *target) {
  SZDFreeList *first = FirstZoneRegion(target);
  SZDFreeList *next = first;
  while (first != nullptr) {
    next = first->next_;
    delete first;
    first = next;
  }
}

SZDFreeList *NextZoneRegion(SZDFreeList *target) { return target->next_; }

SZDFreeList *PrevZoneRegion(SZDFreeList *target) { return target->prev_; }

SZDFreeList *FirstZoneRegion(SZDFreeList *target) {
  SZDFreeList *last = target;
  SZDFreeList *current = target;
  while (current != nullptr) {
    last = current;
    current = current->prev_;
  }
  return last;
}

SZDFreeList *lastZoneRegion(SZDFreeList *target) {
  SZDFreeList *last = target;
  SZDFreeList *current = target;
  while (current != nullptr) {
    last = current;
    current = current->next_;
  }
  return last;
}

void FreeZones(SZDFreeList *target, SZDFreeList **orig) {
  if (!target->used_) {
    // This, this is highly illegal!
    return;
  }
  // Checkpoint to ensure that target can be deleted
  SZDFreeList *prev = target->prev_;
  SZDFreeList *next = target->next_;
  uint64_t zones = target->zones_;
  uint64_t begin_zone = target->begin_zone_;
  // Set state to free.
  target->used_ = false;
  *orig = target;
  // merge with prev?
  if (prev != nullptr && !prev->used_) {
    prev->zones_ += zones;
    zones = 0;
    prev->next_ = next;
    if (next) {
      next->prev_ = prev;
    }
    *orig = prev; // prevents origin becoming null...
    delete target;
  }
  // merge with next
  if (next != nullptr && !next->used_ && zones > 0) {
    next->begin_zone_ = begin_zone;
    next->zones_ += zones;
    zones = 0;
    next->prev_ = prev;
    if (prev) {
      prev->next_ = next;
    }
    *orig = next; // prevents origin becoming null...
    delete target;
  }
}

void AllocZonesFromRegion(SZDFreeList *target, uint64_t zones) {
  if (target->used_ || target->zones_ < zones) {
    // Should not happen obviously
    return;
  }
  // Split
  if (target->zones_ > zones) {
    // New next
    SZDFreeList *next = new SZDFreeList();
    next->used_ = false;
    next->begin_zone_ = target->begin_zone_ + zones;
    next->zones_ = target->zones_ - zones;
    next->prev_ = target;
    // Change pointers
    if (target->next_ != nullptr) {
      target->next_->prev_ = next;
      next->next_ = target->next_;
    }
    target->next_ = next;
    // Alter current
    target->zones_ = zones;
  }
  target->used_ = true;
}

SZDStatus AllocZones(std::vector<std::pair<uint64_t, u_int64_t>> &zone_regions,
                     SZDFreeList **from, uint64_t requested_zones) {
  SZDFreeList *start = *from;
  // forward
  while (requested_zones > 0 && *from) {
    if (!(*from)->used_) {
      uint32_t claimed_zones = std::min((*from)->zones_, requested_zones);
      AllocZonesFromRegion(*from, claimed_zones);
      std::pair region = std::make_pair((*from)->begin_zone_, (*from)->zones_);
      zone_regions.push_back(region);
      requested_zones -= claimed_zones;
      if (requested_zones == 0) {
        return SZDStatus::Success;
      }
    }
    *from = (*from)->next_;
  }
  // backward
  *from = start;
  while (requested_zones > 0 && *from) {
    if (!(*from)->used_) {
      uint32_t claimed_zones = std::min((*from)->zones_, requested_zones);
      AllocZonesFromRegion(*from, claimed_zones);
      std::pair region = std::make_pair((*from)->begin_zone_, (*from)->zones_);
      zone_regions.push_back(region);
      requested_zones -= claimed_zones;
      if (requested_zones == 0) {
        return SZDStatus::Success;
      }
    }
    *from = (*from)->prev_;
  }
  // No space found...
  if ((*from) == nullptr) {
    *from = start; // prevents memory leaks and nullptrs
  }
  return SZDStatus::InvalidArguments;
}

SZDStatus FindRegion(const uint64_t ident, SZDFreeList *from,
                     SZDFreeList **target) {
  SZDFreeList *first = FirstZoneRegion(from);
  while (first != nullptr) {
    if (first->begin_zone_ == ident) {
      *target = first;
      return SZDStatus::Success;
    }
    first = first->next_;
  }
  return SZDStatus::InvalidArguments;
}

static uint64_t Decode64(const char *data) {
  // Important first cast to uint64t to avoid signed logic or undefined
  // shifts...
  return static_cast<uint64_t>(data[0]) |
         (static_cast<uint64_t>(data[1]) << 8) |
         (static_cast<uint64_t>(data[2]) << 16) |
         (static_cast<uint64_t>(data[3]) << 24) |
         (static_cast<uint64_t>(data[4]) << 32) |
         (static_cast<uint64_t>(data[5]) << 40) |
         (static_cast<uint64_t>(data[6]) << 48) |
         (static_cast<uint64_t>(data[7]) << 56);
}

static void Encode64(char *data, uint64_t nr) {
  data[0] = nr & 0xff;
  data[1] = (nr >> 8) & 0xff;
  data[2] = (nr >> 16) & 0xff;
  data[3] = (nr >> 24) & 0xff;
  data[4] = (nr >> 32) & 0xff;
  data[5] = (nr >> 40) & 0xff;
  data[6] = (nr >> 48) & 0xff;
  data[7] = (nr >> 56) & 0xff;
}

// 17 bytes * frags + 8 bytes for size
// TODO: this is temporary, preferably rework...
char *EncodeFreelist(SZDFreeList *target, uint64_t *size) {
  std::string data;
  SZDFreeList *first = FirstZoneRegion(target);
  char entry[17];
  while (first) {
    Encode64(entry, first->begin_zone_);
    Encode64(entry + sizeof(uint64_t), first->zones_);
    memcpy(entry + sizeof(uint64_t) * 2, &first->used_, sizeof(bool));
    data.append(entry, sizeof(entry));
    first = first->next_;
  }
  char *output = new char[data.size() + sizeof(uint64_t) + 1];
  uint64_t out_size = static_cast<uint64_t>(data.size() + sizeof(uint64_t));
  Encode64(output, out_size);
  memcpy(output + sizeof(uint64_t), data.data(), data.size());
  *size = out_size;
  return output;
}

SZDStatus DecodeFreelist(const char *buffer, uint64_t buffer_size,
                         SZDFreeList **target, uint32_t *zones_free) {
  if (buffer_size < sizeof(uint64_t)) {
    return SZDStatus::InvalidArguments;
  }
  // Size of original encoded string.
  uint64_t true_size;
  true_size = Decode64(buffer);
  if (true_size > buffer_size) {
    return SZDStatus::InvalidArguments;
  }
  buffer_size = true_size;

  SZDFreeList *prev = nullptr;
  *target = prev; // Ensure consistent error handling;
  *zones_free = 0;
  // Try parsing entries
  uint64_t walker = sizeof(uint64_t);
  while (walker < buffer_size) {
    SZDFreeList *next = new SZDFreeList;
    next->begin_zone_ = Decode64(buffer + walker);
    walker += sizeof(uint64_t);
    next->zones_ = Decode64(buffer + walker);
    walker += sizeof(uint64_t);
    memcpy(&next->used_, buffer + walker, sizeof(bool));
    if (!next->used_) {
      *zones_free += next->zones_;
    }
    walker += sizeof(bool);
    if (prev != nullptr) {
      next->prev_ = prev;
      prev->next_ = next;
    } else {
      next->prev_ = nullptr;
    }
    next->next_ = nullptr;
    prev = next;
    *target = next;
  }
  return SZDStatus::Success;
}

bool TESTFreeListsEqual(SZDFreeList *left, SZDFreeList *right) {
  if (right == nullptr && left != nullptr) {
    return false;
  }
  SZDFreeList *first_left = FirstZoneRegion(left);
  SZDFreeList *first_right = FirstZoneRegion(right);
  while (first_left != nullptr && first_right != nullptr) {
    if (first_left->begin_zone_ != first_right->begin_zone_ ||
        first_left->zones_ != first_right->zones_ ||
        first_left->used_ != first_right->used_) {
      return false;
    }
    first_left = first_left->next_;
    first_right = first_right->next_;
  }
  if (!(first_left == nullptr && first_right == nullptr)) {
    return false;
  }
  return true;
};

} // namespace SZDFreeListFunctions
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE
