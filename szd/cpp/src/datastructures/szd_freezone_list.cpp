#include "szd/datastructures/szd_freezone_list.hpp"
#include "szd/szd.h"
#include "szd/szd_channel_factory.hpp"

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

SZDStatus AllocZones(std::vector<SZDFreeList *> &zone_regions,
                     SZDFreeList **from, uint64_t requested_zones) {
  SZDFreeList *start = *from;
  // forward
  while (requested_zones > 0 && *from) {
    if (!(*from)->used_) {
      uint32_t claimed_zones = std::min((*from)->zones_, requested_zones);
      AllocZonesFromRegion(*from, claimed_zones);
      zone_regions.push_back(*from);
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
      zone_regions.push_back(*from);
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

char *EncodeFreelist(SZDFreeList *target) {
  SZDFreeList *first = target;
  while (first) {

    first = first->next_;
  }
  char *output = (char *)calloc(sizeof("deadbeef") + 1, sizeof(char));
  return output;
}

void DecodeFreelist() {}
} // namespace SZDFreeListFunctions
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE