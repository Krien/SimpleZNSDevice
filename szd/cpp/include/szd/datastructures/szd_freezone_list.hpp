/** \file
 * Interface for simple log structures
 * */
#pragma once
#ifndef SZD_FREEZONE_LIST_H
#define SZD_FREEZONE_LIST_H

#include "szd/datastructures/szd_buffer.hpp"
#include "szd/szd.h"
#include "szd/szd_channel.hpp"
#include "szd/szd_channel_factory.hpp"
#include "szd/szd_status.hpp"

#include <string>
#include <vector>

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {

struct SZDFreeList {
  uint64_t begin_zone_;
  uint64_t zones_;
  bool used_;
  SZDFreeList *prev_;
  SZDFreeList *next_;
};

namespace SZDFreeListFunctions {
void Init(SZDFreeList **freelist, uint64_t begin_zone, uint64_t max_zone);
void Destroy(SZDFreeList *target);

SZDFreeList *NextZoneRegion(SZDFreeList *target);
SZDFreeList *PrevZoneRegion(SZDFreeList *target);
SZDFreeList *FirstZoneRegion(SZDFreeList *target);
SZDFreeList *lastZoneRegion(SZDFreeList *target);

void FreeZones(SZDFreeList *target, SZDFreeList **orig);
void AllocZonesFromRegion(SZDFreeList *target, uint64_t zones);
SZDStatus AllocZones(std::vector<std::pair<uint64_t, u_int64_t>> &zone_regions,
                     SZDFreeList **from, uint64_t requested_zones);
SZDStatus FindRegion(const uint64_t ident, SZDFreeList *from,
                     SZDFreeList **target);

char *EncodeFreelist(SZDFreeList *target, uint64_t *size);
SZDStatus DecodeFreelist(const char *buffer, uint64_t buffer_size,
                         SZDFreeList **target, uint32_t *zones_free);

bool TESTFreeListsEqual(SZDFreeList *left, SZDFreeList *right);
} // namespace SZDFreeListFunctions
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif