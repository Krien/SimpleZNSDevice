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
SZDStatus AllocZones(std::vector<SZDFreeList *> &zone_regions,
                     SZDFreeList **from, uint64_t requested_zones);

char *EncodeFreelist(SZDFreeList *target);
void DecodeFreelist();
} // namespace SZDFreeListFunctions
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif