/** \file
 * SZD channel factory, allowing channels to be created at one point only.
 * */
#pragma once
#ifndef SZD_CPP_CHANNEL_FACTORY_H
#define SZD_CPP_CHANNEL_FACTORY_H

#include "szd/szd.h"
#include "szd/szd_channel.hpp"
#include "szd/szd_status.hpp"

namespace SIMPLE_ZNS_DEVICE_NAMESPACE {
/**
 * @brief Simple class meant to ensure that SZD channels are created at one
 * point. Allowing limiting the amount of channels and abstracting away
 * complexity.
 */
class SZDChannelFactory {
public:
  SZDChannelFactory(SZD::EngineManager *em, size_t max_channel_count);
  ~SZDChannelFactory();
  // No copying or implicits
  SZDChannelFactory(const SZDChannelFactory &) = delete;
  SZDChannelFactory &operator=(const SZDChannelFactory &) = delete;

  inline void Ref() { ++refs_; }
  inline void Unref() {
    if (--refs_ <= 0) {
      delete this;
    }
  }
  inline size_t Getref() { return refs_; }

  SZDStatus register_raw_qpair(QPair **qpair);
  SZDStatus unregister_raw_qpair(QPair *qpair);
  SZDStatus register_channel(SZDChannel **channel,
                             bool preserve_async_buffer = false,
                             uint32_t channel_depth = 1);
  SZDStatus register_channel(SZDChannel **channel, uint64_t min_zone_nr,
                             uint64_t max_zone_nr,
                             bool preserve_async_buffer = false,
                             uint32_t channel_depth = 1);
  SZDStatus unregister_channel(SZDChannel *channel);

private:
  size_t max_channel_count_;
  size_t channel_count_;
  EngineManager *em_;
  size_t refs_;
};
} // namespace SIMPLE_ZNS_DEVICE_NAMESPACE

#endif
