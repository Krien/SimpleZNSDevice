#include "szd_test_util.hpp"
#include <gtest/gtest.h>
#include <szd/szd.h>
#include <szd/szd_channel.hpp>
#include <szd/szd_channel_factory.hpp>
#include <szd/szd_device.hpp>
#include <szd/szd_status.hpp>

#include <string>
#include <vector>

namespace {

class SZDChannelTest : public ::testing::Test {};

static constexpr uint64_t begin_zone = 10;
static constexpr uint64_t end_zone = 15;
static_assert(begin_zone + 4 < end_zone);

TEST_F(SZDChannelTest, AllignmentTest) {
  SZD::SZDDevice dev("AllignmentTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);
  // 0 bytes
  ASSERT_EQ(channel->allign_size(0), 0);
  // 1 byte
  ASSERT_EQ(channel->allign_size(1), info.lba_size);
  // Below 1 lba
  ASSERT_EQ(channel->allign_size(info.lba_size - 10), info.lba_size);
  // Exactly 1 lba
  ASSERT_EQ(channel->allign_size(info.lba_size), info.lba_size);
  // A little more than 1 lba
  ASSERT_EQ(channel->allign_size(info.lba_size + 10), info.lba_size * 2);
  // go up to uint64max
  uint64_t max = ~0UL - info.lba_size - 1;
  ASSERT_EQ(channel->allign_size(max),
            (((max + info.lba_size - 1) / info.lba_size) * info.lba_size));
  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, DirectIO) {
  SZD::SZDDevice dev("DirectIO");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  // Have to reset for a clean state
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  // Create buffers. Test cases are alligned and have 1 zone + 2 lbas.
  uint64_t begin_lba = begin_zone * info.zone_cap;
  uint64_t write_head = begin_lba;
  uint64_t range = info.lba_size * info.zone_cap + info.lba_size * 2;
  SZDTestUtil::RAIICharBuffer bufferw(range+1);
  SZDTestUtil::RAIICharBuffer bufferr(range+1);
  SZDTestUtil::CreateCyclicPattern(bufferw.buff_, range, 0);

  // Write 1 zone and 2 lbas and verify if this data can be read.
  ASSERT_EQ(channel->DirectAppend(&write_head, bufferw.buff_, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, begin_lba + info.zone_cap + 2);
  ASSERT_EQ(channel->DirectRead(begin_lba, bufferr.buff_, range, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(bufferw.buff_, bufferr.buff_, range) == 0);

  // We should be able to append again
  ASSERT_EQ(channel->DirectAppend(&write_head, bufferw.buff_, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, begin_lba + 2 * (info.zone_cap + 2));
  ASSERT_EQ(
      channel->DirectRead(begin_lba + range / info.lba_size, bufferr.buff_, range, true),
      SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(bufferw.buff_, bufferr.buff_, range) == 0);

  // We can write in the last zone
  write_head = (end_zone - 1) * info.zone_cap;
  uint64_t smaller_range = info.lba_size * info.zone_cap;
  ASSERT_EQ(channel->DirectAppend(&write_head, bufferw.buff_, smaller_range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, end_zone * info.zone_cap);
  ASSERT_EQ(
      channel->DirectRead((end_zone - 1) * info.zone_cap, bufferr.buff_, smaller_range, true),
      SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(bufferw.buff_, bufferr.buff_, smaller_range) == 0);

  // We can not write to first zone anymore
  write_head = begin_lba;
  ASSERT_NE(channel->DirectAppend(&write_head, bufferw.buff_, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, begin_lba);

  // We can also not write out of bounds
  write_head = 0; // We assume begin_zone is > 0
  ASSERT_NE(channel->DirectAppend(&write_head, bufferw.buff_, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, 0);
  write_head = end_zone * info.zone_cap;
  ASSERT_NE(channel->DirectAppend(&write_head, bufferw.buff_, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, end_zone * info.zone_cap);
  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, DirectIONonAlligned) {
  SZD::SZDDevice dev("DirectIONonAlligned");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  // Have to reset before the test can start
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  // Create buffers for test, which are just two pages
  uint64_t range = info.lba_size * 2;
  SZDTestUtil::RAIICharBuffer bufferw(range+1);
  SZDTestUtil::RAIICharBuffer bufferr(range+1);
  memset(bufferr.buff_, 0, range);
  SZDTestUtil::CreateCyclicPattern(bufferw.buff_, range, 0);

  uint64_t begin_lba = begin_zone * info.zone_cap;
  uint64_t write_head = begin_lba;
  // When we say that we allign, we can not write unalligned
  ASSERT_NE(channel->DirectAppend(&write_head, bufferw.buff_,
                                  info.lba_size + info.lba_size - 10, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, begin_lba);
  // When we do not allign, it should succeed with padding
  ASSERT_EQ(channel->DirectAppend(&write_head, bufferw.buff_,
                                  info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, begin_lba + 2);

  // When we say that we allign, we can not read unalligned
  ASSERT_NE(channel->DirectRead(begin_lba, bufferr.buff_,
                                info.lba_size + info.lba_size - 10, true),
            SZD::SZDStatus::Success);
  // When we do not allign, it should succeed with padding
  ASSERT_EQ(channel->DirectRead(begin_lba, bufferr.buff_,
                                info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(bufferr.buff_, bufferw.buff_, info.lba_size + info.lba_size - 10) ==
              0);
  // Ensure that all padding are 0 bytes
  for (size_t i = info.lba_size + info.lba_size - 10; i < range; i++) {
    ASSERT_EQ(bufferr.buff_[i], 0);
  }

  // Reread and ensure that the padding written earlier is also 0 bytes
  ASSERT_EQ(channel->DirectRead(begin_lba, bufferr.buff_,
                                2 * info.lba_size, false),
            SZD::SZDStatus::Success);
  for (size_t i = info.lba_size + info.lba_size - 10; i < range; i++) {
    ASSERT_EQ(bufferr.buff_[i], 0);
  }

  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, BufferIO) {
  SZD::SZDDevice dev("BufferIO");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  // Have to reset device for a clean state
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  // Setup. We will create 3 equal sized parts. We flush the middle part.
  // Read it into the last, then flush a non-alligned area around the last 2
  // parts and read it into the first.
  SZD::SZDBuffer buffer(info.lba_size * 3, info.lba_size);
  char *raw_buffer = nullptr;
  ASSERT_EQ(buffer.GetBuffer((void **)&raw_buffer), SZD::SZDStatus::Success);
  ASSERT_NE(raw_buffer, nullptr);

  uint64_t range = info.lba_size;
  SZDTestUtil::CreateCyclicPattern(raw_buffer + range, range, 0);

  uint64_t start_head = begin_zone * info.zone_cap;
  uint64_t write_head = start_head;
  ASSERT_EQ(channel->FlushBufferSection(&write_head, buffer, range /*addr*/, range /*size*/, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ReadIntoBuffer(start_head, &buffer, 2 * range /*addr*/, range /*size*/, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(
      memcmp(raw_buffer + range, raw_buffer + range * 2, info.lba_size) == 0);

  start_head = write_head;
  ASSERT_NE(channel->FlushBufferSection(&write_head, buffer,
                                        range + info.lba_size - 10,
                                        info.lba_size - 40, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->FlushBufferSection(&write_head, buffer,
                                        range + info.lba_size - 10,
                                        info.lba_size - 40, false),
            SZD::SZDStatus::Success);
  ASSERT_NE(
      channel->ReadIntoBuffer(start_head, &buffer, 10, info.lba_size - 49, true),
      SZD::SZDStatus::Success);
  ASSERT_EQ(
      channel->ReadIntoBuffer(start_head, &buffer, 10, info.lba_size - 49, false),
      SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(raw_buffer + 10, raw_buffer + range + info.lba_size - 10,
                     info.lba_size - 49) == 0);

  // Full alligned
  start_head = write_head;
  ASSERT_EQ(channel->FlushBuffer(&write_head, buffer), SZD::SZDStatus::Success);
  SZD::SZDBuffer shadow_buffer(info.lba_size * 3, info.lba_size);
  char *raw_shadow_buffer;
  ASSERT_EQ(shadow_buffer.GetBuffer((void **)&raw_shadow_buffer),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ReadIntoBuffer(start_head, &shadow_buffer, 0, range * 3, true),
            SZD::SZDStatus::Success);

  ASSERT_TRUE(memcmp(raw_buffer, raw_shadow_buffer, range * 3) == 0);
  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, ResetRespectsRange) {
  SZD::SZDDevice dev("ResetRespectsRange");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 4);
  SZD::SZDChannel *channel, *channel1, *channel2, *channel3;
  factory.register_channel(&channel);
  factory.register_channel(&channel1, begin_zone, begin_zone + 1, false, 1);
  factory.register_channel(&channel2, begin_zone + 1, begin_zone + 2, false, 1);
  factory.register_channel(&channel3, begin_zone + 2, begin_zone + 3, false, 1);

  // Have to reset device for a clean state.
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  // Flood first zone with (in)correct channels.
  uint64_t range = info.lba_size * 2;
  SZDTestUtil::RAIICharBuffer bufferw(range + 1);
  SZDTestUtil::CreateCyclicPattern(bufferw.buff_, range, 0);
  uint64_t first_start_head = begin_zone * info.zone_cap;
  uint64_t first_write_head = first_start_head;
  ASSERT_NE(channel2->DirectAppend(&first_write_head, bufferw.buff_,
                                  info.lba_size * 2 - 10, false),
            SZD::SZDStatus::Success);
  ASSERT_NE(channel3->DirectAppend(&first_write_head, bufferw.buff_,
                                  info.lba_size * 2 - 10, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(first_write_head, first_start_head);
  ASSERT_EQ(channel1->DirectAppend(&first_write_head, bufferw.buff_,
                                  info.lba_size * 2 - 10, false),
            SZD::SZDStatus::Success);

  // Flood third zone with (in)correct channels.
  uint64_t third_start_head = (begin_zone + 3) * info.zone_cap;
  uint64_t third_write_head = third_start_head;
  ASSERT_NE(channel1->DirectAppend(&third_write_head, bufferw.buff_,
                                  info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);
  ASSERT_NE(channel2->DirectAppend(&third_write_head, bufferw.buff_,
                                  info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);  
  ASSERT_EQ(channel3->DirectAppend(&third_write_head, bufferw.buff_,
                                  info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);

  // Reset middle zone.
  ASSERT_EQ(channel2->ResetAllZones(), SZD::SZDStatus::Success);

  // Check if the reset did not affect the surrounding area.
  uint64_t zhead;
  ASSERT_EQ(channel1->ZoneHead(first_start_head, &zhead), SZD::SZDStatus::Success);
  ASSERT_EQ(zhead, first_start_head + 2);
  ASSERT_EQ(channel3->ZoneHead(third_start_head, &zhead), SZD::SZDStatus::Success);
  ASSERT_EQ(zhead, third_start_head + 2);

  factory.unregister_channel(channel);
  factory.unregister_channel(channel1);
  factory.unregister_channel(channel2);
  factory.unregister_channel(channel3);
}

} // namespace
