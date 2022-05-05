#include <gtest/gtest.h>
#include <szd/cpp/szd_channel.h>
#include <szd/cpp/szd_channel_factory.h>
#include <szd/cpp/szd_device.h>
#include <szd/cpp/szd_status.h>
#include <szd/szd.h>

#include <string>
#include <vector>

namespace {

class SZDChannelTest : public ::testing::Test {};

void CreatePattern(char *arr, size_t range, uint64_t jump) {
  for (size_t i = 0; i < range; i++) {
    arr[i] = (i + jump) % 256;
  }
}

static void szdsetup(uint64_t min_zone, uint64_t max_zone,
                     SZD::SZDDevice *device, SZD::DeviceInfo *dinfo) {
  ASSERT_EQ(device->Init(), SZD::SZDStatus::Success);
  std::vector<SZD::DeviceOpenInfo> info;
  ASSERT_EQ(device->Probe(info), SZD::SZDStatus::Success);
  std::string device_to_use = "None";
  for (auto it = info.begin(); it != info.end(); it++) {
    if (it->is_zns) {
      device_to_use.assign(it->traddr);
    }
  }
  ASSERT_EQ(device->Open(device_to_use, min_zone, max_zone),
            SZD::SZDStatus::Success);
  ASSERT_EQ(device->GetInfo(dinfo), SZD::SZDStatus::Success);
}

TEST_F(SZDChannelTest, AllignmentTest) {
  SZD::SZDDevice dev("AllignmentTest");
  SZD::DeviceInfo info;
  szdsetup(10, 15, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);
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
  szdsetup(10, 15, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  // Have to reset
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  // Alligned (1 zone + 2 lbas)
  uint64_t slba = 10 * info.zone_size;
  uint64_t range = info.lba_size * info.zone_size + info.lba_size * 2;
  char bufferw[range + 1];
  char bufferr[range + 1];
  CreatePattern(bufferw, range, 0);
  uint64_t wslba = slba;

  ASSERT_EQ(channel->DirectAppend(&wslba, bufferw, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(wslba, slba + info.zone_size + 2);
  ASSERT_EQ(channel->DirectRead(slba, bufferr, range, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(bufferw, bufferr, range) == 0);

  // Can append again
  ASSERT_EQ(channel->DirectAppend(&wslba, bufferw, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(wslba, slba + 2 * (info.zone_size + 2));
  ASSERT_EQ(
      channel->DirectRead(slba + range / info.lba_size, bufferr, range, true),
      SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(bufferw, bufferr, range) == 0);

  // Can not write to first zone anymore
  uint64_t wslba2 = slba;
  ASSERT_NE(channel->DirectAppend(&wslba2, bufferw, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(wslba2, slba);
  // Can not write out of bounds
  wslba2 = 0;
  ASSERT_NE(channel->DirectAppend(&wslba2, bufferw, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(wslba2, 0);
  wslba2 = 15 * info.zone_size;
  ASSERT_NE(channel->DirectAppend(&wslba2, bufferw, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(wslba2, 15 * info.zone_size);
  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, DirectIONonAlligned) {
  SZD::SZDDevice dev("DirectIONonAlligned");
  SZD::DeviceInfo info;
  szdsetup(10, 15, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  // Have to reset
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  uint64_t range = info.lba_size * 2;
  char bufferw[range + 1];
  char bufferr[range + 1];
  memset(bufferr, 0, range);
  CreatePattern(bufferw, range, 0);

  uint64_t wslba = 10 * info.zone_size;
  uint64_t slba = wslba;
  ASSERT_NE(channel->DirectAppend(&wslba, bufferw,
                                  info.lba_size + info.lba_size - 10, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(wslba, slba);
  ASSERT_EQ(channel->DirectAppend(&wslba, bufferw,
                                  info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(wslba, slba + 2);

  ASSERT_NE(channel->DirectRead(slba, bufferr,
                                info.lba_size + info.lba_size - 10, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->DirectRead(slba, bufferr,
                                info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(bufferr, bufferw, info.lba_size + info.lba_size - 10) ==
              0);
  for (size_t i = info.lba_size + info.lba_size - 10; i < range; i++) {
    ASSERT_EQ(bufferr[i], 0);
  }
  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, BufferIO) {
  SZD::SZDDevice dev("BufferIO");
  SZD::DeviceInfo info;
  szdsetup(10, 15, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  // Have to reset
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  // Setup. We will create 3 equal sized parts. We flush the middle part.
  // Read it into the last, then flush a non-alligned area around the last 2
  // parts and read it into the first.
  SZD::SZDBuffer buffer(info.lba_size * 3, info.lba_size);
  char *raw_buffer;
  ASSERT_EQ(buffer.GetBuffer((void **)&raw_buffer), SZD::SZDStatus::Success);

  uint64_t range = info.lba_size;
  CreatePattern(raw_buffer + range, range, 0);

  uint64_t wslba = 10 * info.zone_size;
  uint64_t slba = wslba;
  ASSERT_EQ(channel->FlushBufferSection(&wslba, buffer, range, range, true),
            SZD::SZDStatus::Success);

  ASSERT_EQ(channel->ReadIntoBuffer(slba, &buffer, range + range, range, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(
      memcmp(raw_buffer + range, raw_buffer + range * 2, info.lba_size) == 0);

  slba = wslba;
  ASSERT_NE(channel->FlushBufferSection(&wslba, buffer,
                                        range + info.lba_size - 10,
                                        info.lba_size - 40, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->FlushBufferSection(&wslba, buffer,
                                        range + info.lba_size - 10,
                                        info.lba_size - 40, false),
            SZD::SZDStatus::Success);

  ASSERT_NE(
      channel->ReadIntoBuffer(slba, &buffer, 10, info.lba_size - 49, true),
      SZD::SZDStatus::Success);
  ASSERT_EQ(
      channel->ReadIntoBuffer(slba, &buffer, 10, info.lba_size - 49, false),
      SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(raw_buffer + 10, raw_buffer + range + info.lba_size - 10,
                     info.lba_size - 49) == 0);

  // Full alligned
  slba = wslba;
  ASSERT_EQ(channel->FlushBuffer(&wslba, buffer), SZD::SZDStatus::Success);
  SZD::SZDBuffer shadow_buffer(info.lba_size * 3, info.lba_size);
  char *raw_shadow_buffer;
  ASSERT_EQ(shadow_buffer.GetBuffer((void **)&raw_shadow_buffer),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ReadIntoBuffer(slba, &shadow_buffer, 0, range * 3, true),
            SZD::SZDStatus::Success);

  ASSERT_TRUE(memcmp(raw_buffer, raw_shadow_buffer, range * 3) == 0);
  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, ResetRespectsRange) {
  SZD::SZDDevice dev("BufferIO");
  SZD::DeviceInfo info;
  szdsetup(10, 15, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 4);
  SZD::SZDChannel *channel, *channel1, *channel2, *channel3;
  factory.register_channel(&channel);
  factory.register_channel(&channel1, 10, 11);
  factory.register_channel(&channel2, 11, 12);
  factory.register_channel(&channel3, 13, 14);

  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  // Flood adjacent zones
  uint64_t range = info.lba_size * 2;
  char bufferw[range + 1];
  CreatePattern(bufferw, range, 0);
  uint64_t first_wslba = 10 * info.zone_size;
  u_int64_t first_slba = first_wslba;
  ASSERT_EQ(channel->DirectAppend(&first_wslba, bufferw,
                                  info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);

  uint64_t third_wslba = 13 * info.zone_size;
  uint64_t third_slba = third_wslba;
  ASSERT_EQ(channel->DirectAppend(&third_wslba, bufferw,
                                  info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);

  // Reset middle.
  ASSERT_EQ(channel2->ResetAllZones(), SZD::SZDStatus::Success);

  // Check if the reset did not affect the surrounding area.
  uint64_t zhead;
  ASSERT_EQ(channel1->ZoneHead(first_slba, &zhead), SZD::SZDStatus::Success);
  ASSERT_EQ(zhead, first_slba + 2);
  ASSERT_EQ(channel3->ZoneHead(third_slba, &zhead), SZD::SZDStatus::Success);
  ASSERT_EQ(zhead, third_slba + 2);

  factory.unregister_channel(channel);
  factory.unregister_channel(channel1);
  factory.unregister_channel(channel2);
  factory.unregister_channel(channel3);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
} // namespace
