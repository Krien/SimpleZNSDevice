#include "szd_test_util.hpp"
#include <gtest/gtest.h>
#include <szd/datastructures/szd_fragmented_log.hpp>
#include <szd/datastructures/szd_freezone_list.hpp>
#include <szd/szd_channel.hpp>
#include <szd/szd_channel_factory.hpp>
#include <szd/szd_device.hpp>
#include <szd/szd_status.hpp>

#include <vector>

namespace {

class SZDTest : public ::testing::Test {};

TEST_F(SZDTest, TestFillingFragmentedLogSimple) {
  SZD::SZDDevice dev("TestFillingACircularLogEphemerally");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(10, 15, &dev, &info);
  SZD::SZDChannelFactory *factory =
      new SZD::SZDChannelFactory(dev.GetDeviceManager(), 2);
  SZD::SZDFragmentedLog log(factory, info, 10, 15);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  // Test empty state
  uint64_t range = (15 - 10) * info.zone_cap * info.lba_size;
  ASSERT_EQ(log.SpaceAvailable(), range);
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(range));
  char buff[range];
  memset(buff, 0, sizeof(buff));

  // Write and read one entry
  std::vector<std::pair<uint64_t, uint64_t>> regions;
  ASSERT_EQ(log.Append("TEST", sizeof("TEST"), regions, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(regions.size(), 1);
  ASSERT_EQ(log.Read(regions, buff, info.lba_size, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff, "TEST", sizeof("TEST")) == 0);
  ASSERT_FALSE(log.Empty());
  // !, yes a small write claims an entire zone in this design...
  ASSERT_TRUE(log.SpaceLeft(range - info.lba_size * info.zone_cap));
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Fill rest of device
  std::vector<std::pair<uint64_t, uint64_t>> regions_full;
  SZDTestUtil::CreateCyclicPattern(buff, range, 0);
  ASSERT_EQ(log.Append(buff, range - info.lba_size * info.zone_cap,
                       regions_full, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // on heap because of stack limitations..
  char *buffr = new char[range - info.lba_size * info.zone_cap];
  ASSERT_EQ(
      log.Read(regions, buffr, range - info.lba_size * info.zone_cap, true),
      SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buffr, buffr, range - info.lba_size * info.zone_cap) == 0);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(!log.SpaceLeft(1, false));
  delete[] buffr;

  // Reset parts and see how space decreases
  ASSERT_EQ(log.Reset(regions), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(info.lba_size * info.zone_cap));
  ASSERT_FALSE(log.Empty());
  ASSERT_EQ(log.Reset(regions_full), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(range));
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.TESTEncodingDecoding());
}

TEST_F(SZDTest, TestFillingFragmentedLogFragmenting) {
  SZD::SZDDevice dev("TestFillingACircularLogEphemerally");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(10, 19, &dev, &info);
  SZD::SZDChannelFactory *factory =
      new SZD::SZDChannelFactory(dev.GetDeviceManager(), 2);
  SZD::SZDFragmentedLog log(factory, info, 10, 19);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);

  // Create 3 regions of each 3 zones.
  size_t size1 = 1 * info.lba_size * info.zone_cap;
  size_t size2 = 2 * info.lba_size * info.zone_cap;
  size_t size3 = 3 * info.lba_size * info.zone_cap;
  size_t size4 = 4 * info.lba_size * info.zone_cap;
  char *first_buff = new char[size3];
  char *mid_buff = new char[size3];
  char *last_buff = new char[size3];
  SZDTestUtil::CreateCyclicPattern(first_buff, size3, 0);
  SZDTestUtil::CreateCyclicPattern(mid_buff, size3, 10);  // other pattern!
  SZDTestUtil::CreateCyclicPattern(last_buff, size3, 15); // other pattern!
  std::vector<std::pair<uint64_t, uint64_t>> first_regions;
  std::vector<std::pair<uint64_t, uint64_t>> mid_regions;
  std::vector<std::pair<uint64_t, uint64_t>> last_regions;
  ASSERT_EQ(log.Append(first_buff, size3, first_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Append(mid_buff, size3, mid_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Append(last_buff, size3, last_regions, true),
            SZD::SZDStatus::Success);

  // No more space.
  ASSERT_TRUE(!log.SpaceLeft(1, false));
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Delete middle region and try setting "2" regions, which should succeed.
  ASSERT_EQ(log.Reset(mid_regions), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(size3));
  mid_regions.clear();
  ASSERT_EQ(log.Append(mid_buff, size2, mid_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(size1));

  // Try setting 3 regions which should fail.
  std::vector<std::pair<uint64_t, uint64_t>> stub_regions;
  ASSERT_NE(log.Append(mid_buff, size3, stub_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Delete first region and set region of 4 zones, this should succeed.
  ASSERT_EQ(log.Reset(first_regions), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(size4));
  ASSERT_TRUE(log.TESTEncodingDecoding());
  char *final_buff = new char[size4];
  SZDTestUtil::CreateCyclicPattern(final_buff, size4, 0);
  first_regions.clear();
  ASSERT_EQ(log.Append(final_buff, size4, first_regions, true),
            SZD::SZDStatus::Success);
  // Filled again
  ASSERT_TRUE(!log.SpaceLeft(1, false));
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Ensure content is consistent
  char *read_buff = new char[size4];
  ASSERT_EQ(log.Read(last_regions, read_buff, size3, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(read_buff, last_buff, size3) == 0);
  ASSERT_EQ(log.Read(mid_regions, read_buff, size2, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(read_buff, mid_buff, size2) == 0);
  ASSERT_EQ(log.Read(first_regions, read_buff, size4, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(read_buff, final_buff, size4) == 0);

  // Delete all regions and see the cascading effect of merges... (heap
  // massaging). Force by getting two free zones adjacent to a full one and then
  // delete the full one.
  ASSERT_EQ(log.Reset(mid_regions), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(size2));
  ASSERT_TRUE(log.TESTEncodingDecoding());
  ASSERT_EQ(log.Reset(first_regions),
            SZD::SZDStatus::Success); // remember that first now surrounds mid..
  ASSERT_TRUE(log.SpaceLeft(size2 + size4));
  ASSERT_TRUE(log.TESTEncodingDecoding());
  ASSERT_EQ(log.Reset(last_regions), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Memory cleanup...
  delete[] first_buff;
  delete[] mid_buff;
  delete[] last_buff;
  delete[] final_buff;
  delete[] read_buff;

  // Fill entire device one last time for good measure...
  uint64_t total_range = (19 - 10) * info.zone_cap * info.lba_size;
  char *total_buff = new char[total_range];
  SZDTestUtil::CreateCyclicPattern(total_buff, total_range, 0);
  first_regions.clear(); // Not used anymore, so why not?
  ASSERT_EQ(log.Append(total_buff, total_range, first_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(log.TESTEncodingDecoding());
  char *total_buff_read = new char[total_range];
  ASSERT_EQ(log.Read(first_regions, total_buff_read, total_range, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(total_buff_read, total_buff, total_range) == 0);
  ASSERT_TRUE(!log.SpaceLeft(1, false));

  // Destroy all
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(total_range, true));
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.TESTEncodingDecoding());

  delete[] total_buff;
  delete[] total_buff_read;
}

} // namespace
