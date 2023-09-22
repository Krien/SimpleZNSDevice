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

static constexpr size_t needed_channels_for_fragmented_log = 2;
static constexpr uint64_t begin_zone = 10;
static constexpr uint64_t end_zone = 15;

TEST_F(SZDTest, FillingFragmentedLogSimpleTest) {
  SZD::SZDDevice dev("FillingFragmentedLogSimpleTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetEngineManager(), needed_channels_for_fragmented_log);
  SZD::SZDFragmentedLog log(factory, info, begin_zone, end_zone, 1, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(0), SZD::SZDStatus::Success);
  // Test empty state
  uint64_t range = (end_zone - begin_zone) * info.zone_cap * info.lba_size;
  ASSERT_EQ(log.SpaceAvailable(), range);
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(range));
  ASSERT_EQ(log.SpaceAvailable(), range);

  SZDTestUtil::RAIICharBuffer buff(range);
  memset(buff.buff_, 0, sizeof(buff));

  // Write and read one entry
  std::vector<std::pair<uint64_t, uint64_t>> regions;
  ASSERT_EQ(log.Append("TEST", sizeof("TEST"), regions, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(regions.size(), 1);
  ASSERT_EQ(log.Read(regions, buff.buff_, info.lba_size, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff.buff_, "TEST", sizeof("TEST")) == 0);
  ASSERT_FALSE(log.Empty());
  // !, yes a small write claims an entire zone in this design...
  ASSERT_TRUE(log.SpaceLeft(range - info.lba_size * info.zone_cap));
  ASSERT_EQ(log.SpaceAvailable(), range - info.lba_size * info.zone_cap);
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Fill rest of device
  std::vector<std::pair<uint64_t, uint64_t>> regions_full;
  SZDTestUtil::CreateCyclicPattern(buff.buff_, range, 0);
  ASSERT_EQ(log.Append(buff.buff_, range - info.lba_size * info.zone_cap,
                       regions_full, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(log.TESTEncodingDecoding());

  size_t read_range = range - info.lba_size * info.zone_cap;
  SZDTestUtil::RAIICharBuffer buffr(read_range);
  ASSERT_EQ(log.Read(regions, buffr.buff_, read_range, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buffr.buff_, "TEST", sizeof("TEST")) == 0);
  for (size_t i = 0; i < read_range - sizeof("TEST"); i++) {
    ASSERT_EQ(buffr.buff_[i + sizeof("TEST")], 0);
  }
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(!log.SpaceLeft(1, false));
  ASSERT_EQ(log.SpaceAvailable(), 0);

  // Reset parts and see how space decreases
  ASSERT_EQ(log.Reset(regions, 0), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(info.lba_size * info.zone_cap));
  ASSERT_EQ(log.SpaceAvailable(), info.lba_size * info.zone_cap);
  ASSERT_FALSE(log.Empty());
  ASSERT_EQ(log.Reset(regions_full, 0), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(range));
  ASSERT_EQ(log.SpaceAvailable(), range);
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.TESTEncodingDecoding());
}

TEST_F(SZDTest, FillingFragmentedLogFragmentingTest) {
  SZD::SZDDevice dev("FillingFragmentedLogFragmentingTest");
  SZD::DeviceInfo info;
  static constexpr uint64_t further_end_zone = 19;
  SZDTestUtil::SZDSetupDevice(begin_zone, further_end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetEngineManager(), needed_channels_for_fragmented_log);
  SZD::SZDFragmentedLog log(factory, info, begin_zone, further_end_zone, 1, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(0), SZD::SZDStatus::Success);

  // Create 3 regions of each 3 zones.
  size_t size1 = 1 * info.lba_size * info.zone_cap;
  size_t size2 = 2 * info.lba_size * info.zone_cap;
  size_t size3 = 3 * info.lba_size * info.zone_cap;
  size_t size4 = 4 * info.lba_size * info.zone_cap;
  SZDTestUtil::RAIICharBuffer first_buff(size3);
  SZDTestUtil::RAIICharBuffer mid_buff(size3);
  SZDTestUtil::RAIICharBuffer last_buff(size3);

  SZDTestUtil::CreateCyclicPattern(first_buff.buff_, size3, 0);
  SZDTestUtil::CreateCyclicPattern(mid_buff.buff_, size3, 10); // other pattern!
  SZDTestUtil::CreateCyclicPattern(last_buff.buff_, size3,
                                   15); // other pattern!
  std::vector<std::pair<uint64_t, uint64_t>> first_regions;
  std::vector<std::pair<uint64_t, uint64_t>> mid_regions;
  std::vector<std::pair<uint64_t, uint64_t>> last_regions;
  ASSERT_EQ(log.Append(first_buff.buff_, size3, first_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Append(mid_buff.buff_, size3, mid_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Append(last_buff.buff_, size3, last_regions, true),
            SZD::SZDStatus::Success);

  // No more space.
  ASSERT_TRUE(!log.SpaceLeft(1, false));
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Delete middle region and try setting "2" regions, which should succeed.
  ASSERT_EQ(log.Reset(mid_regions, 0), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(size3));
  ASSERT_EQ(log.SpaceAvailable(), size3);
  mid_regions.clear();
  ASSERT_EQ(log.Append(mid_buff.buff_, size2, mid_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(size1));
  ASSERT_EQ(log.SpaceAvailable(), size1);

  // Try setting 3 regions which should fail.
  std::vector<std::pair<uint64_t, uint64_t>> stub_regions;
  ASSERT_NE(log.Append(mid_buff.buff_, size3, stub_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Delete first region and set region of 4 zones, this should succeed.
  ASSERT_EQ(log.Reset(first_regions, 0), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(size4));
  ASSERT_EQ(log.SpaceAvailable(), size4);
  ASSERT_TRUE(log.TESTEncodingDecoding());
  SZDTestUtil::RAIICharBuffer final_buff(size4);
  SZDTestUtil::CreateCyclicPattern(final_buff.buff_, size4, 0);
  first_regions.clear();
  ASSERT_EQ(log.Append(final_buff.buff_, size4, first_regions, true),
            SZD::SZDStatus::Success);
  // Filled again
  ASSERT_TRUE(!log.SpaceLeft(1, false));
  ASSERT_EQ(log.SpaceAvailable(), 0);
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Ensure content is consistent
  SZDTestUtil::RAIICharBuffer read_buff(size4);
  ASSERT_EQ(log.Read(last_regions, read_buff.buff_, size3, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(read_buff.buff_, last_buff.buff_, size3) == 0);
  ASSERT_EQ(log.Read(mid_regions, read_buff.buff_, size2, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(read_buff.buff_, mid_buff.buff_, size2) == 0);
  ASSERT_EQ(log.Read(first_regions, read_buff.buff_, size4, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(read_buff.buff_, final_buff.buff_, size4) == 0);

  // Delete all regions and see the cascading effect of merges... (heap
  // massaging). Force by getting two free zones adjacent to a full one and then
  // delete the full one.
  ASSERT_EQ(log.Reset(mid_regions, 0), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(size2));
  ASSERT_EQ(log.SpaceAvailable(), size2);
  ASSERT_TRUE(log.TESTEncodingDecoding());
  ASSERT_EQ(log.Reset(first_regions, 0),
            SZD::SZDStatus::Success); // remember that first now surrounds mid..
  ASSERT_TRUE(log.SpaceLeft(size2 + size4));
  ASSERT_EQ(log.SpaceAvailable(), size2 + size4);
  ASSERT_TRUE(log.TESTEncodingDecoding());
  ASSERT_EQ(log.Reset(last_regions, 0), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.TESTEncodingDecoding());

  // Fill entire device one last time for good measure...
  uint64_t total_range =
      (further_end_zone - begin_zone) * info.zone_cap * info.lba_size;
  SZDTestUtil::RAIICharBuffer total_buff(total_range);
  SZDTestUtil::CreateCyclicPattern(total_buff.buff_, total_range, 0);
  first_regions.clear(); // Not used anymore, so why not?
  ASSERT_EQ(log.Append(total_buff.buff_, total_range, first_regions, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(log.TESTEncodingDecoding());
  SZDTestUtil::RAIICharBuffer total_buff_read(total_range);
  ASSERT_EQ(log.Read(first_regions, total_buff_read.buff_, total_range, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(total_buff_read.buff_, total_buff.buff_, total_range) ==
              0);
  ASSERT_TRUE(!log.SpaceLeft(1, false));
  ASSERT_EQ(log.SpaceAvailable(), 0);

  // Destroy all
  ASSERT_EQ(log.ResetAll(0), SZD::SZDStatus::Success);
  ASSERT_TRUE(log.SpaceLeft(total_range, true));
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.TESTEncodingDecoding());
}

TEST_F(SZDTest, MultipleWritersFragmentedLogFragmentingTest) {
  SZD::SZDDevice dev("MultipleWritersFragmentedLogFragmentingTest");
  SZD::DeviceInfo info;
  static constexpr uint64_t further_end_zone = 19;
  SZDTestUtil::SZDSetupDevice(begin_zone, further_end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetEngineManager(), needed_channels_for_fragmented_log + 1);
  SZD::SZDFragmentedLog log(factory, info, begin_zone, further_end_zone, 1, 2);

  // Reset first
  ASSERT_EQ(log.ResetAll(0), SZD::SZDStatus::Success);

  uint64_t range = info.lba_size * 2;
  SZDTestUtil::RAIICharBuffer bufferw(range + 1);
  SZDTestUtil::CreateCyclicPattern(bufferw.buff_, range, 0);

  // Add some data
  std::vector<std::pair<uint64_t, uint64_t>> regions1, regions2;
  ASSERT_EQ(log.Append(bufferw.buff_, range, regions1, true, 0),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Append(bufferw.buff_, range, regions2, true, 1),
            SZD::SZDStatus::Success);
}

} // namespace
