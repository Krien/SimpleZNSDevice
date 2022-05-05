#include <gtest/gtest.h>
#include <szd/cpp/szd_device.h>
#include <szd/cpp/szd_status.h>

#include <vector>

namespace {

class SZDTest : public ::testing::Test {};

TEST_F(SZDTest, TestFillingCircularLog) {
  SZD::SZDDevice dev("GetInfo");
  ASSERT_EQ(dev.Init(), SZD::SZDStatus::Success);
  // Find and pick first device
  std::vector<SZD::DeviceOpenInfo> info;
  ASSERT_EQ(dev.Probe(info), SZD::SZDStatus::Success);
  std::string device_to_use = "None";
  for (auto it = info.begin(); it != info.end(); it++) {
    if (it->is_zns) {
      device_to_use.assign(it->traddr);
    }
  }
  ASSERT_EQ(dev.Open(device_to_use, 10, 15), SZD::SZDStatus::Success);

  SZD::DeviceInfo dinfo;
  ASSERT_EQ(dev.GetInfo(&dinfo), SZD::SZDStatus::Success);
  ASSERT_GT(dinfo.lba_cap, 0);
  ASSERT_GT(dinfo.lba_size, 0);
  ASSERT_GT(dinfo.max_lba, 0);
  ASSERT_GT(dinfo.mdts, 0);
  ASSERT_GT(dinfo.zasl, 0);
  ASSERT_EQ(dinfo.min_lba, 10 * dinfo.zone_size);
  ASSERT_EQ(dinfo.max_lba, 15 * dinfo.zone_size);

  ASSERT_EQ(dev.Destroy(), SZD::SZDStatus::Success);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
} // namespace