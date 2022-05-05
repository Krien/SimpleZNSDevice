#include <gtest/gtest.h>
#include <szd/cpp/szd_device.hpp>
#include <szd/cpp/szd_status.hpp>

#include <vector>

namespace {

class SZDTest : public ::testing::Test {};

TEST_F(SZDTest, OpenAndClosing) {
  SZD::SZDDevice dev("OpenAndClosing");
  ASSERT_EQ(dev.Init(), SZD::SZDStatus::Success);
  std::vector<SZD::DeviceOpenInfo> info;
  ASSERT_EQ(dev.Probe(info), SZD::SZDStatus::Success);
  for (auto it = info.begin(); it != info.end(); it++) {
    if (it->is_zns) {
      ASSERT_EQ(dev.Open(it->traddr), SZD::SZDStatus::Success);
      ASSERT_EQ(dev.Close(), SZD::SZDStatus::Success);
    } else {
      ASSERT_NE(dev.Open(it->traddr), SZD::SZDStatus::Success);
    }
  }
  ASSERT_EQ(dev.Destroy(), SZD::SZDStatus::Success);
}

TEST_F(SZDTest, OpenAndClosing2) {
  SZD::SZDDevice dev("ForgetToInit");
  ASSERT_EQ(dev.Init(), SZD::SZDStatus::Success);
  std::vector<SZD::DeviceOpenInfo> info;
  ASSERT_EQ(dev.Probe(info), SZD::SZDStatus::Success);
  for (auto it = info.begin(); it != info.end(); it++) {
    if (it->is_zns) {
      ASSERT_EQ(dev.Open(it->traddr), SZD::SZDStatus::Success);
      ASSERT_EQ(dev.Close(), SZD::SZDStatus::Success);
    } else {
      ASSERT_NE(dev.Open(it->traddr), SZD::SZDStatus::Success);
    }
  }
  ASSERT_EQ(dev.Destroy(), SZD::SZDStatus::Success);
}

TEST_F(SZDTest, InvalidDevice) {
  SZD::SZDDevice dev("InvalidDevice");
  ASSERT_EQ(dev.Init(), SZD::SZDStatus::Success);
  ASSERT_NE(dev.Open("InvalidDevice"), SZD::SZDStatus::Success);
  ASSERT_EQ(dev.Destroy(), SZD::SZDStatus::Success);
}

TEST_F(SZDTest, OrderMisuse) {
  SZD::SZDDevice dev("OrderMisuse");
  // All but init should be illegal
  SZD::DeviceInfo dinfo;
  ASSERT_NE(dev.Reinit(), SZD::SZDStatus::Success);
  ASSERT_NE(dev.Close(), SZD::SZDStatus::Success);
  ASSERT_NE(dev.GetInfo(&dinfo), SZD::SZDStatus::Success);
  ASSERT_NE(dev.Destroy(), SZD::SZDStatus::Success);
  // Step 1
  ASSERT_EQ(dev.Init(), SZD::SZDStatus::Success);
  // Try breaking
  ASSERT_NE(dev.Close(), SZD::SZDStatus::Success);
  ASSERT_NE(dev.GetInfo(&dinfo), SZD::SZDStatus::Success);
  // Test reinit
  ASSERT_EQ(dev.Reinit(), SZD::SZDStatus::Success);

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
  ASSERT_NE(dev.Open(device_to_use, 10, 15), SZD::SZDStatus::Success);

  ASSERT_EQ(dev.GetInfo(&dinfo), SZD::SZDStatus::Success);
  ASSERT_GT(dinfo.lba_cap, 0);
  ASSERT_GT(dinfo.lba_size, 0);
  ASSERT_GT(dinfo.max_lba, 0);
  ASSERT_GT(dinfo.mdts, 0);
  ASSERT_GT(dinfo.zasl, 0);
  ASSERT_EQ(dinfo.min_lba, 10 * dinfo.zone_size);
  ASSERT_EQ(dinfo.max_lba, 15 * dinfo.zone_size);

  // Now close and test
  ASSERT_EQ(dev.Close(), SZD::SZDStatus::Success);
  ASSERT_NE(dev.GetInfo(&dinfo), SZD::SZDStatus::Success);
  // Destroy and test again
  ASSERT_EQ(dev.Destroy(), SZD::SZDStatus::Success);
  ASSERT_NE(dev.Reinit(), SZD::SZDStatus::Success);
  ASSERT_NE(dev.Close(), SZD::SZDStatus::Success);
  ASSERT_NE(dev.GetInfo(&dinfo), SZD::SZDStatus::Success);
  ASSERT_NE(dev.Destroy(), SZD::SZDStatus::Success);
}

TEST_F(SZDTest, TestValidInfo) {
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