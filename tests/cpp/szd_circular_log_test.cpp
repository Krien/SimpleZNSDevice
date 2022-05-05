#include "szd_test_util.h"
#include <gtest/gtest.h>
#include <szd/cpp/datastructures/szd_circular_log.h>
#include <szd/cpp/szd_channel.h>
#include <szd/cpp/szd_channel_factory.h>
#include <szd/cpp/szd_device.h>
#include <szd/cpp/szd_status.h>

#include <vector>

namespace {

class SZDTest : public ::testing::Test {};

TEST_F(SZDTest, TestFillingACircularLogEphemerally) {
  SZD::SZDDevice dev("TestFillingACircularLogEphemerally");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(10, 15, &dev, &info);
  SZD::SZDChannelFactory *factory =
      new SZD::SZDChannelFactory(dev.GetDeviceManager(), 1);
  SZD::SZDCircularLog log(factory, info, 11, 13);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
  // Test empty state
  ASSERT_EQ(log.GetWriteHead(), 11 * info.zone_size);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_size);
  ASSERT_TRUE(log.Empty());
  uint64_t range = (13 - 11) * info.zone_size * info.lba_size;
  ASSERT_TRUE(log.SpaceLeft(range));
  char buff[range];
  memset(buff, 0, sizeof(buff));

  // We can not read when nothing is written
  uint64_t slba = 11 * info.zone_size;
  ASSERT_NE(log.Read(slba, buff, info.lba_size, true), SZD::SZDStatus::Success);

  // Write and read one entry
  uint64_t blocks;
  ASSERT_EQ(log.Append("TEST", sizeof("TEST"), &blocks, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, 1);
  ASSERT_EQ(log.Read(slba, buff, info.lba_size, true), SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff, "TEST", sizeof("TEST")) == 0);
  ASSERT_EQ(log.GetWriteHead(), 11 * info.zone_size + 1);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_size);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(range - info.lba_size));

  // Fill entire device
  char wbuff[range - info.lba_size];
  SZDTestUtil::CreateCyclicPattern(wbuff, range - info.lba_size, info.lba_size);
  ASSERT_EQ(log.Append(wbuff, range - info.lba_size, &blocks, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, (range - info.lba_size) / info.lba_size);
  ASSERT_EQ(log.Read(slba + 1, buff, range - info.lba_size, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff, wbuff, range - info.lba_size) == 0);
  ASSERT_EQ(log.GetWriteHead(), 13 * info.zone_size);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_size);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(0));

  // Try going out of bounds
  ASSERT_NE(log.Append("TEST", sizeof("TEST"), &blocks, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, 0);
  ASSERT_EQ(log.GetWriteHead(), 13 * info.zone_size);

  // Does reset work?
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.GetWriteHead(), 11 * info.zone_size);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_size);
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(range));

  // Flood again
  SZDTestUtil::CreateCyclicPattern(wbuff, range - info.lba_size, 0);
  ASSERT_EQ(log.Append(wbuff, range - info.lba_size, &blocks, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, (range - info.lba_size) / info.lba_size);
  ASSERT_EQ(log.Read(slba, buff, range - info.lba_size, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff, wbuff, range - info.lba_size) == 0);
  ASSERT_EQ(log.GetWriteHead(), 13 * info.zone_size - 1);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_size);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(info.lba_size));
}

TEST_F(SZDTest, TestFillingACircularPersistently) {
  SZD::SZDDevice dev("TestFillingACircularPersistently");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(10, 15, &dev, &info);
  SZD::SZDChannelFactory *factory =
      new SZD::SZDChannelFactory(dev.GetDeviceManager(), 1);
  factory->Ref();

  // Cleanup first round
  {
    SZD::SZDCircularLog log(factory, info, 10, 15);

    // We need to reset all data if it is there, as always.
    ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.GetWriteHead(), 10 * info.zone_size);
    ASSERT_EQ(log.GetWriteTail(), 10 * info.zone_size);
  }

  char buff[info.lba_size * 3];
  SZDTestUtil::CreateCyclicPattern(buff, info.lba_size * 3, 0);

  // We are going to repeatedly recreate logs, append some data and verify that
  // the pointers still match up after recovery.
  for (uint64_t slba = 10 * info.zone_size; slba < 15 * info.zone_size - 3;
       slba += 3) {
    SZD::SZDCircularLog log(factory, info, 10, 15);
    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.GetWriteHead(), slba);
    ASSERT_EQ(log.GetWriteTail(), 10 * info.zone_size);
    ASSERT_EQ(log.Append((const char *)buff, info.lba_size * 3, nullptr, true),
              SZD::SZDStatus::Success);
  }

  // last round, get and verify the data
  SZD::SZDBuffer buffer((15 - 10) * info.zone_size * info.lba_size,
                        info.lba_size);
  {
    SZD::SZDCircularLog log(factory, info, 10, 15);

    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(
        log.Read(log.GetWriteTail(), &buffer, 0,
                 (log.GetWriteHead() - log.GetWriteTail()) * info.lba_size,
                 true),
        SZD::SZDStatus::Success);
    char *verify_buffer;
    ASSERT_EQ(buffer.GetBuffer((void **)&verify_buffer),
              SZD::SZDStatus::Success);
    for (uint64_t slba = 0; slba < 5 * info.zone_size - 3; slba += 3) {
      ASSERT_TRUE(memcmp(verify_buffer + slba * info.lba_size, buff,
                         3 * info.lba_size) == 0);
    }
  }

  factory->Unref();
}

TEST_F(SZDTest, TestCircularLogCircularPattern) {
  SZD::SZDDevice dev("TestCircularLogCircularPattern");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(10, 15, &dev, &info);
  SZD::SZDChannelFactory *factory =
      new SZD::SZDChannelFactory(dev.GetDeviceManager(), 2);
  SZD::SZDCircularLog log(factory, info, 10, 15);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);

  // Can not consume tail when empty
  ASSERT_NE(log.ConsumeTail(10 * info.zone_size, 10 * info.zone_size + 3),
            SZD::SZDStatus::Success);

  // Fill up to end - 3
  uint64_t range =
      (15 - 10) * info.zone_size * info.lba_size - 3 * info.lba_size;
  char buff[range];
  SZDTestUtil::CreateCyclicPattern(buff, range, 0);
  uint64_t blocks;
  ASSERT_EQ(log.Append(buff, range, &blocks, true), SZD::SZDStatus::Success);

  // Consume 3 entries from tail.
  ASSERT_EQ(log.ConsumeTail(10 * info.zone_size, 10 * info.zone_size + 3),
            SZD::SZDStatus::Success);
  ASSERT_NE(log.Append(buff, info.lba_size * 9, &blocks, true),
            SZD::SZDStatus::Success);
  // Can not eat same area again
  ASSERT_NE(log.ConsumeTail(10 * info.zone_size, 10 * info.zone_size + 3),
            SZD::SZDStatus::Success);

  // Try if pointers could be partially recovered on restart
  {
    SZD::SZDCircularLog tlog(factory, info, 10, 15);
    ASSERT_EQ(tlog.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(tlog.GetWriteHead(), log.GetWriteHead());
    ASSERT_EQ(tlog.GetWriteTail(),
              (log.GetWriteTail() / info.zone_size) * info.zone_size);
  }

  // Consume entire zone and try to append
  ASSERT_EQ(log.ConsumeTail(10 * info.zone_size + 3, 11 * info.zone_size),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Append(buff, info.lba_size * 9, &blocks, true),
            SZD::SZDStatus::Success);

  // Make some more space
  ASSERT_EQ(log.ConsumeTail(11 * info.zone_size, 12 * info.zone_size),
            SZD::SZDStatus::Success);

  // Repeatedly eat and append
  for (uint64_t slba = 0; slba < 5 * info.zone_size - 6; slba += 6) {
    uint64_t eat_address_first = 12 * info.zone_size + slba;
    eat_address_first =
        eat_address_first >= 15 * info.zone_size
            ? (eat_address_first - 15 * info.zone_size) + 10 * info.zone_size
            : eat_address_first;
    uint64_t eat_address_second = 12 * info.zone_size + slba + 6;
    eat_address_second =
        eat_address_second > 15 * info.zone_size
            ? (eat_address_second - 15 * info.zone_size) + 10 * info.zone_size
            : eat_address_second;

    ASSERT_EQ(log.ConsumeTail(eat_address_first, eat_address_second),
              SZD::SZDStatus::Success);
    ASSERT_EQ(log.Append(buff, info.lba_size * 6, &blocks, true),
              SZD::SZDStatus::Success);

    // Try if pointers could be partially recovered on restart
    {
      SZD::SZDCircularLog tlog(factory, info, 10, 15);
      ASSERT_EQ(tlog.RecoverPointers(), SZD::SZDStatus::Success);
      ASSERT_EQ(tlog.GetWriteHead(), log.GetWriteHead());
      ASSERT_EQ(tlog.GetWriteTail(),
                (log.GetWriteTail() / info.zone_size) * info.zone_size);
    }
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
} // namespace
