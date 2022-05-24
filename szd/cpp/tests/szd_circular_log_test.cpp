#include "szd_test_util.hpp"
#include <gtest/gtest.h>
#include <szd/datastructures/szd_circular_log.hpp>
#include <szd/szd_channel.hpp>
#include <szd/szd_channel_factory.hpp>
#include <szd/szd_device.hpp>
#include <szd/szd_status.hpp>

#include <vector>

namespace {

class SZDTest : public ::testing::Test {};

TEST_F(SZDTest, TestFillingACircularLogEphemerally) {
  SZD::SZDDevice dev("TestFillingACircularLogEphemerally");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(10, 15, &dev, &info);
  SZD::SZDChannelFactory *factory =
      new SZD::SZDChannelFactory(dev.GetDeviceManager(), 2);
  SZD::SZDCircularLog log(factory, info, 11, 13, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
  // Test empty state
  ASSERT_EQ(log.GetWriteHead(), 11 * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_cap);
  ASSERT_TRUE(log.Empty());
  uint64_t range = (13 - 11) * info.zone_cap * info.lba_size;
  ASSERT_TRUE(log.SpaceLeft(range));
  char buff[range];
  memset(buff, 0, sizeof(buff));

  // We can not read when nothing is written
  uint64_t slba = 11 * info.zone_cap;
  ASSERT_NE(log.Read(slba, buff, info.lba_size, true), SZD::SZDStatus::Success);

  // Write and read one entry
  uint64_t blocks;
  ASSERT_EQ(log.Append("TEST", sizeof("TEST"), &blocks, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, 1);
  ASSERT_EQ(log.Read(slba, buff, info.lba_size, true), SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff, "TEST", sizeof("TEST")) == 0);
  ASSERT_EQ(log.GetWriteHead(), 11 * info.zone_cap + 1);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_cap);
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
  ASSERT_EQ(log.GetWriteHead(), 13 * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_cap);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(0));

  // Try going out of bounds
  ASSERT_NE(log.Append("TEST", sizeof("TEST"), &blocks, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, 0);
  ASSERT_EQ(log.GetWriteHead(), 13 * info.zone_cap);

  // Does reset work?
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.GetWriteHead(), 11 * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_cap);
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
  ASSERT_EQ(log.GetWriteHead(), 13 * info.zone_cap - 1);
  ASSERT_EQ(log.GetWriteTail(), 11 * info.zone_cap);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(info.lba_size));
}

TEST_F(SZDTest, TestFillingACircularPersistently) {
  SZD::SZDDevice dev("TestFillingACircularPersistently");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(10, 15, &dev, &info);
  SZD::SZDChannelFactory *factory =
      new SZD::SZDChannelFactory(dev.GetDeviceManager(), 2);
  factory->Ref();

  // Cleanup first round
  {
    SZD::SZDCircularLog log(factory, info, 10, 15, 1);

    // We need to reset all data if it is there, as always.
    ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.GetWriteHead(), 10 * info.zone_cap);
    ASSERT_EQ(log.GetWriteTail(), 10 * info.zone_cap);
  }

  char buff[info.lba_size * 3];
  SZDTestUtil::CreateCyclicPattern(buff, info.lba_size * 3, 0);

  // We are going to repeatedly recreate logs, append some data and verify that
  // the pointers still match up after recovery.
  for (uint64_t slba = 10 * info.zone_cap; slba < 15 * info.zone_cap - 3;
       slba += 3) {
    SZD::SZDCircularLog log(factory, info, 10, 15, 1);
    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.GetWriteHead(), slba);
    ASSERT_EQ(log.GetWriteTail(), 10 * info.zone_cap);
    ASSERT_EQ(log.Append((const char *)buff, info.lba_size * 3, nullptr, true),
              SZD::SZDStatus::Success);
  }

  // last round, get and verify the data
  SZD::SZDBuffer buffer((15 - 10) * info.zone_cap * info.lba_size,
                        info.lba_size);
  {
    SZD::SZDCircularLog log(factory, info, 10, 15, 1);

    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(
        log.Read(log.GetWriteTail(), &buffer, 0,
                 (log.GetWriteHead() - log.GetWriteTail()) * info.lba_size,
                 true),
        SZD::SZDStatus::Success);
    char *verify_buffer;
    ASSERT_EQ(buffer.GetBuffer((void **)&verify_buffer),
              SZD::SZDStatus::Success);
    for (uint64_t slba = 0; slba < 5 * info.zone_cap - 3; slba += 3) {
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
      new SZD::SZDChannelFactory(dev.GetDeviceManager(), 4);
  SZD::SZDCircularLog log(factory, info, 10, 15, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);

  // Can not consume tail when empty
  ASSERT_NE(log.ConsumeTail(10 * info.zone_cap, 10 * info.zone_cap + 3),
            SZD::SZDStatus::Success);

  // Fill up to end - 3
  uint64_t range =
      (15 - 10) * info.zone_cap * info.lba_size - 3 * info.lba_size;
  char buff[range];
  SZDTestUtil::CreateCyclicPattern(buff, range, 0);
  uint64_t blocks;
  ASSERT_EQ(log.Append(buff, range, &blocks, true), SZD::SZDStatus::Success);

  // Consume 3 entries from tail.
  ASSERT_EQ(log.ConsumeTail(10 * info.zone_cap, 10 * info.zone_cap + 3),
            SZD::SZDStatus::Success);
  ASSERT_NE(log.Append(buff, info.lba_size * 9, &blocks, true),
            SZD::SZDStatus::Success);
  // Can not eat same area again
  ASSERT_NE(log.ConsumeTail(10 * info.zone_cap, 10 * info.zone_cap + 3),
            SZD::SZDStatus::Success);

  // Try if pointers could be partially recovered on restart
  {
    SZD::SZDCircularLog tlog(factory, info, 10, 15, 1);
    ASSERT_EQ(tlog.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(tlog.GetWriteHead(), log.GetWriteHead());
    ASSERT_EQ(tlog.GetWriteTail(),
              (log.GetWriteTail() / info.zone_cap) * info.zone_cap);
  }

  // Consume entire zone and try to append
  ASSERT_EQ(log.ConsumeTail(10 * info.zone_cap + 3, 11 * info.zone_cap),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Append(buff, info.lba_size * 9, &blocks, true),
            SZD::SZDStatus::Success);

  // Make some more space
  ASSERT_EQ(log.ConsumeTail(11 * info.zone_cap, 12 * info.zone_cap),
            SZD::SZDStatus::Success);

  char bufferr[6 * info.lba_size];

  // Repeatedly eat and append
  for (uint64_t slba = 0; slba < 5 * info.zone_cap - 6; slba += 6) {
    uint64_t eat_address_first = 12 * info.zone_cap + slba;
    uint64_t eat_address_second = 12 * info.zone_cap + slba + 6;
    eat_address_second =
        eat_address_first >= 15 * info.zone_cap
            ? (eat_address_second - 15 * info.zone_cap) + 10 * info.zone_cap
            : eat_address_second;
    eat_address_first =
        eat_address_first >= 15 * info.zone_cap
            ? (eat_address_first - 15 * info.zone_cap) + 10 * info.zone_cap
            : eat_address_first;

    ASSERT_EQ(log.ConsumeTail(eat_address_first, eat_address_second),
              SZD::SZDStatus::Success);
    uint64_t waddress = log.GetWriteHead();
    ASSERT_EQ(log.Append(buff, info.lba_size * 6, &blocks, true),
              SZD::SZDStatus::Success);

    // Test if the data is consistent
    {
      ASSERT_EQ(log.Read(waddress, bufferr, 6 * info.lba_size, true),
                SZD::SZDStatus::Success);
      ASSERT_TRUE(memcmp(bufferr, buff, 6 * info.lba_size) == 0);
    }

    // Try if pointers could be partially recovered on restart
    {
      SZD::SZDCircularLog tlog(factory, info, 10, 15, 1);
      ASSERT_EQ(tlog.RecoverPointers(), SZD::SZDStatus::Success);
      ASSERT_EQ(tlog.GetWriteHead(), log.GetWriteHead());
      ASSERT_EQ(tlog.GetWriteTail(),
                (log.GetWriteTail() / info.zone_cap) * info.zone_cap);
    }
  }
}

} // namespace
