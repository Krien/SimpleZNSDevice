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

static constexpr size_t needed_channels_for_circular_log = 3;
static constexpr uint64_t begin_zone = 10;
static constexpr uint64_t end_zone = 15;

TEST_F(SZDTest, FillingACircularLogEphemerallyTest) {
  SZD::SZDDevice dev("FillingACircularLogEphemerallyTest");
  static constexpr uint64_t begin_zone_log = begin_zone + 1;
  static constexpr uint64_t end_zone_log = end_zone - 2;
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetEngineManager(), needed_channels_for_circular_log);
  SZD::SZDCircularLog log(factory, info, begin_zone_log, end_zone_log, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
  // Test empty state
  ASSERT_EQ(log.GetWriteHead(), begin_zone_log * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), begin_zone_log * info.zone_cap);
  ASSERT_TRUE(log.Empty());
  uint64_t range =
      (end_zone_log - begin_zone_log) * info.zone_cap * info.lba_size;
  ASSERT_TRUE(log.SpaceLeft(range));
  ASSERT_EQ(log.SpaceAvailable(), range);

  SZDTestUtil::RAIICharBuffer buff(range);
  memset(buff.buff_, 0, sizeof(buff));

  // We can not read when nothing is written
  uint64_t slba = begin_zone_log * info.zone_cap;
  ASSERT_NE(log.Read(slba, buff.buff_, info.lba_size, true),
            SZD::SZDStatus::Success);

  // Write and read one entry
  uint64_t blocks;
  ASSERT_EQ(log.Append("TEST", sizeof("TEST"), &blocks, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, 1);
  ASSERT_EQ(log.Read(slba, buff.buff_, info.lba_size, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff.buff_, "TEST", sizeof("TEST")) == 0);
  ASSERT_EQ(log.GetWriteHead(), begin_zone_log * info.zone_cap + 1);
  ASSERT_EQ(log.GetWriteTail(), begin_zone_log * info.zone_cap);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(range - info.lba_size));
  ASSERT_EQ(log.SpaceAvailable(), range - info.lba_size);

  // Fill entire device
  SZDTestUtil::RAIICharBuffer wbuff(range - info.lba_size);
  SZDTestUtil::CreateCyclicPattern(wbuff.buff_, range - info.lba_size,
                                   info.lba_size);
  ASSERT_EQ(log.Append(wbuff.buff_, range - info.lba_size, &blocks, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, (range - info.lba_size) / info.lba_size);
  ASSERT_EQ(log.Read(slba + 1, buff.buff_, range - info.lba_size, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff.buff_, wbuff.buff_, range - info.lba_size) == 0);
  ASSERT_EQ(log.GetWriteHead(), end_zone_log * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), begin_zone_log * info.zone_cap);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(0));
  ASSERT_EQ(log.SpaceAvailable(), 0);

  // Try going out of bounds
  ASSERT_NE(log.Append("TEST", sizeof("TEST"), &blocks, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, 0);
  ASSERT_EQ(log.GetWriteHead(), end_zone_log * info.zone_cap);

  // Does reset work?
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.GetWriteHead(), begin_zone_log * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), begin_zone_log * info.zone_cap);
  ASSERT_TRUE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(range));
  ASSERT_EQ(log.SpaceAvailable(), range);

  // Flood again
  SZDTestUtil::CreateCyclicPattern(wbuff.buff_, range - info.lba_size, 0);
  ASSERT_EQ(log.Append(wbuff.buff_, range - info.lba_size, &blocks, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(blocks, (range - info.lba_size) / info.lba_size);
  ASSERT_EQ(log.Read(slba, buff.buff_, range - info.lba_size, true),
            SZD::SZDStatus::Success);
  ASSERT_TRUE(memcmp(buff.buff_, wbuff.buff_, range - info.lba_size) == 0);
  ASSERT_EQ(log.GetWriteHead(), end_zone_log * info.zone_cap - 1);
  ASSERT_EQ(log.GetWriteTail(), begin_zone_log * info.zone_cap);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(info.lba_size));
  ASSERT_EQ(log.SpaceAvailable(), info.lba_size);
}

TEST_F(SZDTest, TestFillingACircularPersistently) {
  SZD::SZDDevice dev("TestFillingACircularPersistently");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetEngineManager(), needed_channels_for_circular_log);
  factory->Ref();

  // Cleanup first round
  {
    SZD::SZDCircularLog log(factory, info, begin_zone, end_zone, 1);

    // We need to reset all data if it is there, as always.
    ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.GetWriteHead(), begin_zone * info.zone_cap);
    ASSERT_EQ(log.GetWriteTail(), begin_zone * info.zone_cap);
  }

  size_t range = info.lba_size * 3;
  SZDTestUtil::RAIICharBuffer buff(range);
  SZDTestUtil::CreateCyclicPattern(buff.buff_, range, 0);

  // We are going to repeatedly recreate logs, append some data and verify that
  // the pointers still match up after recovery.
  for (uint64_t slba = begin_zone * info.zone_cap;
       slba < end_zone * info.zone_cap - (range / info.lba_size);
       slba += range / info.lba_size) {
    SZD::SZDCircularLog log(factory, info, begin_zone, end_zone, 1);
    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.GetWriteHead(), slba);
    ASSERT_EQ(log.GetWriteTail(), begin_zone * info.zone_cap);
    ASSERT_EQ(log.Append((const char *)buff.buff_, range, nullptr, true),
              SZD::SZDStatus::Success);
  }

  // last round, get and verify the data
  SZD::SZDBuffer buffer(dev.GetEngineManager(), (end_zone - begin_zone) * info.zone_cap * info.lba_size,
                        info.lba_size);
  {
    SZD::SZDCircularLog log(factory, info, begin_zone, end_zone, 1);

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
      ASSERT_TRUE(memcmp(verify_buffer + slba * info.lba_size, buff.buff_,
                         3 * info.lba_size) == 0);
    }
  }

  factory->Unref();
}

TEST_F(SZDTest, CircularLogCircularPatternTest) {
  SZD::SZDDevice dev("CircularLogCircularPatternTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetEngineManager(), needed_channels_for_circular_log * 2);
  SZD::SZDCircularLog log(factory, info, begin_zone, end_zone, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);

  // Can not consume tail when empty
  ASSERT_NE(log.ConsumeTail(begin_zone * info.zone_cap,
                            begin_zone * info.zone_cap + 3),
            SZD::SZDStatus::Success);

  // Fill up to end - 3
  uint64_t range = (end_zone - begin_zone) * info.zone_cap * info.lba_size -
                   3 * info.lba_size;
  SZDTestUtil::RAIICharBuffer buff(range);
  SZDTestUtil::CreateCyclicPattern(buff.buff_, range, 0);
  uint64_t blocks;
  ASSERT_EQ(log.Append(buff.buff_, range, &blocks, true),
            SZD::SZDStatus::Success);

  // Consume 3 entries from tail.
  ASSERT_EQ(log.ConsumeTail(begin_zone * info.zone_cap,
                            begin_zone * info.zone_cap + 3),
            SZD::SZDStatus::Success);
  // We have no space left (even after consuming a bit)
  ASSERT_NE(log.Append(buff.buff_, info.lba_size * 9, &blocks, true),
            SZD::SZDStatus::Success);
  // Can not eat same area again
  ASSERT_NE(log.ConsumeTail(begin_zone * info.zone_cap,
                            begin_zone * info.zone_cap + 3),
            SZD::SZDStatus::Success);

  // Try if pointers could be partially recovered on restart
  {
    SZD::SZDCircularLog tlog(factory, info, begin_zone, end_zone, 1);
    ASSERT_EQ(tlog.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(tlog.GetWriteHead(), log.GetWriteHead());
    ASSERT_EQ(tlog.GetWriteTail(),
              (log.GetWriteTail() / info.zone_cap) * info.zone_cap);
  }

  // Consume entire zone and try to append
  ASSERT_EQ(log.ConsumeTail(begin_zone * info.zone_cap + 3,
                            (begin_zone + 1) * info.zone_cap),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Append(buff.buff_, info.lba_size * 9, &blocks, true),
            SZD::SZDStatus::Success);

  // Make some more space
  ASSERT_EQ(log.ConsumeTail((begin_zone + 1) * info.zone_cap,
                            (begin_zone + 2) * info.zone_cap),
            SZD::SZDStatus::Success);

  SZDTestUtil::RAIICharBuffer bufferr(6 * info.lba_size);

  // Repeatedly eat and append
  for (uint64_t slba = 0; slba < 5 * info.zone_cap - 6; slba += 6) {
    uint64_t eat_address_first = (begin_zone + 2) * info.zone_cap + slba;
    uint64_t eat_address_second = (begin_zone + 2) * info.zone_cap + slba + 6;
    eat_address_second = eat_address_first >= end_zone * info.zone_cap
                             ? (eat_address_second - end_zone * info.zone_cap) +
                                   begin_zone * info.zone_cap
                             : eat_address_second;
    eat_address_first = eat_address_first >= end_zone * info.zone_cap
                            ? (eat_address_first - end_zone * info.zone_cap) +
                                  begin_zone * info.zone_cap
                            : eat_address_first;

    ASSERT_EQ(log.ConsumeTail(eat_address_first, eat_address_second),
              SZD::SZDStatus::Success);
    uint64_t waddress = log.GetWriteHead();
    ASSERT_EQ(log.Append(buff.buff_, info.lba_size * 6, &blocks, true),
              SZD::SZDStatus::Success);

    // Test if the data is consistent
    {
      ASSERT_EQ(log.Read(waddress, bufferr.buff_, 6 * info.lba_size, true),
                SZD::SZDStatus::Success);
      ASSERT_TRUE(memcmp(bufferr.buff_, buff.buff_, 6 * info.lba_size) == 0);
    }

    // Try if pointers could be partially recovered on restart
    {
      SZD::SZDCircularLog tlog(factory, info, begin_zone, end_zone, 1);
      ASSERT_EQ(tlog.RecoverPointers(), SZD::SZDStatus::Success);
      ASSERT_EQ(tlog.GetWriteHead(), log.GetWriteHead());
      ASSERT_EQ(tlog.GetWriteTail(),
                (log.GetWriteTail() / info.zone_cap) * info.zone_cap);
    }
  }
}

TEST_F(SZDTest, CircularLogMultipleReaderTest) {
  SZD::SZDDevice dev("CircularLogCircularPatternTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetEngineManager(),
      needed_channels_for_circular_log + /* 1 reader extra */ 1);
  SZD::SZDCircularLog log(factory, info, begin_zone, end_zone, 2);
  ASSERT_EQ(log.GetNumberOfReaders(), 2);

  // Reset first
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);

  // Add some data
  size_t range = info.lba_size * 3;
  SZDTestUtil::RAIICharBuffer buffw(range);
  SZDTestUtil::CreateCyclicPattern(buffw.buff_, range, 0);
  ASSERT_EQ(log.Append(buffw.buff_, range, nullptr, true),
            SZD::SZDStatus::Success);

  // 2 readers
  SZDTestUtil::RAIICharBuffer buffr1(range);
  SZDTestUtil::RAIICharBuffer buffr2(range);
  ASSERT_EQ(log.Read(begin_zone * info.zone_cap, buffr1.buff_,
                     info.lba_size * 2, true, 0),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.Read(begin_zone * info.zone_cap, buffr2.buff_, info.lba_size,
                     true, 1),
            SZD::SZDStatus::Success);

  // This should have been done with a memcmp in all fairness
  for (size_t i = 0; i < info.lba_size * 2; i++) {
    ASSERT_EQ(buffw.buff_[i], buffr1.buff_[i]);
  }
  for (size_t i = 0; i < info.lba_size; i++) {
    ASSERT_EQ(buffw.buff_[i + info.lba_size * 2], buffr2.buff_[i]);
  }
}

} // namespace
