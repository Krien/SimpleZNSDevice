#include "szd_test_util.hpp"
#include <gtest/gtest.h>
#include <szd/datastructures/szd_once_log.hpp>
#include <szd/szd_channel.hpp>
#include <szd/szd_channel_factory.hpp>
#include <szd/szd_device.hpp>
#include <szd/szd_status.hpp>

#include <vector>

namespace {

class SZDTest : public ::testing::Test {};

static constexpr size_t needed_channels_for_once_log = 2;
static constexpr uint64_t begin_zone = 10;
static constexpr uint64_t end_zone = 15;

TEST_F(SZDTest, OnceLogEphemeralTest) {
  SZD::SZDDevice dev("OnceLogEphemeralTest");
  static constexpr uint64_t begin_zone_log = begin_zone + 1;
  static constexpr uint64_t end_zone_log = end_zone - 2;
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetDeviceManager(), needed_channels_for_once_log);
  SZD::SZDOnceLog log(factory, info, begin_zone_log, end_zone_log, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAllForce(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
  // Test empty state
  ASSERT_EQ(log.GetWriteHead(), begin_zone_log * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), begin_zone_log * info.zone_cap);
  ASSERT_TRUE(log.Empty());
  uint64_t range =
      (end_zone_log - begin_zone_log) * info.zone_cap * info.lba_size;
  ASSERT_TRUE(log.SpaceLeft(range));

  SZDTestUtil::RAIICharBuffer buff(range);
  memset(buff.buff_, 0, range);

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
  for (size_t i = sizeof("TEST"); i < range; i++) {
    ASSERT_EQ(buff.buff_[i], 0);
  }
  ASSERT_EQ(log.GetWriteHead(), begin_zone_log * info.zone_cap + 1);
  ASSERT_EQ(log.GetWriteTail(), begin_zone_log * info.zone_cap);
  ASSERT_FALSE(log.Empty());
  ASSERT_TRUE(log.SpaceLeft(range - info.lba_size));

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
}

TEST_F(SZDTest, OnceLogPersistenceTest) {
  SZD::SZDDevice dev("OnceLogPersistenceTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetDeviceManager(), needed_channels_for_once_log);
  factory->Ref();

  // Cleanup first round
  {
    SZD::SZDOnceLog log(factory, info, begin_zone, end_zone, 1);

    // We need to reset all data if it is there, as always.
    ASSERT_EQ(log.ResetAllForce(), SZD::SZDStatus::Success);
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
       slba < end_zone * info.zone_cap - 3; slba += 3) {
    SZD::SZDOnceLog log(factory, info, begin_zone, end_zone, 1);
    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.GetWriteHead(), slba);
    ASSERT_EQ(log.GetWriteTail(), begin_zone * info.zone_cap);
    ASSERT_EQ(log.Append((const char *)buff.buff_, range, nullptr, true),
              SZD::SZDStatus::Success);
  }

  // last round, get and verify the data
  SZD::SZDBuffer buffer((end_zone - begin_zone) * info.zone_cap * info.lba_size,
                        info.lba_size);
  {
    SZD::SZDOnceLog log(factory, info, begin_zone, end_zone, 1);

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

TEST_F(SZDTest, OnceLogMarkInactiveTest) {
  SZD::SZDDevice dev("OnceLogMarkInactiveTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetDeviceManager(), needed_channels_for_once_log);
  factory->Ref();
  SZD::SZDOnceLog log(factory, info, begin_zone, end_zone, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAllForce(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.SpaceAvailable(),
            (end_zone - begin_zone) * info.zone_cap * info.lba_size);

  // MarkInactive will not do anything on an empty log
  ASSERT_EQ(log.MarkInactive(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.GetWriteHead(), begin_zone * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), begin_zone * info.zone_cap);
  ASSERT_EQ(log.SpaceAvailable(),
            (end_zone - begin_zone) * info.zone_cap * info.lba_size);

  // MarkInactive will waste part of a zone if writehead is halveway
  size_t range = info.lba_size * 3;
  SZDTestUtil::RAIICharBuffer buff(range);
  SZDTestUtil::CreateCyclicPattern(buff.buff_, range, 0);
  ASSERT_EQ(log.Append(buff.buff_, range, nullptr, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(log.MarkInactive(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.GetWriteHead(), (begin_zone + 1) * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), begin_zone * info.zone_cap);
  ASSERT_EQ(log.SpaceAvailable(),
            (end_zone - begin_zone - 1) * info.zone_cap * info.lba_size);

  // MarkActive will not do anything when writehead is equal to the beginning of
  // a zone
  ASSERT_EQ(log.MarkInactive(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.GetWriteHead(), (begin_zone + 1) * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), begin_zone * info.zone_cap);
  ASSERT_EQ(log.SpaceAvailable(),
            (end_zone - begin_zone - 1) * info.zone_cap * info.lba_size);

  // We should just be able to write again
  ASSERT_EQ(log.Append(buff.buff_, range, nullptr, true),
            SZD::SZDStatus::Success);

  // Reset should respect our wishes
  ASSERT_EQ(log.ResetAll(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.GetWriteHead(), begin_zone * info.zone_cap);
  ASSERT_EQ(log.GetWriteTail(), begin_zone * info.zone_cap);
  ASSERT_EQ(log.SpaceAvailable(),
            (end_zone - begin_zone) * info.zone_cap * info.lba_size);

  factory->Unref();
}

TEST_F(SZDTest, OnceLogReadAllTest) {
  SZD::SZDDevice dev("OnceLogReadAllTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetDeviceManager(), needed_channels_for_once_log);
  factory->Ref();
  SZD::SZDOnceLog log(factory, info, begin_zone, end_zone, 1);

  // We need to reset all data if it is there, as always.
  ASSERT_EQ(log.ResetAllForce(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);
  ASSERT_EQ(log.SpaceAvailable(),
            (end_zone - begin_zone) * info.zone_cap * info.lba_size);

  // Test read all works
  size_t range = (info.zone_cap + 3) * info.lba_size;
  SZDTestUtil::RAIICharBuffer buffw(range);
  SZDTestUtil::CreateCyclicPattern(buffw.buff_, range, 0);
  ASSERT_EQ(log.Append(buffw.buff_, range, nullptr, true),
            SZD::SZDStatus::Success);
  std::string out;
  ASSERT_EQ(log.ReadAll(out), SZD::SZDStatus::Success);
  ASSERT_EQ(out.size(), range);
  for (size_t i = 0; i < range; i++) {
    ASSERT_EQ(out[i], buffw.buff_[i]);
  }

  factory->Unref();
}

// Does not cover most edge cases
TEST_F(SZDTest, OnceLogAsyncTest) {
  SZD::SZDDevice dev("OnceLogAsyncTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory *factory = new SZD::SZDChannelFactory(
      dev.GetDeviceManager(), needed_channels_for_once_log);
  factory->Ref();
  SZD::SZDChannel **channel = new SZD::SZDChannel *[1];
  factory->register_channel(channel, true, 4);
  {
    SZD::SZDOnceLog log(factory, info, begin_zone, end_zone, 1, channel);
    ASSERT_EQ(log.ResetAllForce(), SZD::SZDStatus::Success);
    ASSERT_EQ(log.RecoverPointers(), SZD::SZDStatus::Success);

    // We should be able to enqueue multiple requests (up to the limit)
    size_t range = 3 * info.lba_size;
    SZDTestUtil::RAIICharBuffer buffw(range);
    for (size_t i = 0; i < 4; i++) {
      ASSERT_EQ(log.AsyncAppend(buffw.buff_, range, nullptr, true),
                SZD::SZDStatus::Success);
    }
    // When queueing more than limit we can still enqueue, but might have to
    // wait
    for (size_t i = 0; i < 4; i++) {
      ASSERT_EQ(log.AsyncAppend(buffw.buff_, range, nullptr, true),
                SZD::SZDStatus::Success);
    }

    // We can sync to ensure persistence
    ASSERT_EQ(log.Sync(), SZD::SZDStatus::Success);
  }
  factory->unregister_channel(*channel);
  factory->Unref();
  delete[] channel;
}

} // namespace
