#include "szd_test_util.hpp"
#include <gtest/gtest.h>
#include <szd/szd.h>
#include <szd/szd_channel.hpp>
#include <szd/szd_channel_factory.hpp>
#include <szd/szd_device.hpp>
#include <szd/szd_status.hpp>

#include <numeric>
#include <string>
#include <vector>

namespace {

class SZDChannelTest : public ::testing::Test {};

static constexpr uint64_t begin_zone = 10;
static constexpr uint64_t end_zone = 15;
static_assert(begin_zone + 4 < end_zone);

uint64_t expected_steps(uint64_t slba, const uint64_t elba,
                        const uint64_t zone_cap, const uint64_t max_step) {
  uint64_t steps = 0;
  uint64_t zone = slba / zone_cap;
  uint64_t step = max_step;
  while (slba < elba) {
    step = elba - slba < max_step ? elba - slba : max_step;
    steps++;
    slba += step;
    if (slba / zone_cap != zone) {
      if (slba % zone_cap != 0) {
        steps++;
      }
      zone++;
    }
  }
  return steps;
}

void expected_heat_distr(uint64_t slba, std::vector<uint64_t> &heat,
                         const uint64_t min_slba, const uint64_t elba,
                         const uint64_t zone_cap, const uint64_t max_step) {
  uint64_t minzone = min_slba / zone_cap;
  uint64_t zone = slba / zone_cap;
  uint64_t step = max_step;
  while (slba < elba) {
    step = elba - slba < max_step ? elba - slba : max_step;
    heat[zone - minzone]++;
    slba += step;
    if (slba / zone_cap != zone) {
      if (slba % zone_cap != 0) {
        heat[zone + 1 - minzone]++;
      }
      zone++;
    }
  }
}

#ifdef SZD_PERF_PER_ZONE_COUNTERS
bool equal_vectors_diag(std::vector<uint64_t> &l, std::vector<uint64_t> &&r) {
  if (l.size() != r.size()) {
    return false;
  }
  for (size_t i = 0; i < l.size(); i++) {
    if (l[i] != r[i]) {
      // printf("Vector not equal at %lu, l = %lu and r = %lu\n", i, l[i],
      // r[i]);
      return false;
    }
  }
  return true;
}
#endif

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

TEST_F(SZDChannelTest, TranslateAddress) {
  SZD::SZDDevice dev("TranslateAddress");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;

  // Mock translations
  // zone_sze = 4096 in all tests
  //  1. EQ (zone_sze = zone_cap)
  //  2. PowerOfTwo (zone_sze = zone_cap * 2)
  //  3. NotAPowerOfTwo (zone_sze = zone_cap + 10)
  dev.GetDeviceManager()->info.zone_size = 4096;

  const auto testallignment = [](SZD::SZDChannel *channel, uint64_t l,
                                 uint64_t r) -> void {
    ASSERT_EQ(channel->TranslateLbaToPba(l), r);
    ASSERT_EQ(channel->TranslatePbaToLba(r), l);
    // Next two are implicit, but just to be sure we test if it can be reversed.
    ASSERT_EQ(channel->TranslateLbaToPba(channel->TranslatePbaToLba(r)), r);
    ASSERT_EQ(channel->TranslatePbaToLba(channel->TranslateLbaToPba(l)), l);
  };

  dev.GetDeviceManager()->info.zone_cap = 4096;
  factory.register_channel(&channel);
  testallignment(channel, 0, 0);
  testallignment(channel, 3, 3);
  testallignment(channel, 4096 >> 1, 4096 >> 1);
  testallignment(channel, 4096, 4096);
  testallignment(channel, 1UL << 63, 1UL << 63);
  factory.unregister_channel(channel);

  dev.GetDeviceManager()->info.zone_cap = 4096 >> 1;
  factory.register_channel(&channel);
  testallignment(channel, 0, 0);
  testallignment(channel, 3, 3);
  testallignment(channel, 4096 >> 1, 4096);
  testallignment(channel, 4096, 4096 << 1);
  testallignment(channel, 4096, 4096 << 1);
  testallignment(channel, 1UL << 31, 1UL << 32);
  testallignment(channel, (1UL << 31) + 42UL, (1UL << 32) + 42UL);
  testallignment(channel, 1UL << 62, 1UL << 63);
  factory.unregister_channel(channel);

  dev.GetDeviceManager()->info.zone_cap = 4096 - 10;
  factory.register_channel(&channel);
  testallignment(channel, 0, 0);
  testallignment(channel, 3, 3);
  testallignment(channel, 4096 - 10, 4096);
  testallignment(channel, 4096, 4096 + 10);
  testallignment(channel, (4096 - 10) * 8, 4096 << 3);
  testallignment(channel, 4096 << 3, (4096 << 3) + (10 << 3));
  testallignment(channel, (4096UL - 10) << 50 /*2^62*/, 4096UL << 50);
  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, DirectIO) {
  SZD::SZDDevice dev("DirectIO");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  uint64_t diag_bytes_written = 0;
  uint64_t diag_append_ops = 0;
  std::vector<uint64_t> appends(end_zone - begin_zone, 0);
  uint64_t diag_bytes_read = 0;
  uint64_t diag_read_ops = 0;
  std::vector<uint64_t> resets(end_zone - begin_zone, 0);
  uint64_t diag_reset_ops = 0;

  // Have to reset for a clean state
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);
  diag_reset_ops += end_zone - begin_zone;
  resets = std::vector<uint64_t>(end_zone - begin_zone, 1);

  // Create buffers. Test cases are alligned and have 1 zone + 2 lbas.
  uint64_t begin_lba = begin_zone * info.zone_cap;
  uint64_t write_head = begin_lba;
  uint64_t range = info.lba_size * info.zone_cap + info.lba_size * 2;
  SZDTestUtil::RAIICharBuffer bufferw(range + 1);
  SZDTestUtil::RAIICharBuffer bufferr(range + 1);
  SZDTestUtil::CreateCyclicPattern(bufferw.buff_, range, 0);

  // Write 1 zone and 2 lbas and verify if this data can be read.
  ASSERT_EQ(channel->DirectAppend(&write_head, bufferw.buff_, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, begin_lba + info.zone_cap + 2);
  diag_bytes_written += range;
  diag_append_ops += expected_steps(begin_lba, begin_lba + info.zone_cap + 2,
                                    info.zone_cap, info.zasl / info.lba_size);
  expected_heat_distr(begin_lba, appends, begin_zone * info.zone_cap,
                      begin_lba + info.zone_cap + 2, info.zone_cap,
                      info.zasl / info.lba_size);
  ASSERT_EQ(channel->DirectRead(begin_lba, bufferr.buff_, range, true),
            SZD::SZDStatus::Success);
  diag_bytes_read += range;
  diag_read_ops += expected_steps(begin_lba, begin_lba + info.zone_cap + 2,
                                  info.zone_cap, info.mdts / info.lba_size);
  ASSERT_TRUE(memcmp(bufferw.buff_, bufferr.buff_, range) == 0);

  // We should be able to append again
  ASSERT_EQ(channel->DirectAppend(&write_head, bufferw.buff_, range, true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(write_head, begin_lba + 2 * (info.zone_cap + 2));
  diag_bytes_written += range;
  diag_append_ops += expected_steps(begin_lba + info.zone_cap + 2,
                                    begin_lba + 2 * (info.zone_cap + 2),
                                    info.zone_cap, info.zasl / info.lba_size);
  expected_heat_distr(begin_lba + info.zone_cap + 2, appends,
                      begin_zone * info.zone_cap,
                      begin_lba + 2 * (info.zone_cap + 2), info.zone_cap,
                      info.zasl / info.lba_size);

  ASSERT_EQ(channel->DirectRead(begin_lba + range / info.lba_size,
                                bufferr.buff_, range, true),
            SZD::SZDStatus::Success);
  diag_bytes_read += range;
  diag_read_ops += expected_steps(begin_lba + info.zone_cap + 2,
                                  begin_lba + 2 * (info.zone_cap + 2),
                                  info.zone_cap, info.mdts / info.lba_size);
  ASSERT_TRUE(memcmp(bufferw.buff_, bufferr.buff_, range) == 0);

  // We can write in the last zone
  write_head = (end_zone - 1) * info.zone_cap;
  uint64_t smaller_range = info.lba_size * info.zone_cap;
  ASSERT_EQ(
      channel->DirectAppend(&write_head, bufferw.buff_, smaller_range, true),
      SZD::SZDStatus::Success);
  diag_bytes_written += smaller_range;
  diag_append_ops +=
      expected_steps((end_zone - 1) * info.zone_cap, end_zone * info.zone_cap,
                     info.zone_cap, info.zasl / info.lba_size);
  expected_heat_distr((end_zone - 1) * info.zone_cap, appends,
                      begin_zone * info.zone_cap, end_zone * info.zone_cap,
                      info.zone_cap, info.zasl / info.lba_size);

  ASSERT_EQ(write_head, end_zone * info.zone_cap);
  ASSERT_EQ(channel->DirectRead((end_zone - 1) * info.zone_cap, bufferr.buff_,
                                smaller_range, true),
            SZD::SZDStatus::Success);
  diag_bytes_read += smaller_range;
  diag_read_ops +=
      expected_steps((end_zone - 1) * info.zone_cap, end_zone * info.zone_cap,
                     info.zone_cap, info.mdts / info.lba_size);
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

// Yes, we need to test our diagnostics as well
#ifdef SZD_PERF_COUNTERS
  ASSERT_EQ(channel->GetBytesWritten(), diag_bytes_written);
  ASSERT_EQ(channel->GetAppendOperationsCounter(), diag_append_ops);
  ASSERT_EQ(channel->GetBytesRead(), diag_bytes_read);
  ASSERT_EQ(channel->GetReadOperationsCounter(), diag_read_ops);
  ASSERT_EQ(channel->GetZonesResetCounter(), diag_reset_ops);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  ASSERT_EQ(std::accumulate(appends.begin(), appends.end(), 0),
            diag_append_ops);
  ASSERT_EQ(equal_vectors_diag(appends, channel->GetAppendOperations()), true);
  ASSERT_EQ(std::accumulate(resets.begin(), resets.end(), 0), diag_reset_ops);
  ASSERT_EQ(equal_vectors_diag(resets, channel->GetZonesReset()), true);
#endif
#endif

  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, DirectIONonAlligned) {
  SZD::SZDDevice dev("DirectIONonAlligned");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  uint64_t diag_bytes_written = 0;
  uint64_t diag_append_ops = 0;
  std::vector<uint64_t> appends(end_zone - begin_zone, 0);
  uint64_t diag_bytes_read = 0;
  uint64_t diag_read_ops = 0;
  std::vector<uint64_t> resets(end_zone - begin_zone, 0);
  uint64_t diag_reset_ops = 0;

  // Have to reset before the test can start
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);
  diag_reset_ops += end_zone - begin_zone;
  resets = std::vector<uint64_t>(end_zone - begin_zone, 1);

  // Create buffers for test, which are just two pages
  uint64_t range = info.lba_size * 2;
  SZDTestUtil::RAIICharBuffer bufferw(range + 1);
  SZDTestUtil::RAIICharBuffer bufferr(range + 1);
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
  diag_bytes_written += 2 * info.lba_size;
  diag_append_ops += expected_steps(begin_lba, begin_lba + 2, info.zone_cap,
                                    info.zasl / info.lba_size);
  expected_heat_distr(begin_lba, appends, begin_zone * info.zone_cap,
                      begin_lba + 2, info.zone_cap, info.zasl / info.lba_size);

  // When we say that we allign, we can not read unalligned
  ASSERT_NE(channel->DirectRead(begin_lba, bufferr.buff_,
                                info.lba_size + info.lba_size - 10, true),
            SZD::SZDStatus::Success);
  // When we do not allign, it should succeed with padding
  ASSERT_EQ(channel->DirectRead(begin_lba, bufferr.buff_,
                                info.lba_size + info.lba_size - 10, false),
            SZD::SZDStatus::Success);
  diag_bytes_read += 2 * info.lba_size;
  diag_read_ops += expected_steps(begin_lba, begin_lba + 2, info.zone_cap,
                                  info.mdts / info.lba_size);
  ASSERT_TRUE(memcmp(bufferr.buff_, bufferw.buff_,
                     info.lba_size + info.lba_size - 10) == 0);
  // Ensure that all padding are 0 bytes
  for (size_t i = info.lba_size + info.lba_size - 10; i < range; i++) {
    ASSERT_EQ(bufferr.buff_[i], 0);
  }

  // Reread and ensure that the padding written earlier is also 0 bytes
  ASSERT_EQ(
      channel->DirectRead(begin_lba, bufferr.buff_, 2 * info.lba_size, false),
      SZD::SZDStatus::Success);
  diag_bytes_read += 2 * info.lba_size;
  diag_read_ops += expected_steps(begin_lba, begin_lba + 2, info.zone_cap,
                                  info.mdts / info.lba_size);
  for (size_t i = info.lba_size + info.lba_size - 10; i < range; i++) {
    ASSERT_EQ(bufferr.buff_[i], 0);
  }

// Yes, we need to test our diagnostics as well
#ifdef SZD_PERF_COUNTERS
  ASSERT_EQ(channel->GetBytesWritten(), diag_bytes_written);
  ASSERT_EQ(channel->GetAppendOperationsCounter(), diag_append_ops);
  ASSERT_EQ(channel->GetBytesRead(), diag_bytes_read);
  ASSERT_EQ(channel->GetReadOperationsCounter(), diag_read_ops);
  ASSERT_EQ(channel->GetZonesResetCounter(), diag_reset_ops);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  ASSERT_EQ(std::accumulate(appends.begin(), appends.end(), 0),
            diag_append_ops);
  ASSERT_EQ(equal_vectors_diag(appends, channel->GetAppendOperations()), true);
  ASSERT_EQ(std::accumulate(resets.begin(), resets.end(), 0), diag_reset_ops);
  ASSERT_EQ(equal_vectors_diag(resets, channel->GetZonesReset()), true);
#endif
#endif

  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, BufferIO) {
  SZD::SZDDevice dev("BufferIO");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  uint64_t diag_bytes_written = 0;
  uint64_t diag_append_ops = 0;
  std::vector<uint64_t> appends(end_zone - begin_zone, 0);
  uint64_t diag_bytes_read = 0;
  uint64_t diag_read_ops = 0;
  std::vector<uint64_t> resets(end_zone - begin_zone, 0);
  uint64_t diag_reset_ops = 0;

  // Have to reset device for a clean state
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);
  diag_reset_ops += end_zone - begin_zone;
  resets = std::vector<uint64_t>(end_zone - begin_zone, 1);

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
  ASSERT_EQ(channel->FlushBufferSection(&write_head, buffer, range /*addr*/,
                                        range /*size*/, true),
            SZD::SZDStatus::Success);
  diag_bytes_written += range;
  diag_append_ops +=
      expected_steps(start_head, start_head + range / info.lba_size,
                     info.zone_cap, info.zasl / info.lba_size);
  expected_heat_distr(start_head, appends, begin_zone * info.zone_cap,
                      start_head + range / info.lba_size, info.zone_cap,
                      info.zasl / info.lba_size);

  ASSERT_EQ(channel->ReadIntoBuffer(start_head, &buffer, 2 * range /*addr*/,
                                    range /*size*/, true),
            SZD::SZDStatus::Success);
  diag_bytes_read += range;
  diag_read_ops +=
      expected_steps(start_head, start_head + range / info.lba_size,
                     info.zone_cap, info.mdts / info.lba_size);
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
  diag_bytes_written += info.lba_size;
  diag_append_ops += expected_steps(start_head, start_head + 1, info.zone_cap,
                                    info.zasl / info.lba_size);
  expected_heat_distr(start_head, appends, begin_zone * info.zone_cap,
                      start_head + 1, info.zone_cap, info.zasl / info.lba_size);

  ASSERT_NE(channel->ReadIntoBuffer(start_head, &buffer, 10, info.lba_size - 49,
                                    true),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ReadIntoBuffer(start_head, &buffer, 10, info.lba_size - 49,
                                    false),
            SZD::SZDStatus::Success);
  diag_bytes_read += info.lba_size;
  diag_read_ops += expected_steps(start_head, start_head + 1, info.zone_cap,
                                  info.mdts / info.lba_size);
  ASSERT_TRUE(memcmp(raw_buffer + 10, raw_buffer + range + info.lba_size - 10,
                     info.lba_size - 49) == 0);

  // Full alligned
  start_head = write_head;
  ASSERT_EQ(channel->FlushBuffer(&write_head, buffer), SZD::SZDStatus::Success);
  diag_bytes_written += buffer.GetBufferSize();
  diag_append_ops += expected_steps(
      start_head, start_head + buffer.GetBufferSize() / info.lba_size,
      info.zone_cap, info.zasl / info.lba_size);
  expected_heat_distr(start_head, appends, begin_zone * info.zone_cap,
                      start_head + buffer.GetBufferSize() / info.lba_size,
                      info.zone_cap, info.zasl / info.lba_size);

  SZD::SZDBuffer shadow_buffer(info.lba_size * 3, info.lba_size);
  char *raw_shadow_buffer;
  ASSERT_EQ(shadow_buffer.GetBuffer((void **)&raw_shadow_buffer),
            SZD::SZDStatus::Success);
  ASSERT_EQ(
      channel->ReadIntoBuffer(start_head, &shadow_buffer, 0, range * 3, true),
      SZD::SZDStatus::Success);
  diag_bytes_read += range * 3;
  diag_read_ops +=
      expected_steps(start_head, start_head + range * 3 / info.lba_size,
                     info.zone_cap, info.mdts / info.lba_size);
  ASSERT_TRUE(memcmp(raw_buffer, raw_shadow_buffer, range * 3) == 0);

// Yes, we need to test our diagnostics as well
#ifdef SZD_PERF_COUNTERS
  ASSERT_EQ(channel->GetBytesWritten(), diag_bytes_written);
  ASSERT_EQ(channel->GetAppendOperationsCounter(), diag_append_ops);
  ASSERT_EQ(channel->GetBytesRead(), diag_bytes_read);
  ASSERT_EQ(channel->GetReadOperationsCounter(), diag_read_ops);
  ASSERT_EQ(channel->GetZonesResetCounter(), diag_reset_ops);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  ASSERT_EQ(std::accumulate(appends.begin(), appends.end(), 0),
            diag_append_ops);
  ASSERT_EQ(equal_vectors_diag(appends, channel->GetAppendOperations()), true);
  ASSERT_EQ(std::accumulate(resets.begin(), resets.end(), 0), diag_reset_ops);
  ASSERT_EQ(equal_vectors_diag(resets, channel->GetZonesReset()), true);
#endif
#endif

  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, ResetZone) {
  SZD::SZDDevice dev("ResetZone");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 4);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  uint64_t diag_bytes_written = 0;
  uint64_t diag_append_ops = 0;
  std::vector<uint64_t> appends(end_zone - begin_zone, 0);
  uint64_t diag_reset_ops = 0;
  std::vector<uint64_t> resets(end_zone - begin_zone, 0);

  // Have to reset device for a clean state.
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);
  diag_reset_ops += end_zone - begin_zone;
  resets = std::vector<uint64_t>(end_zone - begin_zone, 1);

  // Write all 4 zones
  {
    uint64_t range = info.lba_size * info.zone_cap * 4;
    SZDTestUtil::RAIICharBuffer bufferw(range + 1);
    SZDTestUtil::CreateCyclicPattern(bufferw.buff_, range, 0);
    uint64_t write_head = begin_zone * info.zone_cap;
    ASSERT_EQ(channel->DirectAppend(&write_head, bufferw.buff_, range, false),
              SZD::SZDStatus::Success);
    ASSERT_EQ(write_head, (begin_zone + 4) * info.zone_cap);
    diag_bytes_written += range;
    diag_append_ops += expected_steps(begin_zone * info.zone_cap, write_head,
                                      info.zone_cap, info.zasl / info.lba_size);
    expected_heat_distr(begin_zone * info.zone_cap, appends,
                        begin_zone * info.zone_cap, write_head, info.zone_cap,
                        info.zasl / info.lba_size);
  }

  // Reset 2 zones in middle and test all write heads
  ASSERT_EQ(channel->ResetZone((begin_zone + 1) * info.zone_cap),
            SZD::SZDStatus::Success);
  diag_reset_ops += 1;
  resets[1]++;
  ASSERT_EQ(channel->ResetZone((begin_zone + 2) * info.zone_cap),
            SZD::SZDStatus::Success);
  diag_reset_ops += 1;
  resets[2]++;
  uint64_t zone_head;
  ASSERT_EQ(channel->ZoneHead(begin_zone * info.zone_cap, &zone_head),
            SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, (begin_zone + 1) * info.zone_cap);
  ASSERT_EQ(channel->ZoneHead((begin_zone + 1) * info.zone_cap, &zone_head),
            SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, (begin_zone + 1) * info.zone_cap);
  ASSERT_EQ(channel->ZoneHead((begin_zone + 2) * info.zone_cap, &zone_head),
            SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, (begin_zone + 2) * info.zone_cap);
  ASSERT_EQ(channel->ZoneHead((begin_zone + 3) * info.zone_cap, &zone_head),
            SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, (begin_zone + 4) * info.zone_cap);

// Yes, we need to test our diagnostics as well
#ifdef SZD_PERF_COUNTERS
  ASSERT_EQ(channel->GetBytesWritten(), diag_bytes_written);
  ASSERT_EQ(channel->GetAppendOperationsCounter(), diag_append_ops);
  ASSERT_EQ(channel->GetZonesResetCounter(), diag_reset_ops);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  ASSERT_EQ(std::accumulate(appends.begin(), appends.end(), 0),
            diag_append_ops);
  ASSERT_EQ(equal_vectors_diag(appends, channel->GetAppendOperations()), true);
  ASSERT_EQ(std::accumulate(resets.begin(), resets.end(), 0), diag_reset_ops);
  ASSERT_EQ(equal_vectors_diag(resets, channel->GetZonesReset()), true);
#endif
#endif

  factory.unregister_channel(channel);
}

TEST_F(SZDChannelTest, ResetAndWriteRespectsRange) {
  SZD::SZDDevice dev("ResetAndWriteRespectsRange");
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
  uint64_t zone_head;
  ASSERT_EQ(channel1->ZoneHead(first_start_head, &zone_head),
            SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, first_start_head + 2);
  ASSERT_EQ(channel3->ZoneHead(third_start_head, &zone_head),
            SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, third_start_head + 2);

  factory.unregister_channel(channel);
  factory.unregister_channel(channel1);
  factory.unregister_channel(channel2);
  factory.unregister_channel(channel3);
}

TEST_F(SZDChannelTest, FinishZone) {
  SZD::SZDDevice dev("FinishZone");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 4);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel);

  uint64_t begin_head = begin_zone * info.zone_cap;
  uint64_t zone_head = begin_head;
  uint64_t write_head = begin_head;
  uint64_t range = info.zone_cap * info.lba_size;
  SZDTestUtil::RAIICharBuffer bufferw(range + 1);
  SZDTestUtil::CreateCyclicPattern(bufferw.buff_, range, 0);

  // Have to reset device for a clean state.
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);

  // finish empty zone
  ASSERT_EQ(channel->FinishZone(begin_head), SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ZoneHead(begin_head, &zone_head), SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, (begin_zone + 1) * info.zone_cap);
  ASSERT_EQ(channel->ResetZone(begin_head), SZD::SZDStatus::Success);

  // finish half a zone
  write_head = begin_head;
  ASSERT_EQ(
      channel->DirectAppend(&write_head, bufferw.buff_, range >> 1, false),
      SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ZoneHead(begin_head, &zone_head), SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, begin_head + ((range >> 1) / info.lba_size));
  ASSERT_EQ(channel->FinishZone(begin_head), SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ZoneHead(begin_head, &zone_head), SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, (begin_zone + 1) * info.zone_cap);
  ASSERT_EQ(channel->ResetZone(begin_head), SZD::SZDStatus::Success);

  // finish full zone
  write_head = begin_head;
  ASSERT_EQ(channel->DirectAppend(&write_head, bufferw.buff_, range, false),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ZoneHead(begin_head, &zone_head), SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, begin_head + (range / info.lba_size));
  ASSERT_EQ(channel->FinishZone(begin_head), SZD::SZDStatus::Success);
  ASSERT_EQ(channel->ZoneHead(begin_head, &zone_head), SZD::SZDStatus::Success);
  ASSERT_EQ(zone_head, (begin_zone + 1) * info.zone_cap);
  ASSERT_EQ(channel->ResetZone(begin_head), SZD::SZDStatus::Success);
  factory.unregister_channel(channel);
}

// Note that testing async is non-trivial. We only test easy paths.
TEST_F(SZDChannelTest, AsyncTest) {
  SZD::SZDDevice dev("AsyncTest");
  SZD::DeviceInfo info;
  SZDTestUtil::SZDSetupDevice(begin_zone, end_zone, &dev, &info);
  SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 1);
  SZD::SZDChannel *channel;
  factory.register_channel(&channel, true, 8);

  uint64_t diag_bytes_written = 0;
  uint64_t diag_append_ops = 0;
  std::vector<uint64_t> appends(end_zone - begin_zone, 0);
  std::vector<uint64_t> resets(end_zone - begin_zone, 0);
  uint64_t diag_reset_ops = 0;

  // Have to reset device for a clean state.
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);
  diag_reset_ops += end_zone - begin_zone;
  resets = std::vector<uint64_t>(end_zone - begin_zone, 1);

  // Verify queue depth
  ASSERT_EQ(channel->GetQueueDepth(), 8);

  // Create buffer
  uint64_t range = info.lba_size * 3;
  SZDTestUtil::RAIICharBuffer bufferw(range + 1);
  SZDTestUtil::CreateCyclicPattern(bufferw.buff_, range, 0);

  // Verify that we can poll without outstanding requests
  channel->PollOnce(0u);
  channel->PollOnce(16u);
  uint32_t any_writer;
  channel->FindFreeWriter(&any_writer);
  channel->Sync();
  ASSERT_EQ(channel->GetOutstandingRequests(), 0);

  // We can only issue 1 async with the same writer number (do NOT test multiple
  // to same writer, this will segfault)
  uint64_t begin_head = begin_zone * info.zone_cap;
  uint64_t write_head = begin_head;
  ASSERT_EQ(channel->AsyncAppend(&write_head, bufferw.buff_, range, 0),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->AsyncAppend(&write_head, bufferw.buff_, range, 1),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->AsyncAppend(&write_head, bufferw.buff_, range, 4),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->GetOutstandingRequests(), 3);
  diag_bytes_written += 3 * range;
  diag_append_ops += 3;
  appends[0] += 3;

  // Ensure that all three request completed with iterative polling
  int sum = 0;
  while (sum < 3) {
    sum = !!channel->PollOnce(0u) + !!channel->PollOnce(1u) +
          !!channel->PollOnce(4u);
  }
  ASSERT_EQ(channel->GetOutstandingRequests(), 0);

  // Enqueue the maximum number of requests
  for (uint32_t i = 0; i < 8; i++) {
    ASSERT_EQ(channel->AsyncAppend(&write_head, bufferw.buff_, range, i),
              SZD::SZDStatus::Success);
  }
  diag_bytes_written += 8 * range;
  diag_append_ops += 8;
  appends[0] += 8;
  ASSERT_EQ(channel->GetOutstandingRequests(), 8);

  // Try to poll till one writer is available
  while (!channel->FindFreeWriter(&any_writer))
    ;
  ASSERT_EQ(channel->GetOutstandingRequests(), 7);

  // Sync the rest of the requests
  ASSERT_EQ(channel->Sync(), SZD::SZDStatus::Success);
  ASSERT_EQ(channel->GetOutstandingRequests(), 0);

  // We can not write beyond the queue depth
  ASSERT_NE(channel->AsyncAppend(&write_head, bufferw.buff_, range, 8),
            SZD::SZDStatus::Success);

  // We can not write more than ZASL
  ASSERT_NE(channel->AsyncAppend(&write_head, bufferw.buff_,
                                 info.zasl + info.lba_size, 0),
            SZD::SZDStatus::Success);

  // We can not write across borders
  ASSERT_EQ(channel->ResetAllZones(), SZD::SZDStatus::Success);
  diag_reset_ops += end_zone - begin_zone;
  resets = std::vector<uint64_t>(end_zone - begin_zone, 2);
  SZDTestUtil::RAIICharBuffer bufferw2((info.zone_cap - 1) * info.lba_size);
  SZDTestUtil::CreateCyclicPattern(bufferw2.buff_,
                                   (info.zone_cap - 1) * info.lba_size, 0);
  write_head = begin_zone * info.zone_cap;
  ASSERT_EQ(channel->DirectAppend(&write_head, bufferw2.buff_,
                                  (info.zone_cap - 1) * info.lba_size, true),
            SZD::SZDStatus::Success);
  diag_bytes_written += (info.zone_cap - 1) * info.lba_size;
  diag_append_ops +=
      expected_steps(begin_zone * info.zone_cap,
                     begin_zone * info.zone_cap + info.zone_cap - 1,
                     info.zone_cap, info.zasl / info.lba_size);
  expected_heat_distr(begin_zone * info.zone_cap, appends,
                      begin_zone * info.zone_cap,
                      begin_zone * info.zone_cap + info.zone_cap - 1,
                      info.zone_cap, info.zasl / info.lba_size);
  ASSERT_NE(channel->AsyncAppend(&write_head, bufferw.buff_, range, 0),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->AsyncAppend(&write_head, bufferw.buff_, info.lba_size, 0),
            SZD::SZDStatus::Success);
  ASSERT_EQ(channel->Sync(), SZD::SZDStatus::Success);
  diag_bytes_written += info.lba_size;
  diag_append_ops += 1;
  appends[0] += 1;

// Yes, we need to test our diagnostics as well
#ifdef SZD_PERF_COUNTERS
  ASSERT_EQ(channel->GetBytesWritten(), diag_bytes_written);
  ASSERT_EQ(channel->GetAppendOperationsCounter(), diag_append_ops);
  ASSERT_EQ(channel->GetZonesResetCounter(), diag_reset_ops);
#ifdef SZD_PERF_PER_ZONE_COUNTERS
  ASSERT_EQ(std::accumulate(appends.begin(), appends.end(), 0),
            diag_append_ops);
  ASSERT_EQ(equal_vectors_diag(appends, channel->GetAppendOperations()), true);
  ASSERT_EQ(std::accumulate(resets.begin(), resets.end(), 0), diag_reset_ops);
  ASSERT_EQ(equal_vectors_diag(resets, channel->GetZonesReset()), true);
#endif
#endif

  factory.unregister_channel(channel);
}

} // namespace
