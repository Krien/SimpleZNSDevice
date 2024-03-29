#include <gtest/gtest.h>
#include <szd/szd_device.hpp>
#include <szd/szd_status.hpp>

namespace SZDTestUtil {
static void SZDSetupDevice(uint64_t min_zone, uint64_t max_zone,
                           SZD::SZDDevice *device, SZD::DeviceInfo *dinfo) {
  ASSERT_EQ(device->Init(), SZD::SZDStatus::Success);
  std::vector<SZD::DeviceOpenInfo> info;
  ASSERT_EQ(device->Probe(info), SZD::SZDStatus::Success);
  std::string device_to_use = "None";
  for (auto it = info.begin(); it != info.end(); it++) {
    if (it->is_zns) {
      device_to_use.assign(it->traddr);
      printf("using device at traddr %s \n", it->traddr.data());
      break;
    }
  }
  ASSERT_EQ(device->Open(device_to_use, min_zone, max_zone),
            SZD::SZDStatus::Success);
  ASSERT_EQ(device->GetInfo(dinfo), SZD::SZDStatus::Success);
}

void CreateCyclicPattern(char *arr, size_t range, uint64_t jump) {
  for (size_t i = 0; i < range; i++) {
    arr[i] = (i + jump) % 256;
  }
}

struct RAIICharBuffer {
  RAIICharBuffer(size_t size) {
    buff_ = (char*)calloc(size, sizeof(char));
  }

  ~RAIICharBuffer() {
    free(buff_);
  }

  char*  buff_; 
};
} // namespace SZDTestUtil
