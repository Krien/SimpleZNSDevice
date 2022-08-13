#include <szd/datastructures/szd_buffer.hpp>
#include <szd/datastructures/szd_circular_log.hpp>
#include <szd/datastructures/szd_fragmented_log.hpp>
#include <szd/datastructures/szd_log.hpp>
#include <szd/datastructures/szd_once_log.hpp>
#include <szd/szd_channel.hpp>
#include <szd/szd_channel_factory.hpp>
#include <szd/szd_device.hpp>

#include <chrono>
#include <cmath>
#include <sys/time.h>

int main(int argc, char** argv) {
    std::string device_to_use = argc <= 1 ? "" : argv[1];
    bool fill  = argc <= 2 ? false : argv[2] == "1";

    // Setup SZD
    SZD::SZDDevice dev("ResetPerfTest");
    dev.Init();
    // Probe devices
    std::string picked_device;
    std::vector<SZD::DeviceOpenInfo> devices_available;
    dev.Probe(devices_available);
    for (auto it = devices_available.begin(); it != devices_available.end(); it++) {
    	if (device_to_use != "" && it->traddr != device_to_use) {
 		continue;
    	}
    	if (it->is_zns) {
       		picked_device.assign(it->traddr);
      		break;
    	}
    }
    // Check picked device is found
    if (picked_device == "") {
	printf("ERROR: No suitable device found\n");
	return 1;
    }
    printf("Using device %s \n", picked_device.data());
    // Open the device for usage
    dev.Open(picked_device);
    // Get device info
    SZD::DeviceInfo info;
    dev.GetInfo(&info);
    // We will be using 1 thread and 2 channels at most
    SZD::SZDChannelFactory factory(dev.GetDeviceManager(), 2);
    SZD::SZDChannel *channel;
    factory.register_channel(&channel);
    // Device must be clean before usage
    printf("cleaning device...\n");
    channel->ResetAllZones();

    // Logs make writes easier
    if (fill) {
	    SZD::SZDOnceLog *log = new SZD::SZDOnceLog(&factory,info,0,info.max_lba/info.zone_size,1,&channel);
	    // Create the write buffer
	    size_t range_to_write = info.zasl;
	    char fill_buff[range_to_write + 1];
	    for (size_t i = 0; i < range_to_write; i++) {
		fill_buff[i] = i % 256;
	    }

	    // Iteratively write
	    printf("Filling device...\n");
	    uint64_t lba = 0;
	    while(log->SpaceLeft(range_to_write)) {
	     log->Append(fill_buff, range_to_write, &lba, true);
	     printf("Space available %lu \n", log->SpaceAvailable());
	    }
	    if (log->SpaceAvailable() > 0) {
	      log->Append(fill_buff, log->SpaceAvailable(),&lba, true);
	    }
    }

    uint64_t begin=0;
    uint64_t end=0;
    uint64_t ti=0, tisq=0;
    uint64_t resets = 0;
    printf("Start reset tests...\n");

    for (uint64_t i = 0; i < info.lba_cap; i += info.zone_size) {
        struct timeval tv;
    	gettimeofday(&tv, nullptr);
        begin = static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
	
	// Workaround for zse translation
	channel->ResetZone((i / info.zone_size) * info.zone_cap);

        gettimeofday(&tv, nullptr);
        end = static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;

        ti += end - begin;
        tisq += (end - begin) * (end - begin);
        resets++;
        printf("Reset %lu %f \n", i, static_cast<double>(end - begin) );
    }

    printf("Test complete:\n");
    printf("%lu zones reset\n", resets);
    printf("\tAVG Reset time %f \n", static_cast<double>(ti) /
               static_cast<double>(resets));
    uint64_t variance = 
      std::sqrt(static_cast<double>(tisq *
                                        resets -
                                    ti *
                                        ti) /
                static_cast<double>(resets *
                                    resets));
    printf("\tSTDEV reset time %ld \n", variance);
    return 0;
}
