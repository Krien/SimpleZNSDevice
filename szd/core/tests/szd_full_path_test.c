
/**
 * \file general tests for the core SZD source.
 * As it needs to deeply test state as well, the test is quite large and has
 * unfortunately become messy.
 * TODO: cleanup...
 */
#ifdef __cplusplus
extern "C" {
#endif

// TODO: remove
#ifdef NDEBUG
#undef NDBEBUG
#endif

// TODO: use a testing framework or something else than raw assert
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <szd/szd.h>
#include <szd/szd_namespace.h>

// TODO: disable by default
#define DEBUG
#ifdef DEBUG
#define DEBUG_TEST_PRINT(str, code)                                            \
  do {                                                                         \
    if ((code) == 0) {                                                         \
      printf("%s\x1B[32m%u\x1B[0m\n", (str), (code));                          \
    } else {                                                                   \
      printf("%s\x1B[31m%u\x1B[0m\n", (str), (code));                          \
    }                                                                          \
  } while (0)
#else
#define DEBUG_TEST_PRINT(str, code)                                            \
  do {                                                                         \
  } while (0)
#endif

#define VALID(rc) assert((rc) == 0)
#define INVALID(rc) assert((rc) != 0)

int write_pattern(char **pattern, QPair *qpair, int32_t size, int32_t jump) {
  // if (*pattern != NULL) {
  //   szd_free(*pattern);
  // }
  *pattern = (char *)szd_calloc(qpair->man->info.lba_size, size, sizeof(char));
  if (*pattern == NULL) {
    return 1;
  }
  for (int j = 0; j < size; j++) {
    (*pattern)[j] = (j + jump) % 200;
  }
  return 0;
}

typedef struct {
  DeviceManager **manager;
  uint64_t write_slba_start;
  uint64_t alt_slba_start;
  int32_t data_offset;
  int32_t alt_offset;
  int rc;
} thread_data;

static pthread_mutex_t mut;
static uint8_t thread_barrier;
#define PLUS_THREAD_BARRIER(mut, bar)                                          \
  pthread_mutex_lock(&mut);                                                    \
  bar += 1;                                                                    \
  pthread_mutex_unlock(&mut);

/* There will be 2 threads. One writes, reads and resets the first zone a 1000
times. The second one, the second zone. Then the two will switch around to see
if they interfere. Hence the need for a barrier and a mutex.
*/
void *worker_thread(void *arg) {
  thread_data *dat = (thread_data *)arg;
  DeviceManager **manager = dat->manager;
  int rc;
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  rc = szd_create_qpair(*manager, qpair);
  if (rc != 0) {
    PLUS_THREAD_BARRIER(mut, thread_barrier);
    dat->rc = rc;
    pthread_exit(NULL);
  }
  uint64_t zone_size_bytes =
      (*manager)->info.lba_size * (*manager)->info.zone_cap;
  char **pattern_1 = (char **)calloc(1, sizeof(char **));
  rc = write_pattern(pattern_1, *qpair, zone_size_bytes, dat->data_offset);
  if (rc != 0) {
    PLUS_THREAD_BARRIER(mut, thread_barrier);
    dat->rc = rc;
    pthread_exit(NULL);
  }
  char *pattern_read_1 = (char *)szd_calloc((*qpair)->man->info.lba_size,
                                            zone_size_bytes, sizeof(char *));
  if (pattern_read_1 == NULL) {
    PLUS_THREAD_BARRIER(mut, thread_barrier);
    dat->rc = rc;
    pthread_exit(NULL);
  }
  // hammering
  for (uint16_t i = 0; i < 200; i++) {
    uint64_t wstart = dat->write_slba_start;
    rc = szd_append(*qpair, &wstart, *pattern_1, zone_size_bytes);
    if (rc != 0) {
      PLUS_THREAD_BARRIER(mut, thread_barrier);
      dat->rc = rc;
      pthread_exit(NULL);
    }
    rc = szd_read(*qpair, dat->write_slba_start, pattern_read_1,
                  zone_size_bytes);
    if (rc != 0) {
      PLUS_THREAD_BARRIER(mut, thread_barrier);
      dat->rc = rc;
      pthread_exit(NULL);
    }
    if (memcmp(pattern_read_1, *pattern_1, zone_size_bytes) != 0) {
      PLUS_THREAD_BARRIER(mut, thread_barrier);
      dat->rc = 1;
      pthread_exit(NULL);
    }
    if (i != 199) {
      rc = szd_reset(*qpair, dat->write_slba_start);
      if (rc != 0) {
        PLUS_THREAD_BARRIER(mut, thread_barrier);
        dat->rc = rc;
        pthread_exit(NULL);
      }
    }
  }

  PLUS_THREAD_BARRIER(mut, thread_barrier);
  pthread_mutex_lock(&mut);
  while (thread_barrier < 2) {
    pthread_mutex_unlock(&mut);
    pthread_mutex_lock(&mut);
  }
  pthread_mutex_unlock(&mut);

  szd_free(pattern_read_1);
  pattern_read_1 = (char *)szd_calloc((*qpair)->man->info.lba_size,
                                      zone_size_bytes, sizeof(char *));
  szd_free(*pattern_1);
  rc = write_pattern(pattern_1, *qpair, zone_size_bytes, dat->alt_offset);
  rc = szd_read(*qpair, dat->alt_slba_start, pattern_read_1, zone_size_bytes);
  if (rc != 0) {
    dat->rc = rc;
    pthread_exit(NULL);
  }
  if (memcmp(pattern_read_1, *pattern_1, zone_size_bytes) != 0) {
    PLUS_THREAD_BARRIER(mut, thread_barrier);
    dat->rc = 1;
    pthread_exit(NULL);
  }
  dat->rc = rc;
  szd_destroy_qpair(*qpair);
  free(qpair);
  free(pattern_1);
  pthread_exit(NULL);
}

int main(int argc, char **argv) {
  int rc;
  printf("----------------------INIT----------------------\n");
  uint64_t min_zone = 2, max_zone = 10;
  DeviceOpenOptions open_opts = {min_zone, max_zone};
  DeviceManager **manager = (DeviceManager **)calloc(1, sizeof(DeviceManager));
  DeviceOptions opts = DeviceOptions_default;
  rc = szd_init(manager, &opts);
  DEBUG_TEST_PRINT("SPDK init ", rc);
  VALID(rc);

  // find devices
  printf("----------------------PROBE----------------------\n");
  char *device_to_use = NULL;
  ProbeInformation **prober =
      (ProbeInformation **)calloc(1, sizeof(ProbeInformation *));
  rc = szd_probe(*manager, prober);
  DEBUG_TEST_PRINT("probe return code ", rc);
  VALID(rc);
  for (int i = 0; i < (*prober)->devices; i++) {
    const char *is_zns = (*prober)->zns[i] ? "true" : "false";
    printf("Device found\n\tname:%s\n\tZNS device:%s\n", (*prober)->traddr[i],
           is_zns);
    if ((*prober)->zns[i]) {
      if (device_to_use) {
        free(device_to_use);
      }
      device_to_use =
          (char *)calloc(strlen((*prober)->traddr[i]) + 1, sizeof(char));
      memcpy(device_to_use, (*prober)->traddr[i], strlen((*prober)->traddr[i]));
    }
  }
  // dangerous! we must be absolutely sure that no other process is using this
  // anymore.
  szd_free_probe_information(*prober);
  free(prober);
  if (!device_to_use) {
    printf("No ZNS Device found.\n Are you sure you have a ZNS device "
           "connected?\n");
    assert(false);
  }
  printf("ZNS device %s found. This device will be used for the rest of the "
         "test.\n",
         device_to_use);

  rc = szd_reinit(manager);
  DEBUG_TEST_PRINT("reinit return code ", rc);
  VALID(rc);

  // init spdk
  printf("----------------------OPENING DEVICE----------------------\n");
  // try non-existent device
  rc = szd_open(*manager, "non-existent traddr", &open_opts);
  DEBUG_TEST_PRINT("non-existent return code ", rc);
  INVALID(rc);

  // try existing device
  rc = szd_open(*manager, device_to_use, &open_opts);
  DEBUG_TEST_PRINT("existing return code ", rc);
  VALID(rc);
  free(device_to_use);

  // ensure that everything from this device is OK
  assert((*manager)->ctrlr != NULL);
  assert((*manager)->ns != NULL);
  assert((*manager)->info.lba_size > 0);
  assert((*manager)->info.mdts > 0);
  assert((*manager)->info.zasl > 0);
  assert((*manager)->info.zone_size > 0);
  assert((*manager)->info.zone_cap > 0);
  assert((*manager)->info.lba_cap > 0);

  // create qpair
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  rc = szd_create_qpair(*manager, qpair);
  DEBUG_TEST_PRINT("Qpair creation code ", rc);
  VALID(rc);
  assert(qpair != NULL);

  // get and verify data (based on ZNS QEMU image)
  DeviceInfo info = {};
  rc = szd_get_device_info(&info, *manager);
  DEBUG_TEST_PRINT("get info code ", rc);
  VALID(rc);
  printf("lba size is %ld\n", info.lba_size);
  printf("zone size is %ld\n", info.zone_size);
  printf("zone cap is %ld\n", info.zone_cap);
  printf("mdts is %ld\n", info.mdts);
  printf("zasl is %ld\n", info.zasl);
  printf("lba_cap is %ld\n", info.lba_cap);
  printf("min lba is %ld\n", info.min_lba);
  printf("max lba is %ld\n", info.max_lba);

  uint64_t write_head;
  uint64_t append_head;
  printf("----------------------WORKLOAD SMALL----------------------\n");
  // make space by resetting the device zones
  rc = szd_reset_all(*qpair);
  DEBUG_TEST_PRINT("reset all code ", rc);
  VALID(rc);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size, &write_head);
  DEBUG_TEST_PRINT("min zone head ", rc);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size);
  char **pattern_1 = (char **)calloc(1, sizeof(char **));
  rc = write_pattern(pattern_1, *qpair, info.lba_size, 10);
  VALID(rc);
  append_head = min_zone * info.zone_size;
  rc = szd_append(*qpair, &append_head, *pattern_1, info.lba_size);
  DEBUG_TEST_PRINT("append alligned ", rc);
  VALID(rc);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size, &write_head);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size + 1);
  char **pattern_2 = (char **)calloc(1, sizeof(char **));
  rc = write_pattern(pattern_2, *qpair, info.zasl, 13);
  VALID(rc);
  rc = szd_append(*qpair, &append_head, *pattern_2, info.zasl);
  DEBUG_TEST_PRINT("append zasl ", rc);
  VALID(rc);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size, &write_head);
  VALID(rc);
  assert(write_head ==
         min_zone * info.zone_size + 1 + info.zasl / info.lba_size);
  char *pattern_read_1 = (char *)szd_calloc((*qpair)->man->info.lba_size,
                                            info.lba_size, sizeof(char *));
  rc = szd_read(*qpair, min_zone * info.zone_size, pattern_read_1,
                info.lba_size);
  DEBUG_TEST_PRINT("read alligned ", rc);
  VALID(rc);
  for (uint64_t i = 0; i < info.lba_size; i++) {
    assert((char)(pattern_read_1)[i] == (char)(*pattern_1)[i]);
  }
  szd_free(*pattern_1);
  szd_free(pattern_read_1);
  char *pattern_read_2 = (char *)szd_calloc((*qpair)->man->info.lba_size,
                                            info.zasl, sizeof(char *));
  rc = szd_read(*qpair, min_zone * info.zone_size + 1, pattern_read_2,
                info.zasl);
  DEBUG_TEST_PRINT("read zasl ", rc);
  VALID(rc);
  for (uint64_t i = 0; i < info.zasl; i++) {
    assert((char)(pattern_read_2)[i] == (char)(*pattern_2)[i]);
  }
  szd_free(*pattern_2);
  rc = szd_reset_all(*qpair);
  DEBUG_TEST_PRINT("reset all ", rc);
  VALID(rc);
  rc = szd_read(*qpair, min_zone * info.zone_size + 1, pattern_read_2,
                info.zasl);
  DEBUG_TEST_PRINT("verify empty first zone ", rc);
  VALID(rc);
  for (uint64_t i = 0; i < info.zasl; i++) {
    assert((char)(pattern_read_2)[i] == 0);
  }
  szd_free(pattern_read_2);

  append_head = min_zone * info.zone_size;
  printf("----------------------WORKLOAD FILL----------------------\n");
  uint64_t number_of_zones = (info.max_lba - info.min_lba) / info.zone_size;
  char **pattern_3 = (char **)calloc(1, sizeof(char **));
  rc = write_pattern(pattern_3, *qpair,
                     info.lba_size * number_of_zones * info.zone_cap, 19);
  VALID(rc);
  rc = szd_append(*qpair, &append_head, *pattern_3,
                  info.lba_size * number_of_zones * info.zone_cap);
  DEBUG_TEST_PRINT("fill entire device ", rc);
  VALID(rc);
  for (uint64_t i = info.min_lba; i < info.max_lba; i += info.zone_size) {
    rc = szd_get_zone_head(*qpair, i, &write_head);
    VALID(rc);
    assert(write_head == i + info.zone_size);
  }
  szd_free(*pattern_3);
  char *pattern_read_3 = (char *)szd_calloc(
      (*qpair)->man->info.lba_size,
      info.lba_size * number_of_zones * info.zone_cap, sizeof(char *));
  assert(pattern_read_3 != NULL);
  rc = szd_read(*qpair, min_zone * info.zone_size, pattern_read_3,
                info.lba_size * number_of_zones * info.zone_cap);
  DEBUG_TEST_PRINT("read entire device ", rc);
  VALID(rc);
  for (uint64_t i = 0; i < info.lba_size * number_of_zones * info.zone_cap;
       i++) {
    assert((char)(pattern_read_3)[i] == (char)(*pattern_3)[i]);
  }
  szd_free(pattern_read_3);
  rc = szd_reset(*qpair, min_zone * info.zone_size + info.zone_size);
  rc = szd_reset(*qpair, min_zone * info.zone_size + info.zone_size * 2) | rc;
  DEBUG_TEST_PRINT("reset zone 2,3 ", rc);
  VALID(rc);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size, &write_head);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size + info.zone_size);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size + info.zone_size,
                         &write_head);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size + info.zone_size);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size + info.zone_size * 2,
                         &write_head);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size + info.zone_size * 2);
  char *pattern_read_4 =
      (char *)szd_calloc((*qpair)->man->info.lba_size,
                         info.lba_size * info.zone_cap, sizeof(char *));
  rc = szd_read(*qpair, info.zone_size * min_zone, pattern_read_4,
                info.lba_size * info.zone_cap);
  DEBUG_TEST_PRINT("read zone 1 ", rc);
  VALID(rc);
  for (uint64_t i = 0; i < info.lba_size * info.zone_cap; i++) {
    assert((char)(pattern_read_4)[i] == (char)(*pattern_3)[i]);
  }
  rc = szd_read(*qpair, min_zone * info.zone_size + info.zone_size,
                pattern_read_4, info.lba_size * info.zone_cap);
  DEBUG_TEST_PRINT("read zone 2 ", rc);
  VALID(rc);
  for (uint64_t i = 0; i < info.lba_size * info.zone_cap; i++) {
    assert((char)(pattern_read_4)[i] == 0);
  }
  rc = szd_read(*qpair, min_zone * info.zone_size + info.zone_size * 2,
                pattern_read_4, info.lba_size * info.zone_cap);
  DEBUG_TEST_PRINT("read zone 3 ", rc);
  VALID(rc);
  for (uint64_t i = 0; i < info.lba_size * info.zone_cap; i++) {
    assert((char)(pattern_read_4)[i] == 0);
  }
  rc = szd_read(*qpair, min_zone * info.zone_size + info.zone_size * 3,
                pattern_read_4, info.lba_size * info.zone_cap);
  DEBUG_TEST_PRINT("read zone 4 ", rc);
  VALID(rc);
  // This ugly loop is necessary to prevent over-allocating DMA. We only want
  // one lba at a time.
  rc = write_pattern(pattern_3, *qpair, info.lba_size,
                     19 + info.zone_cap * 3 * info.lba_size);
  for (uint64_t i = 0; i < info.lba_size * info.zone_cap; i++) {
    if (i % info.lba_size == 0 && i > 0) {
      szd_free(*pattern_3);
      rc = write_pattern(pattern_3, *qpair, info.lba_size,
                         19 + i + info.zone_cap * 3 * info.lba_size);
      VALID(rc);
    }
    assert((char)(pattern_read_4)[i] == (char)(*pattern_3)[i % info.lba_size]);
  }
  rc = szd_reset_all(*qpair);
  DEBUG_TEST_PRINT("reset all ", rc);
  VALID(rc);

  append_head = min_zone * info.zone_size;
  printf("----------------------WORKLOAD ZONE EDGE----------------------\n");
  rc = write_pattern(pattern_3, *qpair, info.lba_size * info.zone_cap * 2, 19);
  rc = szd_append(*qpair, &append_head, *pattern_3,
                  info.lba_size * (info.zone_cap - 3));
  DEBUG_TEST_PRINT("zone friction part 1: append 1 zoneborder - 3 ", rc);
  VALID(rc);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size, &write_head);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size + info.zone_cap - 3);
  rc = szd_append(*qpair, &append_head,
                  *pattern_3 + info.lba_size * (info.zone_cap - 3),
                  info.lba_size * 6);
  DEBUG_TEST_PRINT("zone friction part 2: append 1 zoneborder + 6 ", rc);
  VALID(rc);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size, &write_head);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size + info.zone_size);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size + info.zone_size,
                         &write_head);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size + info.zone_size + 3);
  rc = szd_append(*qpair, &append_head,
                  *pattern_3 + info.lba_size * (info.zone_cap + 3),
                  info.lba_size * 13);
  DEBUG_TEST_PRINT("zone friction part 3: append 1 zoneborder + 16 ", rc);
  VALID(rc);
  rc = szd_get_zone_head(*qpair, min_zone * info.zone_size + info.zone_size,
                         &write_head);
  VALID(rc);
  assert(write_head == min_zone * info.zone_size + info.zone_size + 16);
  rc = szd_read(*qpair, min_zone * info.zone_size, pattern_read_4,
                info.lba_size * (info.zone_cap - 3));
  DEBUG_TEST_PRINT("zone friction part 4: read 1 zoneborder - 3 ", rc);
  VALID(rc);
  rc = szd_read(*qpair, min_zone * info.zone_size + info.zone_cap - 3,
                pattern_read_4 + info.lba_size * (info.zone_cap - 3),
                info.lba_size * 6);
  DEBUG_TEST_PRINT("zone friction part 5: read 1 zoneborder + 3 ", rc);
  VALID(rc);
  rc = szd_read(*qpair, min_zone * info.zone_size + info.zone_cap + 3,
                pattern_read_4 + info.lba_size * (info.zone_cap + 3),
                info.lba_size * 13);
  DEBUG_TEST_PRINT("zone friction part 6: read 1 zoneborder + 16 ", rc);
  VALID(rc);
  for (uint64_t i = 0; i < info.lba_size * (info.zone_cap + 15); i++) {
    assert((char)(pattern_read_4)[i] == (char)(*pattern_3)[i]);
  }
  szd_free(*pattern_3);
  szd_free(pattern_read_4);
  rc = szd_reset_all(*qpair);
  DEBUG_TEST_PRINT("reset all ", rc);
  VALID(rc);

  printf(
      "----------------------WORKLOAD MULTITHREADING----------------------\n");
  printf("This might take a time...\n");
  pthread_mutex_init(&mut, NULL);
  pthread_t thread1;
  thread_data first_thread_dat = {.manager = manager,
                                  .write_slba_start = min_zone * info.zone_size,
                                  .alt_slba_start = min_zone * info.zone_size +
                                                    2 * info.zone_size,
                                  .data_offset = 3,
                                  .alt_offset = 9,
                                  .rc = 0};
  rc = pthread_create(&thread1, NULL, worker_thread, (void *)&first_thread_dat);
  VALID(rc);
  pthread_t thread2;
  thread_data second_thread_dat = {
      .manager = manager,
      .write_slba_start = min_zone * info.zone_size + 2 * info.zone_size,
      .alt_slba_start = min_zone * info.zone_size,
      .data_offset = 9,
      .alt_offset = 3,
      .rc = 0};
  rc =
      pthread_create(&thread2, NULL, worker_thread, (void *)&second_thread_dat);
  VALID(rc);

  if (pthread_join(thread1, NULL) != 0) {
    DEBUG_TEST_PRINT("Error in thread1 ", 1);
  }
  rc = first_thread_dat.rc;
  DEBUG_TEST_PRINT("thread 2 writes and reads ", rc);
  VALID(rc);
  if (pthread_join(thread2, NULL) != 0) {
    DEBUG_TEST_PRINT("Error in thread2 ", 1);
  }
  rc = second_thread_dat.rc;
  DEBUG_TEST_PRINT("thread 3 writes and reads ", rc);
  VALID(rc);

  printf("----------------------CLOSE----------------------\n");
  // destroy qpair
  rc = szd_destroy_qpair(*qpair);
  DEBUG_TEST_PRINT("valid destroy code ", rc);
  VALID(rc);

  // close device
  rc = szd_close(*manager);
  DEBUG_TEST_PRINT("valid close code ", rc);
  VALID(rc);

  // can not close twice
  rc = szd_close(*manager);
  DEBUG_TEST_PRINT("invalid close code ", rc);
  INVALID(rc);

  rc = szd_destroy(*manager);
  DEBUG_TEST_PRINT("valid shutdown code ", rc);
  VALID(rc);

  // cleanup local
  free(pattern_1);
  free(pattern_2);
  free(pattern_3);

  free(qpair);
  free(manager);
  // Werror-unused-parameter
  (void)argc;
  (void)argv;
}

#ifdef __cplusplus
}
#endif
