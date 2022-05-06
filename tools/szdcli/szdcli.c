#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <szd/szd.h>
#include <unistd.h>

int min_zns(uint64_t l1, uint64_t l2) { return l1 < l2 ? l1 : l2; }

int print_help_util() {
  fprintf(
      stdout,
      "znscli [options]\n"
      "options:\n"
      " probe   get trid from all devices and ZNS indicators\n"
      " info    get device information (sizes etc.)\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      " zones   get write heads from each zone and print them\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request "
      "zoneinformation from\n"
      " reset   reset zones on a ZNS device\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      "   -l <slba>   REQUIRED - slba of zone to reset\n"
      "   -a          OPTIONAL - reset the whole device instead of one zone\n"
      " write   append data to zones on a ZNS device\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      "   -l <slba>   REQUIRED - slba of zone to append to\n"
      "   -s <size>   REQUIRED - bytes to write in multiple of lba_size\n"
      "   -d <data>   REQUIRED - data to write to the device\n"
      " read    read bytes from a ZNS device\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      "   -l <slba>   REQUIRED - slba of zone to read from (does not need to "
      "be alligned to a zone)\n"
      "   -s <size>   REQUIRED - bytes to read in multiple of lba_size\n");
  return 0;
}

void print_disclaimer() {
  fprintf(
      stdout,
      "DISCLAIMER:\n"
      " This tool is not tested for security concerns (buffer overflows etc.), "
      "use at own risk!\n"
      " This tool is meant to debug ZNS device, not for actual production "
      "use.\n"
      " The tool will also only work properly with NVMe ZNS devices only\n");
}

int parse_reset_zns(int argc, char **argv, DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(0x100, sizeof(char *));
  bool trid_set = false;
  bool reset_all = false;
  int64_t slba = -1;
  while ((op = getopt(argc, argv, "t:l:a::")) != -1) {
    switch (op) {
    case 'l':
      slba = (int64_t)szd_spdk_strtol(optarg, 10);
      if (slba < 0) {
        free(*manager);
        free(trid);
        fprintf(stderr, "Invalid slba\n");
        return slba;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(trid, 0x100, "%s", optarg);
      break;
    case 'a':
      reset_all = true;
      break;
    default:
      free(*manager);
      free(trid);
      return 1;
    }
  }
  if (!trid_set || slba < 0) {
    fprintf(stderr, "Not all required arguments are set for reset\n");
    print_help_util();
    free(*manager);
    free(trid);
    return 1;
  }
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int rc = szd_open(*manager, trid, &ooptions);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    print_help_util();
    free(*manager);
    free(trid);
    return 1;
  }
  DeviceInfo info = (*manager)->info;
  if (info.lba_cap < (uint64_t)slba || (uint64_t)slba % info.zone_size != 0) {
    fprintf(stderr, "Invalid slba \n");
    szd_destroy(*manager);
    free(trid);
    return 1;
  }
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  rc = szd_create_qpair(*manager, qpair);
  if (rc != 0) {
    free(qpair);
    szd_destroy(*manager);
    free(trid);
    return rc;
  }
  if (reset_all) {
    fprintf(stdout, "Resetting complete device %s \n", trid);
  } else {
    fprintf(stdout, "Resetting device %s at %lu \n", trid, (uint64_t)slba);
  }
  if (reset_all) {
    rc = szd_reset_all(*qpair);
  } else {
    rc = szd_reset(*qpair, (uint64_t)slba);
  }
  szd_destroy_qpair(*qpair);
  free(qpair);
  szd_destroy(*manager);
  free(trid);
  return rc;
}

int parse_read_zns(int argc, char **argv, DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(MAX_TRADDR_LENGTH, sizeof(char *));
  bool trid_set = false;
  int64_t lba = -1;
  int64_t size = -1;
  while ((op = getopt(argc, argv, "t:l:s:")) != -1) {
    switch (op) {
    case 'l':
      lba = (int64_t)szd_spdk_strtol(optarg, 10);
      if (lba < 0) {
        fprintf(stderr, "Invalid lba %s\n", optarg);
        print_help_util();
        free(*manager);
        free(trid);
        return lba;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(trid, MAX_TRADDR_LENGTH - 1, "%s", optarg);
      break;
    case 's':
      size = (int64_t)szd_spdk_strtol(optarg, 10);
      if (size < 0) {
        free(*manager);
        free(trid);
        fprintf(stderr, "Invalid size %s\n", optarg);
        print_help_util();
        return size;
      }
      break;
    default:
      free(*manager);
      free(trid);
      return 1;
    }
  }
  if (!trid_set || lba < 0 || size < 0) {
    fprintf(stderr, "Not all required arguments are set for a read\n");
    print_help_util();
    free(*manager);
    free(trid);
    return 1;
  }
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int rc = szd_open(*manager, trid, &ooptions);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    print_help_util();
    free(trid);
    return 1;
  }
  DeviceInfo info = (*manager)->info;
  if (info.lba_cap < (uint64_t)lba || (uint64_t)size % info.lba_size != 0) {
    fprintf(stderr,
            "Invalid slba or size \n"
            " requested lba:%lu <-CHECK-> lba capacity: %lu\n"
            " requested size:%lu <-CHECK-> lba size %lu",
            (uint64_t)lba, info.lba_cap, (uint64_t)size, info.lba_size);
    free(trid);
    szd_destroy(*manager);
    return 1;
  }
  size = min_zns(size, (info.lba_cap - lba) * info.lba_size);
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  rc = szd_create_qpair(*manager, qpair);
  if (rc != 0) {
    fprintf(stderr, "Error creating qpair\n");
    free(qpair);
    szd_destroy(*manager);
    free(trid);
    return rc;
  }
  char *data = (char *)szd_calloc(info.lba_size, size, sizeof(char *));
  if (!data) {
    fprintf(stderr, "Error allocating with SPDKs malloc\n");
    szd_destroy_qpair(*qpair);
    free(qpair);
    szd_destroy(*manager);
    free(trid);
    return rc;
  }
  rc = szd_read(*qpair, lba, data, size);
  if (rc != 0) {
    fprintf(stderr, "Error reading %d\n", rc);
    szd_free(data);
    szd_destroy_qpair(*qpair);
    free(qpair);
    rc = szd_destroy(*manager);
    free(trid);
    return rc;
  }
  fprintf(stdout, "Read data from device %s, location %lu, size %lu:\n", trid,
          lba, size);
  // prevent stopping on a \0.
  for (int i = 0; i < size; i++) {
    fprintf(stdout, "%c", data[i]);
  }
  fprintf(stdout, "\n");
  szd_free(data);
  szd_destroy_qpair(*qpair);
  free(qpair);
  rc = szd_destroy(*manager);
  free(trid);
  return rc;
}

int parse_write_zns(int argc, char **argv, DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(MAX_TRADDR_LENGTH, sizeof(char));
  bool trid_set = false;
  int64_t lba = -1;
  int64_t size = -1;
  int64_t data_size = -1;
  char *data = NULL;
  while ((op = getopt(argc, argv, "t:l:s:d:")) != -1) {
    switch (op) {
    case 'l':
      lba = (int64_t)szd_spdk_strtol(optarg, 10);
      if (lba < 0) {
        free(*manager);
        free(trid);
        fprintf(stderr, "Invalid lba %s\n", optarg);
        print_help_util();
        return lba;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(trid, MAX_TRADDR_LENGTH - 1, "%s", optarg);
      break;
    case 's':
      size = (int64_t)szd_spdk_strtol(optarg, 10);
      if (size < 0) {
        free(*manager);
        free(trid);
        fprintf(stderr, "Invalid size %s\n", optarg);
        print_help_util();
        return size;
      }
      break;
    case 'd':
      data_size = strlen(optarg) + 1;
      data = (char *)calloc(data_size, sizeof(char));
      snprintf(data, data_size, "%s", optarg);
      break;
    default:
      free(*manager);
      free(trid);
      return 1;
    }
  }
  if (!trid_set || lba < 0 || size < 0 || !data) {
    free(trid);
    if (data != NULL) {
      free(data);
    }
    free(*manager);
    fprintf(stderr, "Not all required arguments are set for a write\n");
    print_help_util();
    return 1;
  }
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int rc = szd_open(*manager, trid, &ooptions);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    print_help_util();
    free(data);
    free(trid);
    return 1;
  }
  DeviceInfo info = (*manager)->info;
  if (info.lba_cap < (uint64_t)lba || (uint64_t)size % info.lba_size != 0 ||
      (uint64_t)lba % info.zone_size != 0) {
    fprintf(stderr,
            "Invalid slba or size \n"
            " requested lba:%lu <-CHECK-> lba capacity: %lu, zone size: %lu\n"
            " requested size:%lu <-CHECK-> lba size %lu",
            (uint64_t)lba, info.lba_cap, info.zone_size, (uint64_t)size,
            info.lba_size);
    print_help_util();
    free(data);
    free(trid);
    szd_destroy(*manager);
    return 1;
  }
  size = min_zns(size, (info.lba_cap - lba) * info.lba_size);
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  rc = szd_create_qpair(*manager, qpair);
  if (rc != 0) {
    fprintf(stderr, "Error creating qpair %d\n", rc);
    free(qpair);
    szd_destroy(*manager);
    free(data);
    free(trid);
    return rc;
  }
  char *data_spdk = (char *)szd_calloc(info.lba_size, size, sizeof(char *));
  if (!data_spdk) {
    fprintf(stderr, "Error allocating with SPDKs malloc\n");
    goto destroy_context;
    return rc;
  }
  snprintf(data_spdk, min_zns(data_size, size), "%s", data);
  rc = szd_append(*qpair, (uint64_t *)&lba, data_spdk, size);
  if (rc != 0) {
    fprintf(stderr, "Error appending %d\n", rc);
    szd_free(data_spdk);
    goto destroy_context;
    return rc;
  }
  fprintf(stdout, "write data to device %s, location %lu, size %lu\n", trid,
          lba, size);
  szd_free(data_spdk);
  goto destroy_context;
  return rc;

destroy_context:
  szd_destroy_qpair(*qpair);
  free(qpair);
  rc = szd_destroy(*manager);
  free(data);
  free(trid);
  return rc;
}

int parse_probe_zns(int argc, char **argv, DeviceManager **manager) {
  (void)argc;
  (void)argv;
  ProbeInformation **prober =
      (ProbeInformation **)calloc(1, sizeof(ProbeInformation *));
  printf("Looking for devices:\n");
  int rc = szd_probe(*manager, prober);
  if (rc != 0) {
    printf("Fatal error during probing %d\n Are you sure you are running as "
           "root?\n",
           rc);
    free(prober);
    return 1;
  }
  for (int i = 0; i < (*prober)->devices; i++) {
    const char *is_zns = (*prober)->zns[i] ? "true" : "false";
    printf("Device found\n\tname:%s\n\tZNS device:%s\n", (*prober)->traddr[i],
           is_zns);
    free((*prober)->traddr[i]);
  }
  (*prober)->devices = 0;
  // dangerous! we must be absolutely sure that no other process is using this
  // anymore.
  free((*prober)->traddr);
  free((*prober)->mut);
  free((*prober)->ctrlr);
  free((*prober)->zns);
  free(*prober);
  free(prober);
  szd_destroy(*manager);
  return 0;
}

int parse_info_zns(int argc, char **argv, DeviceManager **manager) {
  (void)argc;
  (void)argv;
  int op;
  char *trid = (char *)calloc(MAX_TRADDR_LENGTH, sizeof(char));
  bool trid_set = false;
  while ((op = getopt(argc, argv, "t:")) != -1) {
    switch (op) {
    case 't':
      trid_set = true;
      snprintf(trid, MAX_TRADDR_LENGTH - 1, "%s", optarg);
      break;
    default:
      free(trid);
      return 1;
    }
  }
  if (!trid_set) {
    free(trid);
    return 1;
  }
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int rc = szd_open(*manager, trid, &ooptions);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    free(trid);
    return 1;
  }
  DeviceInfo info = (*manager)->info;
  fprintf(stdout,
          "Getting information from device %s:\n"
          "\t*lba size (in bytes)    :%lu\n"
          "\t*zone capacity (in lbas):%lu\n"
          "\t*amount of zones        :%lu\n"
          "\t*total amount of lbas   :%lu\n"
          "\t*mdts (in bytes)        :%lu\n"
          "\t*zasl (in bytes)        :%lu\n",
          trid, info.lba_size, info.zone_size, info.lba_cap / info.zone_size,
          info.lba_cap, info.mdts, info.zasl);
  free(trid);
  szd_destroy(*manager);
  return rc;
}

int parse_zones_zns(int argc, char **argv, DeviceManager **manager) {
  int op;
  char *trid = (char *)calloc(MAX_TRADDR_LENGTH, sizeof(char));
  bool trid_set = false;
  while ((op = getopt(argc, argv, "t:")) != -1) {
    switch (op) {
    case 't':
      trid_set = true;
      snprintf(trid, MAX_TRADDR_LENGTH - 1, "%s", optarg);
      break;
    default:
      free(trid);
      return 1;
    }
  }
  if (!trid_set) {
    return 1;
  }
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int rc = szd_open(*manager, trid, &ooptions);
  if (rc != 0) {
    fprintf(stderr, "Invalid trid %s\n", trid);
    free(trid);
    print_help_util();
    return 1;
  }
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  rc = szd_create_qpair(*manager, qpair);
  if (rc != 0) {
    fprintf(stderr, "Error allocating with SPDKs malloc %d\n", rc);
    free(trid);
    free(qpair);
    szd_destroy(*manager);
    return rc;
  }
  DeviceInfo info = (*manager)->info;
  printf("Printing zone writeheads for device %s:\n", trid);
  uint64_t zone_head;
  for (uint64_t i = 0; i < info.lba_cap; i += info.zone_size) {
    rc = szd_get_zone_head(*qpair, i, &zone_head);
    if (rc != 0) {
      fprintf(stderr, "Error during getting zonehead %d\n", rc);
      return 1;
    }
    printf("\tslba:%6lu - wp:%6lu - %lu/%lu\n", i, zone_head, zone_head - i,
           info.zone_size);
  }
  free(trid);
  szd_destroy_qpair(*qpair);
  free(qpair);
  szd_destroy(*manager);
  return rc;
}

int parse_help_zns(int argc, char **argv, DeviceManager **manager) {
  (void)argc;
  (void)argv;
  (void)manager;
  return print_help_util();
}

int parse_args_zns(int argc, char **argv, DeviceManager **manager) {
  if (!argc) {
    return 1;
  }
// checks if first word matches a command, if it is try to parse the rest and
// return the rc.
#define TRY_PARSE_ARGS_ZNS_COMMAND(FUNCTIONNAME, argc, argv, manager)          \
  do {                                                                         \
    if (strncmp((argv)[0], #FUNCTIONNAME,                                      \
                min_zns(strlen(argv[0]), strlen(#FUNCTIONNAME))) == 0) {       \
      return parse_##FUNCTIONNAME##_zns(argc, argv, manager);                  \
    }                                                                          \
  } while (0)
  TRY_PARSE_ARGS_ZNS_COMMAND(reset, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(read, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(write, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(info, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(probe, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(zones, argc, argv, manager);
  TRY_PARSE_ARGS_ZNS_COMMAND(help, argc, argv, manager);
  fprintf(stderr, "Command not recognised...\n");
  return print_help_util();
}

int main(int argc, char **argv) {
  int rc;
  if (argc < 2) {
    printf("Not enough args provided\n");
    print_help_util();
    print_disclaimer();
    return 1;
  }

  DeviceManager **manager =
      (DeviceManager **)calloc(1, sizeof(DeviceManager));
  DeviceOptions options = {.name = "znscli", .setup_spdk = true};
  rc = szd_init(manager, &options);
  if (rc != 0) {
    free(manager);
    print_disclaimer();
    return rc;
  }

  rc = parse_args_zns(argc - 1, &argv[1], manager);
  if (rc != 0) {
    printf("error during operation \n");
    free(manager);
    print_disclaimer();
    return rc;
  }
  free(manager);
  print_disclaimer();
  return rc;
}

#ifdef __cplusplus
}
#endif
