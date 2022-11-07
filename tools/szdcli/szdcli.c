#ifdef __cplusplus
extern "C" {
#endif

#include <getopt.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <szd/szd.h>
#include <unistd.h>

#define MAX_TRADDR_LENGTH 0x100
#define ERROR_STATE 1

typedef struct {
  DeviceManager **dev_manager;
  DeviceOptions dev_options;
  char *target_trid;
} CliContext;

int min_uint64_t(uint64_t l1, uint64_t l2) { return l1 < l2 ? l1 : l2; }

int print_help_util() {
  fprintf(
      stdout,
      "szdcli [options]\n"
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
      " append   append data to zones on a ZNS device\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      "   -l <slba>   REQUIRED - slba of zone to append to\n"
      "   -s <size>   REQUIRED - bytes to append (must be multiple of "
      "lba_size and needs to be human-readable, no weird bytes)\n"
      "   -d <data>   REQUIRED - data to append to the device\n"
      " read    read bytes from a ZNS device\n"
      "   -t <trid>   REQUIRED - ZNS trid of the device to request information "
      "from\n"
      "   -l <slba>   REQUIRED - slba of zone to read from (does not need to "
      "be alligned to a zone)\n"
      "   -s <size>   REQUIRED - bytes to read (must be multiple of "
      "lba_size)\n");
  return 0;
}

void print_disclaimer() {
  fprintf(
      stdout,
      "DISCLAIMER:\n"
      " This tool is not tested for security concerns (buffer overflows etc.), "
      "use at your own risk!\n"
      " This tool is meant to debug ZNS device, not for actual production "
      "use.\n"
      " The tool will also only work properly with NVMe ZNS devices only\n");
}

int parse_reset_zns(int argc, char **argv, CliContext *cli_context) {
  // Parse and setup args
  bool trid_set = false;
  bool reset_all = false;
  int64_t slba = -1;
  int op = -1;
  while ((op = getopt(argc, argv, "t:l:a::")) != -1) {
    switch (op) {
    case 'l':
      slba = (int64_t)szd_spdk_strtol(optarg, 10);
      if (slba < 0) {
        fprintf(stderr, "Reset request: invalid slba %lu\n", slba);
        return ERROR_STATE;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(cli_context->target_trid, MAX_TRADDR_LENGTH, "%s", optarg);
      break;
    case 'a':
      reset_all = true;
      break;
    }
  }
  if (!trid_set || slba < 0) {
    fprintf(stderr, "Reset request: Not all required arguments are set\n");
    return ERROR_STATE;
  }

  // Open device and verify args for device
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int szd_rc;
  if ((szd_rc = szd_open(*cli_context->dev_manager, cli_context->target_trid,
                         &ooptions))) {
    fprintf(stderr, "Reset request: Invalid trid %s\n",
            cli_context->target_trid);
    return ERROR_STATE;
  }
  DeviceInfo info = (*cli_context->dev_manager)->info;
  if (info.lba_cap < (uint64_t)slba || (uint64_t)slba % info.zone_size != 0) {
    fprintf(stderr, "Reset request: invalid slba %lu\n", slba);
    return ERROR_STATE;
  }

  // Setup QPair
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  if ((szd_rc = szd_create_qpair(*cli_context->dev_manager, qpair))) {
    free(qpair);
    return ERROR_STATE;
  }

  // Reset
  if (reset_all) {
    fprintf(stdout, "Info: Resetting all zones on device %s\n",
            cli_context->target_trid);
  } else {
    fprintf(stdout, "Info: Resetting zone at slba %lu of device %s\n",
            (uint64_t)slba, cli_context->target_trid);
  }
  szd_rc =
      reset_all ? szd_reset_all(*qpair) : szd_reset(*qpair, (uint64_t)slba);

  szd_destroy_qpair(*qpair);
  free(qpair);
  return szd_rc != 0 ? ERROR_STATE : 0;
}

int parse_read_zns(int argc, char **argv, CliContext *cli_context) {
  // parse and setup args
  bool trid_set = false;
  int64_t lba = -1;
  int64_t size = -1;
  int op = -1;
  while ((op = getopt(argc, argv, "t:l:s:")) != -1) {
    switch (op) {
    case 'l':
      lba = (int64_t)szd_spdk_strtol(optarg, 10);
      if (lba < 0) {
        fprintf(stderr, "Read request: Invalid lba %s\n", optarg);
        return ERROR_STATE;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(cli_context->target_trid, MAX_TRADDR_LENGTH, "%s", optarg);
      break;
    case 's':
      size = (int64_t)szd_spdk_strtol(optarg, 10);
      if (size < 0) {
        fprintf(stderr, "Read request: Invalid size %s\n", optarg);
        return ERROR_STATE;
      }
      break;
    default:
      return ERROR_STATE;
    }
  }
  if (!trid_set || lba < 0 || size < 0) {
    fprintf(stderr, "Not all required arguments are set for a read\n");
    return ERROR_STATE;
  }

  // Open device and verify args for device
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int szd_rc;
  if ((szd_rc = szd_open(*cli_context->dev_manager, cli_context->target_trid,
                         &ooptions))) {
    fprintf(stderr, "Read request: Invalid trid %s\n",
            cli_context->target_trid);
    return ERROR_STATE;
  }
  DeviceInfo info = (*cli_context->dev_manager)->info;
  if (info.lba_cap < (uint64_t)lba || (uint64_t)size % info.lba_size != 0) {
    fprintf(stderr,
            "Read request: Invalid slba or size \n"
            " requested lba:%lu <-CHECK-> lba capacity: %lu\n"
            " requested size:%lu <-CHECK-> lba size %lu",
            (uint64_t)lba, info.lba_cap, (uint64_t)size, info.lba_size);
    return 1;
  }
  size = min_uint64_t(size, (info.lba_cap - lba) * info.lba_size);

  // Setup Qpair
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  if ((szd_rc = szd_create_qpair(*cli_context->dev_manager, qpair))) {
    fprintf(stderr, "Read request: Error creating qpair\n");
    free(qpair);
    return ERROR_STATE;
  }

  // Setup DMA buffer to hold the data to "read"
  char *data = (char *)szd_calloc(info.lba_size, size, sizeof(char *));
  if (!data) {
    fprintf(stderr, "Read request: Error allocating read buffer. Check "
                    "available hugepages.\n");
    szd_destroy_qpair(*qpair);
    free(qpair);
    return ERROR_STATE;
  }

  // Run command
  if ((szd_rc = szd_read(*qpair, lba, data, size))) {
    fprintf(stderr, "Read request: Error reading %d\n", szd_rc);
    szd_free(data);
    szd_destroy_qpair(*qpair);
    free(qpair);
    return ERROR_STATE;
  }
  fprintf(stdout, "Info: Read data from lba %lu with size %lu from device %s\n",
          lba, size, cli_context->target_trid);

  // Write read data to stdout and prevent stopping on a \0.
  for (int i = 0; i < size; i++) {
    fprintf(stdout, "%c", data[i]);
  }
  fprintf(stdout, "\n");

  szd_free(data);
  szd_destroy_qpair(*qpair);
  free(qpair);
  return 0;
}

int parse_append_zns(int argc, char **argv, CliContext *cli_context) {
  // Parse and setup args
  bool trid_set = false;
  int64_t lba = -1;
  int64_t size = -1;
  int64_t data_size = -1;
  int op = -1;
  char *data = NULL;
  while ((op = getopt(argc, argv, "t:l:s:d:")) != -1) {
    switch (op) {
    case 'l':
      lba = (int64_t)szd_spdk_strtol(optarg, 10);
      if (lba < 0) {
        fprintf(stderr, "Append request: Invalid lba %s\n", optarg);
        return ERROR_STATE;
      }
      break;
    case 't':
      trid_set = true;
      snprintf(cli_context->target_trid, MAX_TRADDR_LENGTH, "%s", optarg);
      break;
    case 's':
      size = (int64_t)szd_spdk_strtol(optarg, 10);
      if (size < 0) {
        fprintf(stderr, "Append request: Invalid size %s\n", optarg);
        return size;
      }
      break;
    case 'd':
      data_size = strlen(optarg) + 1;
      data = (char *)calloc(data_size, sizeof(char));
      snprintf(data, data_size, "%s", optarg);
      break;
    default:
      return ERROR_STATE;
    }
  }
  if (!trid_set || lba < 0 || size < 0 || !data) {
    if (data != NULL) {
      free(data);
    }
    fprintf(
        stderr,
        "Append request: Not all required arguments are set for an append\n");
    return ERROR_STATE;
  }

  // Open device and verify args for device
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int szd_rc;
  if ((szd_rc = szd_open(*cli_context->dev_manager, cli_context->target_trid,
                         &ooptions))) {
    fprintf(stderr, "Append request: Invalid trid %s\n",
            cli_context->target_trid);
    free(data);
    return ERROR_STATE;
  }
  DeviceInfo info = (*cli_context->dev_manager)->info;
  if (info.lba_cap < (uint64_t)lba || (uint64_t)size % info.lba_size != 0 ||
      (uint64_t)lba % info.zone_size != 0) {
    fprintf(stderr,
            "Append request: Invalid slba or size \n"
            " requested lba:%lu <-CHECK-> lba capacity: %lu, zone size: %lu\n"
            " requested size:%lu <-CHECK-> lba size %lu\n",
            (uint64_t)lba, info.lba_cap, info.zone_size, (uint64_t)size,
            info.lba_size);
    free(data);
    return ERROR_STATE;
  }
  size = min_uint64_t(size, (info.lba_cap - lba));

  // Setup Qpair
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  if ((szd_rc = szd_create_qpair(*cli_context->dev_manager, qpair))) {
    fprintf(stderr, "Append request: Error creating qpair %d\n", szd_rc);
    free(qpair);
    free(data);
    return ERROR_STATE;
  }

  // Setup DMA buffer and copy data to write it
  char *data_spdk = (char *)szd_calloc(info.lba_size, size, sizeof(char *));
  if (!data_spdk) {
    fprintf(stderr, "Append request: Error allocating DMA memory. Check "
                    "hugepages available.\n");
    goto destroy_context;
    return ERROR_STATE;
  }
  snprintf(data_spdk, min_uint64_t(data_size, size), "%s", data);

  // Run command
  uint64_t old_lba = lba;
  if ((szd_rc = szd_append(*qpair, (uint64_t *)&lba, data_spdk, size))) {
    fprintf(stderr, "Append request: Error appending %d\n", szd_rc);
    szd_free(data_spdk);
    goto destroy_context;
    return ERROR_STATE;
  }
  fprintf(stdout,
          "Append request: Append data at location %lu with size %lu to device "
          "%s\n",
          old_lba, size, cli_context->target_trid);
  szd_free(data_spdk);
  goto destroy_context;
  return 0;

// Prevents making mistakes with forgetting to dealloc
destroy_context:
  szd_destroy_qpair(*qpair);
  free(qpair);
  free(data);
  return 0;
}

int parse_probe_zns(int argc, char **argv, CliContext *cli_context) {
  (void)argc;
  (void)argv;
  ProbeInformation **prober =
      (ProbeInformation **)calloc(1, sizeof(ProbeInformation *));
  printf("Looking for devices:\n");
  int szd_rc;
  if ((szd_rc = szd_probe(*cli_context->dev_manager, prober))) {
    printf("Probe request: Fatal error during probing %d\n Are you sure you "
           "are running as "
           "root?\n",
           szd_rc);
    free(prober);
    return ERROR_STATE;
  }
  for (int i = 0; i < (*prober)->devices; i++) {
    const char *is_zns = (*prober)->zns[i] ? "true" : "false";
    printf("Device found\n\t-traddr:%s\n\t-ZNS?:%s\n", (*prober)->traddr[i],
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
  return 0;
}

int parse_info_zns(int argc, char **argv, CliContext *cli_context) {
  // Parse and setup args
  (void)argc;
  (void)argv;
  bool trid_set = false;
  int op = -1;
  while ((op = getopt(argc, argv, "t:")) != -1) {
    switch (op) {
    case 't':
      trid_set = true;
      snprintf(cli_context->target_trid, MAX_TRADDR_LENGTH, "%s", optarg);
      break;
    default:
      return ERROR_STATE;
    }
  }
  if (!trid_set) {
    fprintf(stderr, "Info request: Not all required arguments are set\n");
    return ERROR_STATE;
  }

  // Open device
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int szd_rc;
  if ((szd_rc = szd_open(*cli_context->dev_manager, cli_context->target_trid,
                         &ooptions))) {
    fprintf(stderr, "Info command: Invalid trid %s or not a ZNS device\n",
            cli_context->target_trid);
    return 1;
  }
  DeviceInfo info = (*cli_context->dev_manager)->info;

  // Run command (which is just printing)
  fprintf(stdout,
          "Getting information from device %s:\n"
          "\t*lba size (in bytes)    :%lu\n"
          "\t*zone capacity (in lbas):%lu\n"
          "\t*amount of zones        :%lu\n"
          "\t*total amount of lbas   :%lu\n"
          "\t*mdts (in bytes)        :%lu\n"
          "\t*zasl (in bytes)        :%lu\n",
          cli_context->target_trid, info.lba_size, info.zone_size,
          info.lba_cap / info.zone_size, info.lba_cap, info.mdts, info.zasl);

  return 0;
}

int parse_zones_zns(int argc, char **argv, CliContext *cli_context) {
  // Parse and setup args
  bool trid_set = false;
  int op = -1;
  while ((op = getopt(argc, argv, "t:")) != -1) {
    switch (op) {
    case 't':
      trid_set = true;
      snprintf(cli_context->target_trid, MAX_TRADDR_LENGTH, "%s", optarg);
      break;
    default:
      return ERROR_STATE;
    }
  }
  if (!trid_set) {
    fprintf(stderr, "Zones request: Not all required arguments are set\n");
    return ERROR_STATE;
  }

  // Open device
  DeviceOpenOptions ooptions = DeviceOpenOptions_default;
  int szd_rc;
  if ((szd_rc = szd_open(*cli_context->dev_manager, cli_context->target_trid,
                         &ooptions))) {
    fprintf(stderr, "Zones request: Invalid trid %s\n",
            cli_context->target_trid);
    return ERROR_STATE;
  }

  // Setup QPair
  QPair **qpair = (QPair **)calloc(1, sizeof(QPair *));
  if ((szd_rc = szd_create_qpair(*cli_context->dev_manager, qpair))) {
    fprintf(stderr, "Zones request: Error allocating with SPDKs malloc %d\n",
            szd_rc);
    free(qpair);
    return ERROR_STATE;
  }
  DeviceInfo info = (*cli_context->dev_manager)->info;

  // Run command
  printf("Info: Printing zone writeheads for device %s:\n",
         cli_context->target_trid);
  uint64_t zone_head;
  for (uint64_t i = 0; i < info.lba_cap; i += info.zone_size) {
    if ((szd_rc = szd_get_zone_head(*qpair, i, &zone_head))) {
      fprintf(stderr, "Zones request: Error during getting zonehead %d\n",
              szd_rc);
      szd_destroy_qpair(*qpair);
      free(qpair);
      return ERROR_STATE;
    }
    printf("\tslba:%6lu - wp:%6lu - %lu/%lu\n", i, zone_head, zone_head - i,
           info.zone_size);
  }

  szd_destroy_qpair(*qpair);
  free(qpair);
  return 0;
}

int parse_help_zns(int argc, char **argv, CliContext *cli_context) {
  (void)argc;
  (void)argv;
  (void)cli_context;
  return print_help_util();
}

CliContext new_cli_context(void) {
  CliContext context = {
      .dev_manager = (DeviceManager **)calloc(1, sizeof(DeviceManager)),
      .dev_options = {.name = "znscli", .setup_spdk = true},
      .target_trid = (char *)calloc(MAX_TRADDR_LENGTH + 1, sizeof(char *))};
  return context;
}

void free_cli_context(CliContext *context) {
  if (context == NULL) {
    return;
  }
  szd_destroy(*context->dev_manager);
  free(context->dev_manager);
  free(context->target_trid);
}

int parse_args_zns(int argc, char **argv, CliContext *cli_context) {
  if (!argc) {
    return 1;
  }
// checks if first word matches a command, if it is try to parse the rest and
// return the rc.
#define TRY_PARSE_ARGS_ZNS_COMMAND(FUNCTIONNAME, argc, argv, cli_context)      \
  do {                                                                         \
    if (strncmp((argv)[0], #FUNCTIONNAME,                                      \
                min_uint64_t(strlen(argv[0]), strlen(#FUNCTIONNAME))) == 0) {  \
      return parse_##FUNCTIONNAME##_zns(argc, argv, cli_context);              \
    }                                                                          \
  } while (0)
  TRY_PARSE_ARGS_ZNS_COMMAND(reset, argc, argv, cli_context);
  TRY_PARSE_ARGS_ZNS_COMMAND(read, argc, argv, cli_context);
  TRY_PARSE_ARGS_ZNS_COMMAND(append, argc, argv, cli_context);
  TRY_PARSE_ARGS_ZNS_COMMAND(info, argc, argv, cli_context);
  TRY_PARSE_ARGS_ZNS_COMMAND(probe, argc, argv, cli_context);
  TRY_PARSE_ARGS_ZNS_COMMAND(zones, argc, argv, cli_context);
  TRY_PARSE_ARGS_ZNS_COMMAND(help, argc, argv, cli_context);
  fprintf(stderr, "Error: Command not recognised\n");
  return print_help_util();
}

int main(int argc, char **argv) {
  print_disclaimer();
  if (argc < 2) {
    fprintf(stderr, "Not enough args provided\n");
    print_help_util();
    return ERROR_STATE;
  }

  // Setup SZD context
  CliContext context = new_cli_context();
  int szd_rc;
  if ((szd_rc = szd_init(context.dev_manager, &context.dev_options))) {
    fprintf(stderr, "Failed to create context. Are you running as root?\n");
    free_cli_context(&context);
    return ERROR_STATE;
  }

  // Run command
  int szd_cli_rc;
  if ((szd_cli_rc = parse_args_zns(argc - 1, &argv[1], &context))) {
    printf("Error: Command failed\n");
  }
  free_cli_context(&context);
  return szd_cli_rc;
}

#ifdef __cplusplus
}
#endif
