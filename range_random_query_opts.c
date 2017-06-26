#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include "range_random_query_opts.h"

void init_options() {
  conn_uri_str = malloc(1);
  conn_uri_str = NULL;
  database_name = malloc(1);
  database_name = NULL;
  collection_name = malloc(1);
  collection_name = NULL;
  fieldname = malloc(4);
  strcpy(fieldname, "_id");
  query_thread_num = 1;
  run_interval = -1.0f;
  min_id = 0;
  max_id = 0;
}

void free_options() {
  free(conn_uri_str);
  free(database_name);
  free(collection_name);
  free(fieldname);
}

int parse_cmd_options(int argc, char **argv, int* err_flag) {

  int c;
  char* tmp_str;
  char* p;
  int tnp_len;
  int i;

  init_options();

  while (1) {
    static struct option long_options[] = {
      {"help",        no_argument, &help_flag,    1},
      {"conn-uri",    required_argument, 0, 'm'},
      {"database",    required_argument, 0, 'd'},
      {"collection",  required_argument, 0, 'c'},
      {"fieldname",   required_argument, 0, 'f'},
      {"threads",     required_argument, 0, 't'},
      {"interval",    required_argument, 0, 'i'},
      {"min-id",      required_argument, 0, 'x'},
      {"max-id",      required_argument, 0, 'y'},
      {0, 0, 0, 0}
    };
    /* getopt_long stores the option index here. */
    int option_index = 0;

    c = getopt_long(argc, argv, "m:d:c:f:t:i:x:y:", long_options, &option_index);

    /* Detect the end of the options. */
    if (c == -1)
      break;

    switch (c) {
      case 0:
        /* If this option set a flag, do nothing else now. */
        if (long_options[option_index].flag != 0)
          break;

        fprintf(stderr, "Unexpected option %s", long_options[option_index].name);
        if (optarg) {
          fprintf(stderr, " with arg %s", optarg);
        }
        fprintf(stderr, "\n");
        *err_flag = 1;
        break;

      case 'm':
        conn_uri_str = realloc(conn_uri_str, strlen(optarg) + 1);
        strcpy(conn_uri_str, optarg);
        //sanity enforcement
        if (conn_uri_str && strlen(conn_uri_str) == 0) {
          free(conn_uri_str);
          conn_uri_str = NULL;
        }
        break;

      case 'd':
        database_name = realloc(database_name, strlen(optarg) + 1);
        strcpy(database_name, optarg);
        //sanity enforcement
        if (database_name && strlen(database_name) == 0) {
          free(database_name);
          database_name = NULL;
        }
        break;

      case 'c':
        collection_name = realloc(collection_name, strlen(optarg) + 1);
        strcpy(collection_name, optarg);
        //sanity enforcement
        if (collection_name && strlen(collection_name) == 0) {
          free(collection_name);
          collection_name = NULL;
        }
        break;

      case 'f':
        fieldname = realloc(fieldname, strlen(optarg) + 1);
        strcpy(fieldname, optarg);
        //sanity enforcement
        if (fieldname && strlen(fieldname) == 0) {
          free(fieldname);
          fieldname = NULL;
        }
        break;

      case 't':
        query_thread_num = atoi(optarg);
        //Todo: confirm > 0 and <= 1024
        break;

      case 'i':
        run_interval = atof(optarg);
        break;

      case 'x':
        min_id = atol(optarg);
        break;

      case 'y':
        max_id = atol(optarg);
        break;

      case '?':
        /* getopt_long already printed an error message. */
        *err_flag = 1;
        break;

      default:
        *err_flag = 1;
    }
  }

  return optind; //return idx to non-option argv argument (would have been 
    //moved to end by getopt() functions if it wasn't already).
}

void dump_cmd_options() {
  char* p;
  size_t i;

  printf("help         = %s\n", help_flag ? "true" : "false");
  printf("conn-uri     = \"%s\"\n", conn_uri_str);
  printf("database     = \"%s\"\n", database_name);
  printf("collection   = \"%s\"\n", collection_name);
  printf("id-fieldname = \"%s\"\n", fieldname);
  printf("threads      = %d\n", query_thread_num);
  printf("interval     = %f\n", run_interval);
  printf("min-id       = %d\n", min_id);
  printf("max-id       = %d\n", min_id);
}

void print_options_help() {
  printf("Options:\n\
  --help\n\
    This message\n\
  -m, --conn-uri\n\
    The connection string in mongodb URI format. Use to specify host, port, \n\
    username, password, authentication db, replset name (if using one), \n\
    read preference, and more advanced options such as ssl, timeouts, \n\
    localThresholdMS, etc.\n\
    See https://docs.mongodb.com/manual/reference/connection-string/\n\
  -d, --database \n\
    Name of the database to query on.\n\
  -c, --collection \n\
    Name of the collection to query on.\n\
  -f, --fieldname \n\
    Name of the collection to query on.\n\
  -t, --threads\n\
    Number of parallel threads to execute the queries. Optional.\n\
  -i, --interval\n\
    Time in seconds to exit after. Optional.\n\
  -x, --min-id\n\
  -y, --max-id\n\
    Min and max number that the random query argument will be chosen betweeen.\n\
");
}
