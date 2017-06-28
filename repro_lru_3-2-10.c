#include <mongoc.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/time.h>
#include <pthread.h>
#include "repro_lru_3-2-10_opts.h"

static volatile sig_atomic_t got_exit_alarm = 0;

void print_usage(FILE* fstr) {
  fprintf(fstr, "Usage: id_query_loop_test [options] <file of ids to query>\n");
}

void print_desc() {
  printf("In a loop will query db.collection.find({ _id: <decimal_id_value>}) \n\
  and output the id, the time, the time elapsed in microsecs, and the matching \n\
  document's byte size.\n\
  The final line of output will be slowest times by several percentiles.\n");
}

static void sigalrmHandler(int sig) {
   got_exit_alarm = 1;
}

void set_process_exit_timer(double timer_secs) {
   struct itimerval new, old;
   struct sigaction sa;

   if (timer_secs <= 0) {
      fprintf(stderr, "set_process_exit_timer() argument must be > 0. Aborting.\n");
      exit(EXIT_FAILURE);
   }

   sigemptyset(&sa.sa_mask);
   sa.sa_flags = 0;
   sa.sa_handler = sigalrmHandler;
   if (sigaction(SIGALRM, &sa, NULL) == -1) {
      fprintf(stderr, "set_process_exit_timer() has failed to set handler for SIGALRM. Aborting.\n");
      exit(EXIT_FAILURE);
   }
   
   new.it_interval.tv_sec = 0;
   new.it_interval.tv_usec = 0;
   new.it_value.tv_sec = (int)timer_secs;
   new.it_value.tv_usec = (int)((timer_secs - new.it_value.tv_sec) * 1000000.0f);
   if (setitimer(ITIMER_REAL, &new, &old) == -1) {
      fprintf(stderr, "set_process_exit_timer() has failed to set interval timer. Aborting.\n");
      exit(EXIT_FAILURE);
   }
}

struct query_loop_thread_retval {
   long sum_rtt_ms;
   size_t count;
};

size_t
test_collection_doc_count(mongoc_collection_t *collection) {
   bson_error_t error;
   int64_t count;
   char *str;
   bool ret;

   count = mongoc_collection_count(collection, MONGOC_QUERY_NONE, NULL, 0, 0, NULL, &error);

   return (size_t)count;
}

size_t
insert_test_collection(mongoc_collection_t *collection, size_t target_ins_count, size_t doc_size) {
   mongoc_bulk_operation_t *bulk;
   bson_error_t error;
   bson_t *doc;
   bson_t reply;
   bool ret;
   uint32_t i;
   uint32_t max_i = 0;
   uint32_t inserted_count = 0;
   bson_iter_t bitr;

   //Create the padding data
   size_t dummy_str_len = doc_size - (3/*"_id"*/ + 4/*_id int32 val*/ + 9/*dummy_val"*/ + 14/*bson hdr*/);
   char *dummy_str = malloc(dummy_str_len + 1);
   char *s = dummy_str;
   while (s - dummy_str < dummy_str_len) {
     *s++ = 'a';
   }

   while (max_i < target_ins_count) {
      bulk = mongoc_collection_create_bulk_operation(collection, true, NULL);
      for (i = max_i; i < max_i + 10000 && i < target_ins_count; i++) {
         doc = BCON_NEW("_id", BCON_INT32(i), "dummy_val", dummy_str);
         mongoc_bulk_operation_insert(bulk, doc);
         bson_destroy(doc);
      }
      max_i = i;
      ret = mongoc_bulk_operation_execute(bulk, &reply, &error);
      if (bson_iter_init_find(&bitr, &reply, "nInserted") && BSON_ITER_HOLDS_INT32(&bitr)) {
         inserted_count += bson_iter_int32(&bitr);
      }
      mongoc_bulk_operation_destroy(bulk);
   }
   bson_destroy(&reply);

   return inserted_count;
}

static void*
run_query_loop(void *args) {
   mongoc_client_pool_t *pool = args;
   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_error_t error;
   const bson_t *doc;
   bson_t query;
   bson_t find_opts, proj_fields;
   size_t max_id = 40000;
   size_t min_id = 0;
   size_t range_sz = max_id + 1 - min_id;
   struct query_loop_thread_retval* ret_p = malloc(sizeof(struct query_loop_thread_retval));
   ret_p->sum_rtt_ms = 0;
   ret_p->count = 0;

   client = mongoc_client_pool_pop(pool);
   collection = mongoc_client_get_collection(client, database_name, collection_name);

//long curr_id = min_id;
   while (!got_exit_alarm) {

      long curr_id = (rand() % range_sz) + min_id;
// curr_id += 1;
// if (curr_id >= max_id) {
//    curr_id = min_id;
// }
      bson_init (&query);
      bson_append_int64(&query, "_id", -1, curr_id);

      bson_init(&find_opts);
      bson_init(&proj_fields);
      bson_append_document_begin (&find_opts, "projection", -1, &proj_fields);
      bson_append_bool(&proj_fields, "long_string", -1, false);
      bson_append_document_end (&find_opts, &proj_fields);
   
         struct timeval start_tp;
         gettimeofday(&start_tp, NULL);

      cursor = mongoc_collection_find_with_opts (
         collection,
         &query,
         &find_opts, 
         NULL); /* read prefs, NULL for default */

      bool cursor_next_ret = mongoc_cursor_next (cursor, &doc);
      struct timeval end_tp;
      gettimeofday(&end_tp, NULL);
      ret_p->sum_rtt_ms += ((end_tp.tv_sec - start_tp.tv_sec) * 1000) + ((end_tp.tv_usec - start_tp.tv_usec) / 1000);
      if (!cursor_next_ret) {
         fprintf (stderr, "No document for { \"_id\": %ld } was found\n", curr_id);
      }

      if (mongoc_cursor_error (cursor, &error)) {
         fprintf (stderr, "Cursor Failure: %s\n", error.message);
         exit(EXIT_FAILURE);
      }

      bson_destroy(&query);
      bson_destroy(&proj_fields);
      bson_destroy(&find_opts);
      mongoc_cursor_destroy(cursor);

      ++ret_p->count;
   } //end while(true)

   mongoc_collection_destroy(collection);
   mongoc_client_pool_push(pool, client);

   return (void*)ret_p;
}

int
main (int argc, char *argv[])
{
   int opt_err_flag = 0;
   int nonopt_arg_idx = parse_cmd_options(argc, argv, &opt_err_flag);

   if (help_flag) {
      print_usage(stdout);
      printf("\n");
      print_desc();
      printf("\n");
      print_options_help();
      free_options();
      exit(EXIT_SUCCESS);
   } else if (opt_err_flag) {
      print_usage(stderr);
      exit(EXIT_FAILURE);
   }
//dump_cmd_options();
//exit(EXIT_SUCCESS);

   if (!conn_uri_str || !database_name || !collection_name) {
      fprintf(stderr, "Aborting. One or more of the neccesary --conn-uri, --database and --collection arguments was absent.\n");
      fprintf(stderr, "Try --help for options description\n");
      print_usage(stderr);
      exit(EXIT_FAILURE);
   }
   
   if (run_interval > 0) {
      set_process_exit_timer(run_interval);
   }

   mongoc_init();

   mongoc_uri_t* conn_uri;
   conn_uri = mongoc_uri_new(conn_uri_str);
   if (!conn_uri) {
      fprintf (stderr, "Failed to parse URI, or otherwise establish mongo connection.\n");
      return EXIT_FAILURE;
   }

   mongoc_client_pool_t* pool;
   pool = mongoc_client_pool_new(conn_uri);

   mongoc_client_pool_set_error_api(pool, 2);

   mongoc_client_t *client; //get one client conn from pool for these initial checks/setup
   client = mongoc_client_pool_pop(pool);
   bson_t ss_reply;
   size_t svr_cache_size = 0;
   if (mongoc_client_get_server_status(client, 0, &ss_reply, NULL)) {
      bson_iter_t bitr;
      bson_iter_t x;
      if (bson_iter_init(&bitr, &ss_reply) &&
          bson_iter_find_descendant(&bitr, "wiredTiger.cache.maximum bytes configured", &x) &&
          BSON_ITER_HOLDS_DOUBLE(&x)) {
         svr_cache_size = (size_t)bson_iter_double(&x);
         fprintf(stdout, "Connected to %s. WT configured cache size is %zu\n", mongoc_uri_get_string(conn_uri), svr_cache_size);
      } else {
         fprintf(stderr, "Connected to %s, but no \"wiredTiger.cache.maximum bytes configured\" found in serverStatus result. Aborting.\n", mongoc_uri_get_string(conn_uri));
         exit(EXIT_FAILURE);
      }
   } else {
      fprintf(stderr, "Failed to connect to %s.\n", mongoc_uri_get_string(conn_uri));
      exit(EXIT_FAILURE);
   }
   bson_destroy(&ss_reply);
   
   mongoc_collection_t *collection;
   collection = mongoc_client_get_collection(client, database_name, collection_name);

   size_t test_coll_avg_doc_size = 1023;
   size_t test_coll_doc_count = svr_cache_size / test_coll_avg_doc_size;
   test_coll_doc_count = test_coll_doc_count + (test_coll_doc_count / 5); //make it an extra 20% larger than cache
   size_t existing_count = test_collection_doc_count(collection);
   if (existing_count >= test_coll_doc_count) {
      fprintf(stdout, "Found %d documents already in %s.%s\n", test_coll_doc_count, database_name, collection_name);
   } else {
      if (existing_count != 0) {
          fprintf(stderr, "There is already a %s.%s collection. It has %d documents rather "
                  "than the desired %d. Aborting rather than overwriting it.\n", 
                  database_name, collection_name, existing_count, test_coll_doc_count);
          exit(EXIT_FAILURE);
      }
      fprintf(stdout, "Inserting %d documents into %s.%s\n", test_coll_doc_count, database_name, collection_name);
      insert_test_collection(collection, test_coll_doc_count, test_coll_avg_doc_size);
   }

   mongoc_collection_destroy(collection);
   mongoc_client_pool_push(pool, client);

   pthread_t* pthread_ptrs = calloc(query_thread_num, sizeof(pthread_t));
   int j;
   for (j = 0; j < query_thread_num; ++j) {
      pthread_create(pthread_ptrs + j, NULL, run_query_loop, pool);
   }
   size_t total_query_count = 0;
   long sum_rtt_ms = 0;
   void* thr_res;
   for (j = 0; j < query_thread_num; ++j) {
      pthread_join(*(pthread_ptrs + j), &thr_res);
      sum_rtt_ms += ((struct query_loop_thread_retval*)thr_res)->sum_rtt_ms;
      total_query_count += ((struct query_loop_thread_retval*)thr_res)->count;
      free(thr_res);
   }

   mongoc_client_pool_destroy(pool);

   mongoc_cleanup ();

   fprintf(stdout, "Count = %lu, Median = %fms\n", total_query_count, (double)sum_rtt_ms / total_query_count);

   free_options();
   return EXIT_SUCCESS;
}
