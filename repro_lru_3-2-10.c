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

void clear_exit_timer() {
   got_exit_alarm = 0;
}

struct query_loop_thread_retval {
   long sum_rtt_ms;
   size_t count;
};

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

void
prepare_test_collection(mongoc_client_t* client, const char* database_name, const char* collection_name, const char* conn_uri_str, size_t *cache_fill_max_id) {

   bson_t ss_reply;
   size_t svr_cache_size = 0;
   if (mongoc_client_get_server_status(client, 0, &ss_reply, NULL)) {
      bson_iter_t bitr;
      bson_iter_t x;
      if (bson_iter_init(&bitr, &ss_reply) &&
          bson_iter_find_descendant(&bitr, "wiredTiger.cache.maximum bytes configured", &x) &&
          BSON_ITER_HOLDS_DOUBLE(&x)) {
         svr_cache_size = (size_t)bson_iter_double(&x);
         fprintf(stdout, "Connected to %s. WT configured cache size is %zu\n", conn_uri_str, svr_cache_size);
      } else {
         fprintf(stderr, "Connected to %s, but no \"wiredTiger.cache.maximum bytes configured\" found in serverStatus result. Aborting.\n", conn_uri_str);
         exit(EXIT_FAILURE);
      }
   } else {
      fprintf(stderr, "Failed to connect to %s.\n", conn_uri_str);
      exit(EXIT_FAILURE);
   }
   bson_destroy(&ss_reply);
   mongoc_collection_t *collection;
   collection = mongoc_client_get_collection(client, database_name, collection_name);

   size_t test_coll_avg_doc_size = 1023;

   bson_t coll_stats_doc;
   mongoc_collection_stats(collection, NULL, &coll_stats_doc, NULL);
//size_t junk_len;
//fprintf(stdout, bson_as_json(&coll_stats_doc, &junk_len));
   bson_iter_t bitr;
   size_t existing_count = 0;
   size_t existing_size_bytes = 0;
   size_t existing_avg_obj_size = 0;
   if (bson_iter_init_find(&bitr, &coll_stats_doc, "count") && BSON_ITER_HOLDS_INT32(&bitr)) {
      existing_count = bson_iter_int32(&bitr);
   }
   if (bson_iter_init_find(&bitr, &coll_stats_doc, "size")) {
      if (BSON_ITER_HOLDS_INT32(&bitr)) {
         existing_size_bytes = bson_iter_int32(&bitr);
      } else if (BSON_ITER_HOLDS_INT64(&bitr)) {
         existing_size_bytes = bson_iter_int64(&bitr);
      } else if (BSON_ITER_HOLDS_DOUBLE(&bitr)) {
         existing_size_bytes = (size_t)bson_iter_double(&bitr);
      }
   }
   if (bson_iter_init_find(&bitr, &coll_stats_doc, "avgObjSize") && BSON_ITER_HOLDS_INT32(&bitr)) {
      existing_avg_obj_size = bson_iter_int32(&bitr);
   }
   assert(existing_count == 0 || (existing_size_bytes > 0 && existing_avg_obj_size > 0));

   if (existing_size_bytes > svr_cache_size) {
      fprintf(stdout, "Found %d documents in existing %dMB collection %s.%s.\n",
              existing_count, existing_size_bytes >> 20, database_name, collection_name);
   } else {
      if (existing_count != 0) {
          fprintf(stderr, "There is already a %s.%s collection. It is %dMB in data size rather "
                  "than the desired %dMB needed to fill the cache. Aborting rather than overwriting it.\n", 
                  database_name, collection_name, existing_size_bytes, svr_cache_size >> 20);
          exit(EXIT_FAILURE);
      }
      size_t ins_doc_count = svr_cache_size / test_coll_avg_doc_size;
      ins_doc_count = ins_doc_count + (ins_doc_count / 10); //make it an extra 10% larger than cache
      fprintf(stdout, "Inserting %d documents into %s.%s to make it a bit larger than the %dMB cache size.\n",
              ins_doc_count, database_name, collection_name, svr_cache_size >> 20);
      insert_test_collection(collection, ins_doc_count, test_coll_avg_doc_size);
      existing_count = ins_doc_count;
      existing_size_bytes = ins_doc_count * test_coll_avg_doc_size;
      existing_avg_obj_size = test_coll_avg_doc_size;
   }
   bson_destroy(&coll_stats_doc);

   *cache_fill_max_id = svr_cache_size / existing_avg_obj_size;
   //*cache_fill_max_id = *cache_fill_max_id + (*cache_fill_max_id / 10); //make the test query range 10% larger than cache

   mongoc_collection_destroy(collection);
}

struct query_loop_args {
  mongoc_client_pool_t *pool;
  size_t min_id;
  size_t max_id;
};

static void*
run_query_loop(void *args) {
   mongoc_client_pool_t *pool = ((struct query_loop_args*)args)->pool;
   size_t max_id = ((struct query_loop_args*)args)->max_id;
   size_t min_id = ((struct query_loop_args*)args)->min_id;

   mongoc_client_t *client;
   mongoc_collection_t *collection;
   mongoc_cursor_t *cursor;
   bson_error_t error;
   const bson_t *doc;
   bson_t query;
   bson_t find_opts, proj_fields;
   size_t range_sz = max_id + 1 - min_id;
   struct query_loop_thread_retval* ret_p = malloc(sizeof(struct query_loop_thread_retval));
   ret_p->sum_rtt_ms = 0;
   ret_p->count = 0;

   client = mongoc_client_pool_pop(pool);
   collection = mongoc_client_get_collection(client, database_name, collection_name);

   while (!got_exit_alarm) {

      long curr_id = (rand() % range_sz) + min_id;
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

void
run_query_load(mongoc_client_pool_t* pool, double exec_interval, int64_t min_id, int64_t max_id) {
   if (exec_interval > 0) {
      set_process_exit_timer(exec_interval);
   }

   //init args for the worker threads
   struct query_loop_args a;
   a.pool = pool;
   a.min_id = min_id;
   a.max_id = max_id;

   pthread_t* pthread_ptrs = calloc(query_thread_num, sizeof(pthread_t));
   int j;
   for (j = 0; j < query_thread_num; ++j) {
      pthread_create(pthread_ptrs + j, NULL, run_query_loop, &a);
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

   fprintf(stdout, "Count = %lu, Median = %fms\n", total_query_count, (double)sum_rtt_ms / total_query_count);
   
   clear_exit_timer();
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
   
   mongoc_init();

   mongoc_uri_t* conn_uri;
   conn_uri = mongoc_uri_new(conn_uri_str);
   if (!conn_uri) {
      fprintf (stderr, "Failed to parse connection URI \"%s\".\n", conn_uri_str);
      return EXIT_FAILURE;
   }

   mongoc_client_pool_t* pool;
   pool = mongoc_client_pool_new(conn_uri);

   mongoc_client_pool_set_error_api(pool, 2);

   mongoc_client_t *client; //get one client conn from pool for these initial checks/setup
   client = mongoc_client_pool_pop(pool);
   
   //This is also where connection will be first tested
   size_t cache_fill_max_id;
   prepare_test_collection(client, database_name, collection_name, mongoc_uri_get_string(conn_uri), &cache_fill_max_id);

   mongoc_client_pool_push(pool, client);

   assert(cache_fill_max_id > 0);
   run_query_load(pool, warmup_interval, 1, cache_fill_max_id);
   usleep((unsigned int)(cooldown_interval * 1000000));
   run_query_load(pool, run_interval, 1, cache_fill_max_id / 5); //query just 20% of the cache size

   mongoc_client_pool_destroy(pool);

   mongoc_cleanup ();

   //fprintf(stdout, "Count = %lu, Median = %fms\n", total_query_count, (double)sum_rtt_ms / total_query_count);

   free_options();
   return EXIT_SUCCESS;
}
