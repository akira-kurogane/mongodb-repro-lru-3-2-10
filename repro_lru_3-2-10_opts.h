int help_flag;
char* conn_uri_str;
char* database_name;
char* collection_name;
int query_thread_num;
double run_interval;

void init_options();
void free_options();
int  parse_cmd_options(int argc, char **argv, int* err_flag);
void dump_cmd_options();
void print_options_help();

