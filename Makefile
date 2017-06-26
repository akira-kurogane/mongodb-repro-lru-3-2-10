#libmongoc_flags=$(pkg-config --cflags --libs libmongoc-1.0)
libmongoc_flags=-I/usr/include/libmongoc-1.0 -I/usr/include/libbson-1.0 -lmongoc-1.0 -lsasl2 -lssl -lcrypto -lrt -lbson-1.0

range_random_query:	range_random_query.c range_random_query_opts.c
	gcc -c -g range_random_query_opts.c
	gcc -c -g range_random_query.c ${libmongoc_flags} -lpthread
	gcc -g -o range_random_query range_random_query.o range_random_query_opts.o ${libmongoc_flags} -lpthread

clean:
	rm -f range_random_query *.o
