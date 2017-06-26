#libmongoc_flags=$(pkg-config --cflags --libs libmongoc-1.0)
libmongoc_flags=-I/usr/include/libmongoc-1.0 -I/usr/include/libbson-1.0 -lmongoc-1.0 -lsasl2 -lssl -lcrypto -lrt -lbson-1.0

repro_lru_3-2-10:	repro_lru_3-2-10.c repro_lru_3-2-10_opts.c
	gcc -c -g repro_lru_3-2-10_opts.c
	gcc -c -g repro_lru_3-2-10.c ${libmongoc_flags} -lpthread
	gcc -g -o repro_lru_3-2-10 repro_lru_3-2-10.o repro_lru_3-2-10_opts.o ${libmongoc_flags} -lpthread

clean:
	rm -f repro_lru_3-2-10 *.o
