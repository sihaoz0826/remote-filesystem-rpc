all: mylib.so server

client.o: client.c client.h
	gcc -Wall -fPIC -DPIC -I../include -c client.c

mylib.o: mylib.c client.h
	gcc -Wall -fPIC -DPIC -I../include -c mylib.c

mylib.so: mylib.o client.o
	gcc -shared -nostartfiles -L../lib -o mylib.so mylib.o client.o -ldl -ldirtree

server: server.c
	gcc -Wall -L../lib -Wl,-rpath,'$$ORIGIN/../lib' -o server server.c -ldirtree

clean:
	rm -f *.o *.so server