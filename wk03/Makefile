all: client server

client : client.o
	$(CC) -o client.out client.o -lpthread -lm -D_GNU_SOURCE -lgsl -lgslcblas

client.o : client.c
	$(CC) -c client.c

server.o: server.c
	$(CC) -c server.c

server: server.o
	$(CC) -o server server.o -D_GNU_SOURCE -lhiredis

clean:
	rm *.o client.out server latency.txt
