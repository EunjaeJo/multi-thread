#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <hiredis/hiredis.h>

#define NUM_KEYS 100000 // 요청 수

struct myheader_hdr {
    uint32_t op;
    uint32_t key;
    uint64_t value;
    uint64_t txTime; // 전송 시간
    uint64_t latency;
	uint64_t seqNum;
} __attribute__((packed));

int put(redisContext*, char*, char*);
char* get(redisContext*, char*);
void initData(redisContext*, int);
int processReq(redisContext*, struct myheader_hdr*, struct myheader_hdr*);

int main(int argc, char* argv[]) {

	int SERVER_PORT = atoi(argv[1]);

	struct sockaddr_in srv_addr;
	memset(&srv_addr, 0, sizeof(srv_addr));
	srv_addr.sin_family = AF_INET;
	srv_addr.sin_port = htons(SERVER_PORT);
	srv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

	int sock;
	if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
		printf("Could not create listen socket\n");
		exit(1);
	}

	if ((bind(sock, (struct sockaddr*)&srv_addr, sizeof(srv_addr))) < 0) {
		printf("Could not bind socket\n");
		exit(1);
	}

	// Connect to Redis server
	redisContext* redis_context = redisConnect("127.0.0.1", 6379);
	if (redis_context->err) {
		printf("Failed to connect to Redis: %s\n", redis_context->errstr);
		return 1;
	}

	if (argc < 2) {
		printf("Input : %s port number\n", argv[0]);
		return 1;
	}

	initData(redis_context); // 초기 데이터 등록

	struct sockaddr_in cli_addr;
	int cli_addr_len = sizeof(cli_addr);

	int n = 0;
	struct myheader_hdr RecvBuffer;
	struct myheader_hdr SendBuffer;

	while (1) {
		n = recvfrom(sock, &RecvBuffer, sizeof(RecvBuffer), 0, (struct sockaddr*)&cli_addr, &cli_addr_len);
		if (n > 0) {
			// 클라이언트 요청 처리
			if (processReq(redis_context, &RecvBuffer, &SendBuffer) == 0) {
				// 응답 전송
				sendto(sock, &SendBuffer, sizeof(SendBuffer), 0, (struct sockaddr*)&cli_addr, sizeof(cli_addr));
			}
		}
	}
	close(sock);

	redisFree(redis_context);

	return 0;
}

void initData(redisContext* c, int numOfKeys) {
	for (int i = 0; i < numOfKeys; i++) {
		char key[10]; // 정수 key값 문자열로 저장
		sprintf(key, "%d", i);
		put(c, key, "value");
	}
}

int put(redisContext* c, char* key, char* value) {
	// Redis 서버에 SET 전송
	redisReply* reply = redisCommand(c, "SET %s %s", key, value); 
	
	freeReplyObject(reply);

	return 0;
}


char* get(redisContext* c, char* key) {
	// Redis 서버에 GET 전송
	redisReply* reply = redisCommand(c, "GET %s", key);
	char* value;

	if (reply->str == NULL) { // 할당되지 않은 key 값일 경우
		 value = strdup("null");
	}
	else {
		 value = strdup(reply->str);
	}

	freeReplyObject(reply);

	return value;
}

int processReq(redisContext* c, struct myheader_hdr* RecvBuffer, struct myheader_hdr* SendBuffer) {
	if (RecvBuffer->op == 0) { // 읽기(get) 요청 처리
		char key_str[10];
		sprintf(key_str, "%d", RecvBuffer->key);
		char* value = get(c, key_str); // key에 대한 value 받아옴

		// 응답용 버퍼에 결과 저장
		SendBuffer->op = 0;
		SendBuffer->key = RecvBuffer->key;
		SendBuffer->value = strtoull(value, NULL, 10);
		SendBuffer->txTime = RecvBuffer->txTime;
		SendBuffer->latency = RecvBuffer->latency;
		SendBuffer->seqNum = RecvBuffer->seqNum;
		
		printf("seq_num: %ld\n", SendBuffer->seqNum);
		free(value);

		return 0;
	}
	else if (RecvBuffer->op == 1) { // 쓰기(put) 요청 처리
		char key_str[10], value_str[512];
		sprintf(key_str, "%d", RecvBuffer->key);
		sprintf(value_str, "%ld", RecvBuffer->value);

		// 쓰기 요청 Redis server에 저장
		put(c, key_str, value_str);

		// 응답용 버퍼에 성공 결과 저장
		SendBuffer->op = 1;
		SendBuffer->key = RecvBuffer->key;
		SendBuffer->value = RecvBuffer->value;
		SendBuffer->txTime = RecvBuffer->txTime;
		SendBuffer->latency = RecvBuffer->latency;
		SendBuffer->seqNum = RecvBuffer->seqNum;
		printf("seq_num: %ld\n", SendBuffer->seqNum);

		return 0;
	}
	else {
		// 예외 처리
		printf("Invalid operation\n");
		return -1;
	}
}
