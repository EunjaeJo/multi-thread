#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>

#pragma pack(1)
struct myheader_hdr {
    uint32_t op;
    uint32_t key;
    uint64_t value;
    uint64_t txTime; // 전송 시간
    uint64_t latency;
    uint64_t seqNum;
} __attribute__((packed));

// 스레드 인자 구조체
struct ThreadArgs {
    int sock;
    struct sockaddr_in srv_addr, cli_addr;
    int cli_addr_len;
};

uint64_t get_cur_ns();
uint64_t get_cur_s();
void* txThread(void*);
void* rxThread(void*);
uint64_t cal_99th(uint64_t*, int);
uint64_t cal_median(uint64_t*, int);
int compare(const void*, const void*);
void recordLatency(double, double);

#define FILENAME "latency.txt"

// Global Variables - parameters
int TARGET_QPS;     // Target Tx ratio
int TOTAL_SECONDS;
int WRITE_RATIO;    // Write ratio
int totalReqs; // 전체 요청 수

int main(int argc, char* argv[]) {
    if (argc < 4) {
        printf("Usage: %s [Starting Tx ratio] [Total time] [Write ratio]\n", argv[0]);
        return 1;
    }

    int starting_QPS = atoi(argv[1]);
    TOTAL_SECONDS = atoi(argv[2]);
    WRITE_RATIO = atoi(argv[3]);
    totalReqs = starting_QPS * TOTAL_SECONDS;

    int SERVER_PORT = 5001;
    const char* server_name = "130.127.134.79";
    // const char* server_name = "localhost";
    struct sockaddr_in srv_addr; // Create socket structure
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, server_name, &srv_addr.sin_addr);

    int sock;
    if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        printf("Could not create socket\n");
        exit(1);
    }

    // 서버 연결 확인
    int connect_status = connect(sock, (struct sockaddr*)&srv_addr, sizeof(srv_addr));
    if (connect_status < 0) {
        printf("Failed to connect to the server\n");
        exit(1);
    }

    struct sockaddr_in cli_addr;
    int cli_addr_len = sizeof(cli_addr);

    pthread_t tx_tid, rx_tid;

    // 각 스레드 실행에 필요한 인자를 구조체로 전달
    struct ThreadArgs tx_args = { .sock = sock, .srv_addr = srv_addr };
    struct ThreadArgs rx_args = { .sock = sock, .srv_addr = srv_addr, .cli_addr = cli_addr, .cli_addr_len = cli_addr_len };

    
    // TARGET_QPS를 증가시키면서 재실행
    for (int qps = starting_QPS; qps <= 10000000; qps += 50) {
        close(sock);
        if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
            printf("Could not create socket\n");
            exit(1);
        }

        TARGET_QPS = qps;
        totalReqs = TARGET_QPS * TOTAL_SECONDS;

        // Tx, Rx 스레드 생성 및 실행
        for (int i = 0; i < TOTAL_SECONDS; i++)
        {
            pthread_create(&tx_tid, NULL, txThread, (void*)&tx_args);
        }
        pthread_create(&rx_tid, NULL, rxThread, (void*)&rx_args);

        printf("Tx QPS: %d\n", TARGET_QPS);

        pthread_join(tx_tid, NULL);
        pthread_join(rx_tid, NULL);
    }

    close(sock);
    return 0;
}


void* txThread(void* arg) {
    // 함수의 인자를 ThreadArgs 구조체로 캐스팅
    struct ThreadArgs* args = (struct ThreadArgs*)arg;
    int sock = args->sock;
    struct sockaddr_in srv_addr = args->srv_addr;

    // GSL 난수 생성기 및 exponential 분포 설정
    const gsl_rng_type* T;
    gsl_rng* r;
    gsl_rng_env_setup();
    T = gsl_rng_default;
    r = gsl_rng_alloc(T);

    double lambda = TARGET_QPS * 1e-9;  // TARGET_QPS가 인자로 받은 target tx rate임
    double mu = 1.0 / lambda;
    uint64_t temp_time = get_cur_ns();

    // 요청 전송
    for (int i = 1; i <= TARGET_QPS; i++) {
        // Packet inter-arrival time을 exponential distribution으로 생성
        uint64_t inter_arrival_time = (uint64_t)(gsl_ran_exponential(r, mu));
        temp_time += inter_arrival_time;

        // Inter-inter_arrival_time만큼 대기
        while (get_cur_ns() < temp_time)
            ;

        // 요청 생성 및 전송
        struct myheader_hdr SendBuffer;
        memset(&SendBuffer, 0, sizeof(SendBuffer));
        SendBuffer.op = (rand() % 100 < WRITE_RATIO) ? 1 : 0;
        SendBuffer.key = rand() % 1000000;
        SendBuffer.value = 1111;
        SendBuffer.txTime = get_cur_ns(); // 전송 시간 설정
        SendBuffer.latency = 0;
        SendBuffer.seqNum = i;

        sendto(sock, &SendBuffer, sizeof(SendBuffer), 0, (struct sockaddr*)&srv_addr, sizeof(srv_addr));
    }

    return NULL;
}

void* rxThread(void* arg) {
    struct ThreadArgs* args = (struct ThreadArgs*)arg;
    int sock = args->sock;
    struct sockaddr_in srv_addr = args->srv_addr;
    struct sockaddr_in cli_addr = args->cli_addr;
    int cli_addr_len = args->cli_addr_len;

    struct myheader_hdr RecvBuffer;
    uint64_t latencies[totalReqs]; // 각 latency 저장 배열
    memset(latencies, 0, sizeof(uint64_t) * totalReqs); // 배열 초기화

    uint64_t sum = 0;
    uint64_t median = 0;
    uint64_t percentile_99 = 0;

    int rxReqs = 0; // 수신된 요청 수

    uint64_t startTime = get_cur_ns(); // 스레드 시작 시간
    uint64_t elapsedTime = 0;  // 스레드 경과 시간

    while (1) {
        ssize_t rx = recvfrom(sock, &RecvBuffer, sizeof(RecvBuffer), MSG_DONTWAIT, (struct sockaddr*)&cli_addr, &cli_addr_len);

        // 패킷 수신 이후 3초 동안 수신받지 못하면 종료
        if (rx < 0) {
            elapsedTime = get_cur_ns() - startTime;
            if (elapsedTime / 1000000000 >= 3) { 
                printf("Unable to receive packet\n");
                break;
            }
            continue;
        }

        // 패킷을 수신한 경우
        startTime = get_cur_ns(); // startTime 초기화
        RecvBuffer.latency = get_cur_ns() - RecvBuffer.txTime;

        latencies[rxReqs] = RecvBuffer.latency;
        sum += RecvBuffer.latency;

        /* for debugging
        printf("Rx seq_num: %ld\n", RecvBuffer.seqNum);
        */
        rxReqs++;
    }

    // 수신된 요청 수
    printf("receivedReqs: %d\n", rxReqs);

    // 총 요청 수만큼 수신해야 latency 기록
    if (rxReqs == totalReqs) {
        // median latency
        median = (double)cal_median(latencies, rxReqs);
        printf("Median latency: %.2lf ns\n", (double)median);

        // 99th percentile latency
        percentile_99 = (double)cal_99th(latencies, rxReqs);
        printf("99th percentile latency: %.2lf ns\n", (double)percentile_99);

        recordLatency(median, percentile_99);
    }

    return NULL;
}

/* Get current time in nanosecond-scale */
uint64_t get_cur_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t t = ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec;
    return t;
}

uint64_t cal_99th(uint64_t* latencies, int numOfReq) {
    qsort(latencies, numOfReq, sizeof(uint64_t), compare);

    int index_99 = (int)(0.99 * numOfReq) - 1;
    return latencies[index_99];
}

uint64_t cal_median(uint64_t* latencies, int numOfReq) {
    qsort(latencies, numOfReq, sizeof(uint64_t), compare);

    if (numOfReq % 2 != 0) {
        return latencies[numOfReq / 2];
    }
    else {
        uint64_t median1 = latencies[numOfReq / 2 - 1];
        uint64_t median2 = latencies[numOfReq / 2];
        return (median1 + median2) / 2;
    }
}

int compare(const void* a, const void* b) {
    return (*(uint64_t*)a - *(uint64_t*)b);
}

void recordLatency(double median, double tailLatency) {
    FILE* file = fopen(FILENAME, "a");
    if (file == NULL) {
        printf("Error opening file.\n");
        return;
    }

    fprintf(file, "%d    %.2lf    %.2lf\n", TARGET_QPS, median, tailLatency);

    fclose(file);
}
