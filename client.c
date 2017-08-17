/*
 * 
 * 
 */ 

#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

#define PORT 8360
#define RETURN_FAILURE 1

#define print_err(fmt, errno)   fprintf(stderr, fmt"%s\n", strerror(errno))

int sockfd;

struct work_data {
   int  id; 
   char *writebuf;
   char *readbuf;
   FILE *file;
};

void *do_work(void *arg) {
    // (void)arg;
    struct work_data *wd = (struct work_data *)arg;
    int nr, nw;
    size_t fw;
    while(1) {
        nw = write(sockfd, wd->writebuf, 8);
        if (nw < 0) {
            printf("id:%d", wd->id);
            print_err("client write error:", errno);
            break;
        }

        nr = read(sockfd, wd->readbuf, 256);
        if (nr < 0) {
            printf("id:%d", wd->id);
            print_err("client read error:", errno);
            break;
        }

        printf("id:%d, receive:%s", wd->id, wd->readbuf);
        fw = 0;
        do {
            fw = fwrite(wd->readbuf+fw, 1, nr-fw, wd->file);
        } while((ssize_t)fw < nr);
    }

    return NULL;
}

int main(void) {
    int work_num = 10;

    struct sockaddr_in serv_addr;
    // int sockfd;

    // ssize_t nr, nw;
    char writebuf[8] = "request";
    char readbuf[256];
    size_t len = 256;
    memset(readbuf, 0, len);

    FILE *file;
    file = fopen("./output", "a");
    if (!file) {
        print_err("client open file error:", errno);
        return RETURN_FAILURE;
    }

    memset((void *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    sockfd = socket(serv_addr.sin_family, SOCK_STREAM, 0);
    if (sockfd < 0) {
        print_err("client create socket error:", errno);
        return RETURN_FAILURE;
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        print_err("client connect error:", errno);
        return RETURN_FAILURE;
    }

    pthread_t pids[work_num];
    for (int i = 0; i < work_num; i++) {
        struct work_data wd;
        wd.id = i;
        wd.writebuf = writebuf;
        wd.readbuf = (char *) malloc (sizeof(char) * 256);
        wd.file = file;
        pthread_create(&pids[i], NULL, do_work, &wd);
    }

    for (int i = 0; i < work_num; i++) {
        pthread_join(pids[i], NULL);
    }
    
    close(sockfd);
    fclose(file);

    return 0;
}
