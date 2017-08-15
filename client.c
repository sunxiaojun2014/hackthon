/*
 * 
 * 
 */ 

#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>

#define PORT 8360
#define RETURN_FAILURE 1

#define print_err(fmt, errno)   fprintf(stderr, fmt"%s\n", strerror(errno))

int main(void) {
    struct sockaddr_in serv_addr;
    int sockfd;

    ssize_t nr, nw;
    char writebuf[8] = "request";
    char readbuf[1024];
    size_t len = 1024;

    FILE *file;

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

    file = fopen("./output", "w+");
    if (!file) {
        print_err("client open file error:", errno);
        return RETURN_FAILURE;
    }

    memset(readbuf, 0, len);
    size_t fw;

    //while (fgets(buf, len, stdin) != NULL) {
    while(1){
        nw = write(sockfd, writebuf, strlen(writebuf));
        if (nw < 0) {
            print_err("client write error:", errno);
            break;
        }

        nr = read(sockfd, readbuf, len);
        if (nr < 0) {
            print_err("client read error:", errno);
            break;
        }
        //printf("nr:%zd\n", nr);
        //printf("receive:%s", readbuf);
        fw = 0;
        do {
            fw = fwrite(readbuf+fw, 1, nr-fw, file);
        } while((ssize_t)fw < nr);
    }

    close(sockfd);
    fclose(file);

    return 0;
}
