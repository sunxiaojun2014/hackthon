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
    char buf[16];
    size_t len = 16;

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
    printf("connect done");

    memset(buf, 0, len);

    while (fgets(buf, len, stdin) != NULL) {
        // printf("buf:%s, buflen:%lu\n", buf, strlen(buf));
        nw = write(sockfd, buf, strlen(buf)+1);
        if (nw < 0) {
            print_err("client write error:", errno);
            break;
        }
        printf("nw:%zd\n", nw);

        nr = read(sockfd, buf, len);
        if (nr < 0) {
            print_err("client read error:", errno);
            break;
        }
        printf("nr:%zd\n", nr);
        printf("receive:%s", buf);
        memset(buf, 0, len);
    }

    return 0;
}
