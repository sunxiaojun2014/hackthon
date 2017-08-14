/*
 * 
 * 
 */ 

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <netinet/ip.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>


#define PORT 8360
#define RETURN_FAILURE 1

#define print_err(fmt, errno)   fprintf(stderr, fmt"%s\n", strerror(errno))

// 从buffer中读取一行,返回读取到的数目(包括换行符)
int readline(char *in, char *out) {
    if (in == NULL) {
        return 0;
    }

    int i;
    for (i = 0; ; i++) {
        // printf("%d, %c\n", i, in[i]);
        out[i] = in[i];
        if (in[i] == '\r' || in[i] == '\n') {
            break;
        }
    }
    return (i+1);
}

// 砖头处理
void brickProcess(char *in, char *out) {
    memcpy(out, in, strlen(in));
}

int main(void) {
    struct sockaddr_in sa, ca;
    int listensock;
    socklen_t addrlen;
    int connfd;
    
    ssize_t nr, nw;
    char buf[1024];
    size_t len = 1024;
    memset(buf, 0, len);

    int filefd;
    struct stat st;
    void *mmapfile;

    memset((void *)&sa, 0, sizeof(sa)); 
    sa.sin_family = AF_INET; 
    sa.sin_port = htons(PORT);
    sa.sin_addr.s_addr = htonl(INADDR_ANY);

    listensock = socket(sa.sin_family, SOCK_STREAM, 0);
    if (listensock < 0) {
        print_err("server create listen socket error:", errno);
        return RETURN_FAILURE;
    }
    
    int reuseaddr = 1;
    if (setsockopt(listensock, SOL_SOCKET, SO_REUSEADDR, (const void *)&reuseaddr, sizeof(int)) < 0) {
        print_err("server setsockopt reuseaddr error:", errno);
        return RETURN_FAILURE;
    }

    if (bind(listensock, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        print_err("server bind error:", errno);
        return RETURN_FAILURE; 
    }

    if (listen(listensock, 1024) < 0) {
        print_err("server listen error:", errno);
        return RETURN_FAILURE;
    }
        
    addrlen = sizeof(ca);
    connfd = accept(listensock, (struct sockaddr *)&ca, &addrlen);
    if (connfd < 0) {
        print_err("server accpet error:", errno);
        return RETURN_FAILURE;
    }

    filefd = open("./testfile", O_RDONLY);
    if (filefd < 0) {
        print_err("open file error:", errno);
        return RETURN_FAILURE;
    }

    if (fstat(filefd, &st) < 0) {
        print_err("get file stat error:", errno);
        return RETURN_FAILURE;
    }

    mmapfile = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, filefd, 0);
    if (mmapfile == NULL) {
        print_err("mmap file error:", errno);
        close(filefd);
        return RETURN_FAILURE;
    }
    
    brickProcess((char *)mmapfile, buf);

    /*
    if (fcntl(connfd, F_SETFL, fcntl(connfd, F_GETFL) | O_NONBLOCK) < 0) {
        print_err("server set fd noblocking error:", errno);
        return RETURN_FAILURE;
    }
    */

    char brick[128];
    char readbuf[1024];
    int pos;
    for(pos = 0; pos < (int)strlen(buf);) {
        nr = read(connfd, readbuf, len);
        if (nr < 0) {
            /*
            if (errno == EAGAIN) {
                printf("eagain nr:%zd", nr);
                continue;
            } else {
            */
                print_err("server read error:", errno);
                return RETURN_FAILURE;
        }
        if (nr == 0) {
            printf("client close\n");
            break;
        }
        printf("nr:%zd\n", nr);
        printf("server receive: %s", readbuf);
        int l = readline(buf+pos, brick); 
        if (l <= 0) {
            continue;
        }
        pos += l;
        // printf("ps:%d, l:%d, brick:%s\n", pos, l, brick);

        nw = write(connfd, brick, strlen(brick));
        if (nw < 0) {
            print_err("server write error:", errno);
            return RETURN_FAILURE;
        }
    } 
    
    printf("file transmit end\n");

    return 0;
}
