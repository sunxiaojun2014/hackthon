/*
 * 
 * 
 */ 

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>


#define PORT 8360
#define RETURN_FAILURE 1

#define print_err(fmt, errno)   fprintf(stderr, fmt"%s\n", strerror(errno))

int total = 0;

// 从buffer中读取一行,返回读取到的数目(包括换行符)
int readline(char *in, char *out) {
    if (in == NULL || strlen(in) == 0) {
        return 0;
    }

    int i;
    for (i = 0; ; i++) {
        out[i] = in[i];
        if (in[i] == '\r' || in[i] == '\n') {
            break;
        }
    }
    out[i+1] = '\0';
    return (i+1);
}

// string 逆序排序
char *reverse(char *str) {
    char *r = str;
    char *p = str;

    while(*p)  p++;
    p -= 2; // 不包含换行符

    while (p > str) {
        *p = *p ^ *str;
        *str = *p ^ *str;
        *p = *p ^ *str;
        p--;
        str++;
    }

    return r;
}

// 砖头处理, 返回处理的总字节数
void brickProcess(char *in, char *out) {
    char count[16], tmp[256];
    int nr, index;
    int in_total =0;
    // int out_total = 0;
    int line_num = 1;
    printf("brick process start\n");
    int i = 1;
    while ((nr = readline(in+in_total, tmp)) > 0) {
        //printf("tmp:%s", tmp);
        in_total += nr;

        //TODO
        // #if LINUX
        //itoa(line_num, buf, 10);
        // #if DARWIN
        sprintf(count, "%d", line_num);
        // printf("buf:%d, %d\n", *count, *(count+1));
        memcpy(out+total, count, strlen(count));
        total += strlen(count);
        // printf("out_total:%d\n", out_total);

        if (nr < 4) {
            reverse(tmp);
            memcpy(out+total, tmp, strlen(tmp));
            total += strlen(tmp);
            // printf("out:%d, tmp_len:%lu\n", out_total, strlen(tmp));
        } else {
            // 去除size/3的数据量
            index = (nr - 1) / 3;
            memcpy(tmp+index, tmp+2*index, strlen(tmp)+2-2*index);
            // tmp 逆序
            reverse(tmp);
            memcpy(out+total, tmp, strlen(tmp));
            // printf("out:%s", out+out_total-1);
            total += strlen(tmp);
        }
        // printf("out_total:%d\n", out_total);
        line_num++;
        //printf("total:%d\n", total);
        if (total > i * 1024 * 1024) {
            printf("total:%d\n", total);
        }
    }
}

void* file_process(void *buf) {
    int filefd;
    struct stat st;
    void *mmapfile;

    // 文件处理
    filefd = open("./testdata1/testfile", O_RDONLY);
    // filefd = open("./testfile", O_RDONLY);
    if (filefd < 0) {
        print_err("open file error:", errno);
        return NULL;
    }

    if (fstat(filefd, &st) < 0) {
        print_err("get file stat error:", errno);
        return NULL;
    }

    mmapfile = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, filefd, 0);
    if (mmapfile == NULL) {
        print_err("mmap file error:", errno);
        close(filefd);
        return NULL;
    }
    
    printf("mmap file end\n");
    brickProcess((char *)mmapfile, (char *)buf);
    printf("brickProcess end\n");
    exit(1);
    return NULL;
}

int main(void) {
    struct sockaddr_in sa, ca;
    int listensock;
    socklen_t addrlen;
    int connfd;
    ssize_t nr, nw;

    // TODO
    char *buf = (char *) malloc(sizeof(char) * 1024);

    pthread_t thread;
    if (pthread_create(&thread, NULL, file_process, buf) < 0) {
        print_err("server pthread create error:", errno);
        return RETURN_FAILURE;
    }
    
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

    /*
    if (fcntl(connfd, F_SETFL, fcntl(connfd, F_GETFL) | O_NONBLOCK) < 0) {
        print_err("server set fd noblocking error:", errno);
        return RETURN_FAILURE;
    }
    */

    char brick[256];
    char readbuf[16];
    int len = 16;
    int pos, l;
    for (pos = 0; pos < total; ) {
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
        // printf("nr:%zd\n", nr);
        // printf("server receive: %s\n", readbuf);
        l = readline(buf+pos, brick); 
        // printf("l:%d, pos:%d, brick:%s",l, pos, brick);
        if (l <= 0) {
            continue;
        }
        pos += l;
        // printf("server send:%s", brick);

        nw = write(connfd, brick, strlen(brick));
        if (nw < 0) {
            print_err("server write error:", errno);
            return RETURN_FAILURE;
        }
    } 
    
    printf("file transmit end\n");

    return 0;
}
