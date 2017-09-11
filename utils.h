#ifndef __UTILS__H
#define __UTILS__H

#include <string.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/tcp.h> // TCP_NODELAY

#include <getopt.h>

#define _GNU_SOURCE 
#include <fcntl.h>
#define _XOPEN_SOURCE 500
#include <unistd.h>



#define RETURN_FAILURE 1
#define THREAD_NUM 8

#define print_err(fmt, errno)   fprintf(stderr, fmt"%s\n", strerror(errno))

int readn(int fd, char *buf, int count);

int writen(int fd, char *buf, int count);

extern inline void spin(char *);
extern inline bool is_end_of_line(char *);
extern inline int calc_width(int);
extern inline int calc_widths(int, int);

inline void spin(char *buf) {
	while(strlen(buf) <= 0) { }
}

inline bool is_end_of_line(char *buf) {
	if (*buf == '\r' || *buf == '\n') {
		return true;
	}
	return false;
}

// 计算每个数字的位数
inline int calc_width(int num) {
	if (num < 10) {
		return 1;
	} else if (num < 100) {
		return 2;
	} else if (num < 1000) {
		return 3;
	} else if (num < 10000) {
		return 4;
	} else if (num < 100000) {
		return 5;
	} else if (num < 1000000) {
		return 6;
	} else if (num < 10000000) {
		return 7;
	} else if (num < 100000000) {
		return 8;
	} else if (num < 1000000000) {
		return 9;
	} else if (num < 10000000000) {
		return 10;
	}
}

inline int calc_widths(int start, int end) {
	int i;
	int total = 0;

	for (i = start; i <= end; i++) {
		total += calc_width(i);
	}
	return total;
}

#endif // __UTILS__H
