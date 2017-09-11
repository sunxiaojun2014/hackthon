#include <unistd.h>
#include <string.h>

#define unlikely(x) __builtin_expect(!!(x), 0) 

int readn(int fd, char *buf, int count) {
	int nr;
	int nleft = count;

	while (nleft > 0) {
		nr = read(fd, buf, nleft);
		if (unlikely(nr <= 0)) {
			return nr;
		}
		nleft -= nr;
		buf += nr;
	}

	return count - nleft;
}

int writen(int fd, const char *buf, int count) {
	int nw;
	int nleft = count;

	while (nleft > 0) {
		nw = write(fd, buf, nleft);
		if (unlikely(nw <= 0)) {
			return nw;
		}
		nleft -= nw;
		buf += nw;
	}

	return count;
}

