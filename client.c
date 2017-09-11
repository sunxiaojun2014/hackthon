/* Author: sxj
 * Date: 2017/09/10
 * 搬砖client  
 */ 

#include "utils.h"

#define PAGE_SIZE 4096

typedef unsigned char uint8;

int fd;
struct block_info *gbi[THREAD_NUM];

struct work_data {
   int  id; 
   int  sockfd;
   int  line_total;
   char *readbuf;
};

struct block_info {
	int  id;
	int  line_total;
	int  line_start;
	int  byte_total;
	char *buf;
};

int length_decode(char *buf) {
	return (uint8)(buf[3]) | (uint8)(buf[2]) << 8 | (uint8)(buf[1]) << 16 | (uint8)(buf[0]) << 24;
}

// 从offset处写文件 
int write_fd(int fd, char *buf, int count, off_t offset) {
	int nw, nleft;
	nleft = count;

	while (nleft > 0) {
		nw = pwrite(fd, buf, nleft, offset);
		buf += nw;
		nleft -= nw;
		offset += nw;
	}
	return count;
}

// 计算块文件的开始行号
int cacl_line_start(int block_id) {
	if (block_id == 0) return 1;

	int line_start = 0;
	int i = 0;
	while (1) {
		if (gbi[i] != NULL && gbi[i]->line_total > 0) {
			if (i == block_id) {
				break;
			}
			line_start += gbi[i]->line_total;
			i++;
		} else {
			continue;
		}
	}
	return (line_start + 1);
}

// 计算输出块开始写的位移
int calc_offset(int line_start, int block_id) {
	if (block_id == 0) return 0;

	int line_bytes = calc_widths(1, line_start - 1);
	int byte_total = 0;
	int i = 0;
	while(1) {
		if (gbi[i] != NULL && gbi[i]->byte_total > 0) {
			if (i == block_id) {
				break;
			}
			byte_total += gbi[i]->byte_total;
			i++;
		} else {
			continue;
		}
	}
	// printf("calc_offset:line_bytes:%d, byte_total:%d\n", line_bytes, byte_total);
	return (byte_total+line_bytes);
}

// 发送请求
void* send_request(void *arg) {
	struct work_data *wd = (struct work_data *)arg;
	int sockfd = wd->sockfd;
	int line_total = wd->line_total;
	char writebuf[256];
	int i = 0;
	int len;

	while (i < line_total) {
		// TODO 这块需要一次写多条数据
		len = ((line_total - i) > 256) ? 256 : (line_total - i);
		if (writen(sockfd, writebuf, len) <= 0) {
			print_err("client write error:", errno);
			return NULL;
		}
		i += len;
	}
	printf("send request end, send %d request\n", i);
	return NULL;
}

// 文件处理, 加行号
void *block_process(void *arg) {
	struct block_info *bi = (struct block_info *)arg;
	char *buf = bi->buf;
	int nleft = bi->byte_total;
	// 写文件的buffer
	// TODO 需要测试多大的buffer是合适的
	char buffer[PAGE_SIZE+256];
	int line_byte_count = 0;
	int pos = 0;
	
	int line_start = cacl_line_start(bi->id);
	bi->line_start = line_start;
	int line_num = line_start;
	int offset = calc_offset(line_start, bi->id);

	printf("id:%d, line_start:%d, offset:%d\n", bi->id, line_start, offset);

	while (nleft > 0) {
		if (*buf) {
			line_byte_count++;
			if (is_end_of_line(buf)) {
				pos += sprintf(buffer + pos, "%d", line_num);	
				memcpy(buffer + pos, buf - line_byte_count + 1, line_byte_count);
				pos += line_byte_count;
				//printf("buffer content:%s", buffer);
				if (pos >= PAGE_SIZE) {
					write_fd(fd, buffer, PAGE_SIZE, offset);
					offset += PAGE_SIZE;
					pos -= PAGE_SIZE;
					memcpy(buffer, buffer+PAGE_SIZE, pos);
				}
				line_num++;
				line_byte_count = 0;
			}
			buf++;
			nleft--;
		} else {
			spin(buf);
		}
	}

	if (pos > 0) {
		write_fd(fd, buffer, pos, offset);
	}
	printf("id:%d block_process end, line_num:%d, offset:%d\n", bi->id, line_num, offset);
}

void *receive_block(void *arg) {
    struct work_data *wd = (struct work_data *)arg;
	char *readbuf = wd->readbuf;
	int sockfd = wd->sockfd;

	char lenbuf[4];
	char block_info[16];
	int len, n, nr;
	int line_num = 0;
	pthread_t pid1;
	pthread_t pid2;

	// 读取块数据元信息
	if (readn(sockfd, block_info, 9) < 0) {
		print_err("client read block info error:", errno);
		close(sockfd);
		return NULL;
	}

	struct block_info *bi = (struct block_info *) malloc(sizeof(struct block_info));
	if (bi == NULL) {
		print_err("malloc new block info error:", errno);
		close(sockfd);
		return NULL;
	}

	int id = (int)block_info[0] - 48;
	int line_total = length_decode(block_info+1);
	int byte_total = length_decode(block_info+5);
	bi->id = id;
	bi->line_total = wd->line_total = line_total;
	bi->byte_total= byte_total;

	// TODO 内存对齐
	char *buf = (char *) calloc(1, sizeof(char) * byte_total);
	printf("id:%d, line_count:%d, byte_total:%d, buf:%p\n", id, wd->line_total, byte_total, buf);
	if (buf == NULL) {
		print_err("malloc new block buffer error:", errno);
		close(sockfd);
		return NULL;
	}
	bi->buf = buf;

	gbi[bi->id]	= bi;
	if (pthread_create(&pid1, NULL, block_process, (void *)bi) < 0) {
		print_err("create new block process thread error:", errno);
		close(sockfd);
		return NULL;
	}

	if (pthread_create(&pid2, NULL, send_request, wd) < 0) {
		print_err("create new send request thread error:", errno);
		close(sockfd);
		return NULL;
	}
	printf("create send request thread\n");

	// TODO 多进程设置文件偏移
    while (1) {
		// TODO 一块一块buffer的读,然后处理
		// 可以先读一块,然后再用前面四个字节作为长度
		nr = readn(sockfd, lenbuf, 4);
		if (nr < 0) {
			print_err("client read length error:", errno);
			close(sockfd);
			return NULL;
		} else if (0 == nr) {
			print_err("server closed connection:", errno);
			break;
		}

		// 根据长度读取数据
		len = length_decode(lenbuf);
		while (len > 0) {
			n = (len > PAGE_SIZE) ? PAGE_SIZE: len;
			if (readn(sockfd, buf, n) <= 0) {
				print_err("client read length error:", errno);
				close(sockfd);
				return NULL;
			}
			// printf("readbuf:%s",readbuf);
			len -= n;
			buf += n;
		}
	}

	printf("client receive block: %d end\n", id);
	close(sockfd);
	pthread_join(pid1, NULL);
    return NULL;
}

void usage(void) {
	printf("Usage: ./client -h [host] -p [port] -f [/path/to/output/file]\n");
}

int main(int argc, char *argv[]) {
    pthread_t pids[THREAD_NUM];
	int sockfd;
    struct sockaddr_in serv_addr;
	char *host;
	int port;
	char *file;

	if (argc < 7) {
		printf("Not enogh parametes\n");
		usage();
		return 0;
	}

	int opt;
	while ((opt = getopt(argc, argv, "h:p:f:")) != -1) {
		switch(opt) {
			case 'h':
				host = optarg;
			case 'p':
				port = atoi(optarg); 
				break;
			case 'f':
				file = optarg;
				break;
			default:
				usage();
				return 0;
		}
	}

	fd = open(file, O_RDWR|O_CREAT, 0666);
	if (fd < 0) {
        print_err("client open file error:", errno);
        return RETURN_FAILURE;
	}

	for (int i = 0; i < THREAD_NUM; i++) {
		gbi[i] = (struct block_info *) malloc(sizeof(struct block_info));
	}

    memset((void *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    serv_addr.sin_addr.s_addr = inet_addr(host);

	int no_delay = 1;
	for (int i = 0; i < THREAD_NUM; ) {
		sockfd = socket(serv_addr.sin_family, SOCK_STREAM, 0);
		if (sockfd < 0) {
			print_err("client create socket error:", errno);
			return RETURN_FAILURE;
		}

		if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
			print_err("client connect error:", errno);
			return RETURN_FAILURE;
		}

		// socket opts set 
		setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, (char *)&no_delay, sizeof(no_delay));
    
        struct work_data *wd = (struct work_data *) malloc(sizeof(struct work_data));
        wd->id = i;
		wd->sockfd = sockfd;
        wd->readbuf = (char *) malloc(sizeof(char) * 1024);
        if (pthread_create(&pids[i], NULL, receive_block, wd) < 0) {
			print_err("create new receive brick thread error:", errno);
			return RETURN_FAILURE;
		}
		i++;
	}

    for (int i = 0; i < THREAD_NUM; i++) {
        pthread_join(pids[i], NULL);
    }
    
	close(fd);
	// TODO free  mem

    return 0;
}
