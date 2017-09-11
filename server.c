/*
 * 搬砖server端
 * 
 */ 

#include <netinet/ip.h>

#include "utils.h"

#define BLOCKSIZE           128*1024*1024  // 每个线程处理128M大小的文件
#define METADATAINITSIZE    16*1024*1024   // 每个线程生成的索引数组的初始的行数
#define DELTA               1024*1024      // 当索引数组大小不够时,增加的数组大小

void new_block_process(int id, int size,  char* begin, char *end);

struct metadata *md[THREAD_NUM];
pthread_t pids[THREAD_NUM];

volatile int read_total;
char *file_buf;

struct metadata {
	char 	*data;
	struct  line_index *li;
	int 	line_total;
	bool 	is_end; // 块是否处理结束
};

struct block_info {
	int  id;
	int  size;
	char *begin;
	char *end;
};

struct netConn {
	int id;
	int connfd;
};

// 文件的每行索引信息
struct line_index {
	int end_pos;
	int byte_count;
	int line_num;
};

// 反转
void reverse(char *begin, char *end) {
	while (end > begin) {
		*end = *end ^ *begin;
		*begin = *end ^ *begin;
		*end = *end ^ *begin;
		end--;
		begin++;
	}
}

// 处理一行
char *line_process(char *in, char *out, int count) {
	if (in== NULL || out== NULL || count == 0) {
		return out;
	}

	in = in - count + 1;

	int i = 0;
	if (count >= 4) { // count 包含换行符 
		// 去除size/3的数据量
		i = (count - 1) / 3;
		memcpy(in+ i, in+ 2*i, count - 2*i);
	}

	int len = count - i;
	reverse(in, in+ len - 2); // 逆序不包含换行符
	memcpy(out, in, len);

	return (out+len);
}

// 读取的文件是否够起一个新的处理线程
// 满足两个条件: 当前线程的处理速度小于文件的读取速度
// 2. 当前文件的读取比一个线程处理的速度要快
bool is_ready(char *begin, int byte_proc) {
	char *buf = begin + BLOCKSIZE + 256;
	if (*buf != '\0' && (byte_proc < (BLOCKSIZE + 256))) {
		return true;
	}
	return false;
}

// 返回每个线程处理的buf的边界
char *buf_edge(char *buf) {
	char *buf_edge = buf + BLOCKSIZE;
	while (!is_end_of_line(buf_edge)) {
		buf_edge++;
	}
	return (buf_edge + 1);
}

// 处理数据的线程
void *block_process(void *arg) {
	struct block_info *bi = (struct block_info *)arg;
    int id = bi->id;
    int size = bi->size;
    char *begin = bi->begin;
    char *end = bi->end;

    char *buf = begin;
    char *pos = begin;
    int line_num, byte_count, byte_total;
	line_num = byte_count = byte_total = 0;

	int is_start_thread = false;
    int len = METADATAINITSIZE;

	struct line_index *li = (struct line_index *) malloc(sizeof(struct line_index) * len);
	md[id]->li = li;
	md[id]->is_end = false;
	md[id]->data = begin;
    printf("Thread %d start block process, %d,  begin:%p, end:%p\n", id, (int)*begin, begin, end);
    while (buf != end) {
        if (*buf != '\0') {
            byte_count++;
			byte_total++;
            if (is_end_of_line(buf)) {
                pos = line_process(buf, pos, byte_count);
                if (line_num >= len) {
                    len += DELTA;
                    li = (struct line_index *) realloc(li, len);
                    md[id]->li = li;
                }
                li[line_num].end_pos = (pos - begin -1);
                li[line_num].byte_count = byte_count;
                line_num++;
                li[line_num - 1].line_num = line_num;
                byte_count = 0;
                // 检查是否可以启动一个新的进程去处理下一块
				if (!is_start_thread && (id != (THREAD_NUM - 1)) && is_ready(begin, byte_total)) {
					end = buf_edge(begin);
					// printf("Thread %d, %d\n", id, (int)*(end-1));
					new_block_process(id + 1, size, end, file_buf+size);
					is_start_thread = true;	
				}	
            }
			buf++;
        } else {
            spin(buf);
        }
    }

	md[id]->line_total = line_num;
	md[id]->is_end = true;
	printf("Thread %d, line_num:%d, byte_total:%d, begin:%p, end:%p, end_pos:%d\n", id, line_num, byte_total, begin, end, li[line_num-1].end_pos);
    return NULL;
}

// 启动一个新的块处理进程
void new_block_process(int id, int size,  char* begin, char *end) {
	struct block_info *bi = (struct block_info *) malloc(sizeof(struct block_info));
	bi->id = id;
	bi->size = size;
	bi->begin = begin;
	bi->end = end;

	if (pthread_create(&pids[id], NULL, block_process, bi) < 0) {
		print_err("create block process thread error:", errno);
		exit(1);
	}
}

void *load_file(void *arg) {
	char *file = (char *)arg;
    int fd;
    struct stat st;

    // 文件处理
    fd = open(file, O_RDONLY);
    if (fd < 0) {
        print_err("open file error:", errno);
        return NULL;
    }

	//posix_fadvise(filefd, 0, 0, POSIX_FADV_SEQUENTIAL);

    if (fstat(fd, &st) < 0) {
        print_err("get file stat error:", errno);
        return NULL;
    }

	readahead(fd, 0, st.st_size);

	// TODO file_buf 的判断
	// TODO 申请1G内存的这段时间什么事都没干(大概0.6s)
    file_buf = (char *) calloc(1, sizeof(char) * st.st_size);
	for (int i = 0; i < THREAD_NUM; i++) {
		md[i] = (struct metadata *) malloc(sizeof(struct metadata));
	}
	int read_count = 0;
	
	// 新起一个线程边读边处理
	printf("load file:begin:%p, end:%p\n", file_buf, file_buf + st.st_size);
	new_block_process(0, st.st_size, file_buf, file_buf + st.st_size);
	
	while((read_count = read(fd, file_buf+read_total, 16384)) > 0) {
		read_total += read_count;
	}
	printf("read_total:%d\n", read_total);
	
	for (int i = 0; i < THREAD_NUM; i++) {
		pthread_join(pids[i], NULL);
	}
	return NULL;
}

// encode length into buffer with big endian
void length_encode(int length, char *buf) {
	buf[0] = length >> 24;
	buf[1] = length >> 16;
	buf[2] = length >> 8;
	buf[3] = length;
}

// 发送砖头
void *send_brick(void *arg) {
	struct netConn *nc = (struct netConn*)arg;
	int connfd = nc->connfd;
	int id = nc->id;

	int line, request_num, length, byte_write;

	while (!md[id]->is_end) { }

	int line_total = md[id]->line_total;
	int byte_total = md[id]->li[line_total-1].end_pos+1;
	char *data = md[id]->data;

    char writebuf[16];
    char readbuf[1024];

	line = byte_write = 0;
	// 先发送块数据的元信息
	sprintf(writebuf, "%d", id);
	length_encode(line_total, writebuf+1);
	length_encode(byte_total, writebuf+5);
	//length_encode(line_start, writebuf+5);
	// printf("line_start:%d, line_count:%d, byte_total%d\n", line_start, line_count, byte_total);
	// printf("id:%d, line_total:%d\n", id, line_total);
	if (writen(connfd, writebuf, 9) < 0) {
		print_err("server write error:", errno);
		close(connfd);
		return NULL;
	}

    for ( ;line < line_total; ) {
		if ((request_num = read(connfd, readbuf, 1024)) <= 0) {
			print_err("server read error:", errno);
			close(connfd);
			return NULL;
		}
		length = md[id]->li[line+request_num-1].end_pos - byte_write + 1;
		length_encode(length, writebuf);
		if (writen(connfd, writebuf, 4) <= 0) {
			print_err("server write length error:", errno);
			close(connfd);
			return NULL;
		}

		// TODO 数据的压缩
		if (writen(connfd, data, length) <= 0) {
			print_err("server write file data error:", errno);
			close(connfd);
			return NULL;
		}
		byte_write += length;
		data += length;
		line += request_num;	
    }

	printf("block: %d transmit finished\n", id);
	close(nc->connfd);
}


// 释放申请的内存资源
void free_mem() {
	free(file_buf);
	
	for (int i = 0; i < THREAD_NUM; i++) {
		free(md[i]->li);
		free(md[i]);
	}
}

void usage(void) {
	printf("Usage: ./server -p [port] -f [/path/to/input/file]\n");
}

int main(int argc, char *argv[]) {
    struct sockaddr_in sa, ca;
    int listensock;
    socklen_t addrlen;
	int port;
	char *file;

	if (argc < 5) {
		printf("Not enogh parametes\n");
		usage();
		return 0;
	}

	int opt;
	while ((opt = getopt(argc, argv, "p:f:")) != -1) {
		switch(opt) {
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
	
	pthread_t pid;
	if (pthread_create(&pid, NULL, load_file, file) < 0) {
        print_err("server create load file thread error:", errno);
        return RETURN_FAILURE;
	}
        
    memset((void *)&sa, 0, sizeof(sa)); 
    sa.sin_family = AF_INET; 
    sa.sin_port = htons(port);
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

	printf("server listen on port: %d\n", port);
        
	int id = 0;
	int no_delay = 1;
	int send_buf = 16384;
    int connfd;
	pthread_t pids[THREAD_NUM];
    addrlen = sizeof(ca);

	while (id < THREAD_NUM) {
		connfd = accept(listensock, (struct sockaddr *)&ca, &addrlen);
		if (connfd < 0) {
			print_err("server accpet error:", errno);
			return RETURN_FAILURE;
		}

		// 禁止Nagle算法
		setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, (char *)&no_delay, sizeof(no_delay));
		// 设置socket send buffer
		setsockopt(connfd, SOL_SOCKET, SO_SNDBUF, &send_buf, sizeof(send_buf));

		struct netConn *nc = (struct netConn*) malloc(sizeof(struct netConn));
		nc->id = id++;
		nc->connfd = connfd;
		if (pthread_create(&pids[id-1], NULL, send_brick, (void *)nc) < 0) {
			print_err("create new send brick errror:", errno);
			return RETURN_FAILURE;
		}
	}

	for (int i = 0; i < THREAD_NUM; i++) {
		pthread_join(pids[i], NULL);
	}

	free_mem();

    printf("file transmit end\n");

    return 0;
}
