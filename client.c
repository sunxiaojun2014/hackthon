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
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
//bool
#include <stdbool.h>

#include "utils.h"


#define PORT 8333
#define RETURN_FAILURE 1
#define THREAD_NUM 1

#define print_err(fmt, errno)   fprintf(stderr, fmt"%s\n", strerror(errno))

#define BLOCK  16*1024*1024
#define DELTA  1024*1024

struct metadata *md;

int line_total = 0;
char *file_buf;

struct work_data {
	char *in;
	char *out;
	int len;
	int line_start;
	int line_end;
	int id;
};

struct block {
	char *buf;
	char *end_pos;
	int  total;
};

struct netConn {
	int id;
	int connfd;
};


// 从buffer中读取一行,返回读取到的数目(包括换行符)
int readline2(char *in, char *out) {
    if (in == NULL || strlen(in) == 0) {
		printf("readline error, fileptr: %p, char:%d, char bfore:%d\n", in, (int)*in, (int)*(in-1));
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
char *reverse2(char *str) {
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


void* brick_process(void *arg) {
	struct work_data *wd = (struct work_data *)arg;
	char *in = wd->in;
	char *out = wd->out;
    char count[16], tmp[256];
    int nr, index;
    int in_total = 0;
    // int out_total = 0;
    int line_num = wd->line_start;
	int line_count = 0;
	int total = 0;
    //printf("brick process start\n");
    while ((in_total < wd->len) &&(nr = readline2(in+in_total, tmp)) > 0) {
        //printf("tmp:%s", tmp);
        in_total += nr;
		// printf("id:%d, in_total:%d\n", wd->i, in_total);
		/*
		if (in_total >= wd->len) {
		//	printf("end:id:%d, in_total:%d, wd->len:%d\n", wd->id, in_total, wd->len);
			break;
		}
		*/

        sprintf(count, "%d", line_num);
        memcpy(out+total, count, strlen(count));
        total += strlen(count);
        // printf("out_total:%d\n", out_total);

        if (nr >= 4) {
            // 去除size/3的数据量
            index = (nr - 1) / 3;
            memcpy(tmp+index, tmp+2*index, strlen(tmp)+2-2*index);
		}
		// tmp 逆序
		reverse2(tmp);
		memcpy(out+total, tmp, strlen(tmp));
		// printf("out:%s", out+out_total-1);
		total += strlen(tmp);
        // printf("out_total:%d\n", out_total);
        line_num++;
		line_count++;
		/*
        printf("total:%d\n", total);
        if (total > i * 1024 * 1024) {
            printf("total:%d\n", total);
        }
		i++;
		*/
    }
   	printf("-----------id:%d, total:%d, line_count:%d, line_total:%d, line_end:%d\n", wd->id, total, line_count, line_num, wd->line_end);
}

/*
// 砖头处理, 返回处理的总字节数
void* brick_process(void *arg) {
	struct work_data *wd = (struct work_data *)arg;
	char *in = wd->in;
	char *out = wd->out;
    char count[16], tmp[256];
    int nr, index;
    int in_total = 0;
    // int out_total = 0;
    int line_num = 1;
	int total = 0;
    printf("brick process start\n");
    int i = 1;
    while ((nr = readline(in+in_total, tmp)) > 0) {
        //printf("tmp:%s", tmp);
        in_total += nr;
		// printf("id:%d, in_total:%d\n", wd->i, in_total);
		if (in_total > wd->len) {
			break;
		}

        sprintf(count, "%d", line_num);
        memcpy(out+total, count, strlen(count));
        total += strlen(count);
        // printf("out_total:%d\n", out_total);

        if (nr > 4) {
            // 去除size/3的数据量
            index = (nr - 1) / 3;
            memcpy(tmp+index, tmp+2*index, strlen(tmp)+2-2*index);
		}
		// tmp 逆序
		reverse(tmp);
		memcpy(out+total, tmp, strlen(tmp));
		// printf("out:%s", out+out_total-1);
		total += strlen(tmp);
        // printf("out_total:%d\n", out_total);
        line_num++;
	}
   	printf("id:%d, total:%d\n", wd->id, total);
}
*/

// 文件的信息
struct file_info {
	char *buf;
	int  size;
};

// 文件的每行索引信息
struct metadata {
	int end_pos;
	int byte_count;
	int line_num;
};

void spin(char *buf) {
	while(strlen(buf) <= 0) { }
}

void reverse(char *begin, char *end) {
	while (end > begin) {
		*end = *end ^ *begin;
		*begin = *end ^ *begin;
		*end = *end ^ *begin;
		end--;
		begin++;
	}
}

// 根据索引信息读取一行,返回写之后的buffer
int readline(char *in, char *out, int line_num) {
	int end_pos = md[line_num].end_pos;
	int byte_count = md[line_num].byte_count;
	memcpy(out, in + end_pos + 1 - byte_count, byte_count);
	return byte_count;
}

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

void *file_process(void *arg) {
	struct file_info *fi = (struct file_info *)arg;
	char *buf = fi->buf;
	int nleft = fi->size;

	// 记录最开始的位置,用于计算end_pos
	char *origin = buf;
	// 记录行处理写入的开始位置
	char *start = buf;
	int line_num = 0;
	int byte_count = 0;
	int end_pos;
	int len = BLOCK;
	printf("start alloc\n");
	md = (struct metadata *) malloc(sizeof(struct metadata) * len);

	// TODO 单线程处理时间大概在十秒左右
	// 如果IO读取速度比较快,处理程序可以改成多进程的版本
	// pthread_t pids[8];
	printf("start file process\n");
	while (nleft > 0) {
		if (*buf != '\0') {
			byte_count++;
			if (*buf == '\r' || *buf == '\n') {
				// line process 可以交给线程去处理
				start = line_process(buf, start, byte_count);
				// printf("line byte_count:%d\n", byte_count);
				// start = line_process(buf - byte_count, start, byte_count);
				if (line_num >= len) {
					len += DELTA;
					md = (struct metadata *)realloc(md, len);
				}
				md[line_num].end_pos = (start - origin - 1);
				md[line_num].byte_count = (line_num > 0) ? (md[line_num].end_pos - md[line_num-1].end_pos) : md[0].end_pos+1;
				md[line_num].line_num = ++line_num;
				byte_count = 0;
			}
			nleft--;
			buf++;
		} else {
			spin(buf);
		}
	}

	line_total = line_num;
	printf("line process end, line_num:%d, end_pos:%d\n", line_num, md[line_num-1].end_pos);

	/*
	int file_len = md[line_num-1].end_pos+1;
	FILE *file = fopen("./output", "a");
	int fw = 0;
	int n;
	do {
		n = fwrite(origin+fw, 1, file_len-fw, file);
		fw += n;
	} while(fw < file_len);

	fclose(file);
	printf("write to file end\n");
	*/
}

void* load_file() {
    int filefd;
    struct stat st;

    // 文件处理
    filefd = open("./testdata1/testfile", O_RDONLY);
    if (filefd < 0) {
        print_err("open file error:", errno);
        return NULL;
    }

    if (fstat(filefd, &st) < 0) {
        print_err("get file stat error:", errno);
        return NULL;
    }

    file_buf = (char *) calloc(1, sizeof(char) * st.st_size);
	int read_count = 0;
	int read_total = 0;
	
	// 新起一个线程边读边处理
	pthread_t pid;
	struct file_info *fi = (struct file_info *) malloc(sizeof(struct file_info));
	if (fi == NULL) {
		print_err("new file info error:", errno);
		exit(1);
	}
	fi->buf = file_buf;
	fi->size = st.st_size;
	if (pthread_create(&pid, NULL, file_process, fi) < 0) {
		print_err("create file process thread error:", errno);
		exit(1);
	}

	while((read_count = read(filefd, file_buf+read_total, 4*1024)) > 0) {
		read_total += read_count;
	}
	printf("read_total:%d\n", read_total);
	/*
    mmapfile = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, filefd, 0);
    if (mmapfile == NULL) {
        print_err("mmap file error:", errno);
        close(filefd);
        return NULL;
    }
    
    printf("mmap file end\n");
	*/
	/*
	pthread_t id[10];
	memset(id, 0, 10);
	int i = 0;
	// int num = 110 * 1024 * 1024;
	int num = 110 * 1024 * 1024;
	int total1 = 0;
	int count = 0;
	bool is_end = false;
	//char *file_start = (char *)mmapfile;
	char *file_start = buf;
	int line_start = 0;
	int line_end = 1;
	int line_num = 0;
	int line_count = 0; // 每行的字符数
	// 读取文件放入buffer中
	int total = 0;
	char *file_ptr = file_start;
	printf("file_start:%p\n", file_ptr);
	//char *end_pos = (char *)malloc();
	while (1) {
		count++;
		line_count++;
		if (*file_ptr == '\r' || *file_ptr == '\n') {
			line_num++;	
			total += line_count;
			line_count = 0;
			// printf("total:%d, count:%d\n", total, count);
			if(count >= num || total >= st.st_size) {
				// 创建一个线程去处理这批数据
				line_start = line_end;
				line_end = line_start + line_num;
				struct work_data* arg = (struct work_data *)malloc(sizeof(struct work_data));
				printf("file_ptr:%p, id:%d, count:%d\n", file_ptr - count + 1, i, count);
				arg->in = file_ptr - count + 1;
				arg->len = count;
				// TODO 对齐
				buffer[i]= (char *)malloc((count+200) * sizeof(char));
				// TODO 这块out其实有bug,极端情况的时候文件bufffer会很大
				arg->out = buffer[i];
				arg->line_start = line_start;
				arg->line_end = line_end;
				arg->id = i;
				printf("new thread:count:%d, total:%d, line_start:%d, line_end:%d, line_num:%d, id:%d\n", count, total, line_start, line_end, line_num, i);
				if (pthread_create(&id[i], NULL, brick_process, (void *)arg) < 0) {
					print_err("server pthread create error:", errno);
					return NULL;
				}
				i++;
				count = 0;
				line_num = 0;
			}
		}

		if (total >= st.st_size) {
			printf("end of file, total:%d\n", total);
			break;
		}
		file_ptr++;
	}
	*/

	/*
	for (i = 0; i < 10 && !is_end; i++) {	
		struct work_data* arg = (struct work_data *)malloc(sizeof(struct work_data));
		arg->in = file_start + total1;
		if ((total1 + num) >= st.st_size) {
			printf("i:%d, filesize;%d\n", i, st.st_size);
			count = st.st_size - total1;
			is_end = true;
		} else {
			count = num;
		}

		total1 += count;

		if (!is_end) {
			// 结尾是完整的一行
			while (*(file_start+total1) != '\r' && *(file_start+total1) != '\n') {
				// printf("xxxxx%c\n", *(arg.in + count));
				total1++;
				count++;
			}
		}

		printf("i:%d, count:%d, total:%d\n", i, count, total1);

		arg->out = (char *)malloc((count+200) * sizeof(char));
		arg->len = count;
		arg->id = i;
		if (pthread_create(&id[i], NULL, brick_process, (void *)arg) < 0) {
			print_err("server pthread create error:", errno);
			return NULL;
		}
	}
	*/

	pthread_join(pid, NULL);

	/*
		*/

    //brick_process((char *)mmapfile, (char *)buf);
    //printf("brickProcess end\n");
    // exit(1);
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
	int block_id = nc->id;
	// 按照线程数进行分割(每个线程数需要发送的数据行数)
	int line_count = (block_id <= THREAD_NUM) ? line_total / THREAD_NUM : line_total - (line_total / THREAD_NUM) * (THREAD_NUM - 1);
	int line_start = (block_id - 1) * (line_total / THREAD_NUM);
	// 总共发送的数据大小 
	int byte_total = (line_start > 0) ? (md[line_start+line_count-1].end_pos - md[line_start-1].end_pos) : (md[line_start+line_count-1].end_pos+1);
	// 开发发送的数据位置
	char *data = (line_start > 0) ? (file_buf + md[line_start-1].end_pos+1) : file_buf; 
	int line = 0;
	int request_num;
	int length;

    char writebuf[16];
    char readbuf[1024];
	// 先发送块数据的元信息
	sprintf(writebuf, "%d", block_id);
	length_encode(line_start, writebuf+1);
	length_encode(line_count, writebuf+5);
	length_encode(byte_total, writebuf+9);
	printf("line_start:%d, line_count:%d, byte_total%d\n", line_start, line_count, byte_total);
	if (writen(connfd, writebuf, 13) < 0) {
		print_err("server write error:", errno);
		close(connfd);
		return NULL;
	}

	// line_count = 16;
    for ( ;line < line_count; ) {
		if ((request_num = read(connfd, readbuf, 1024)) <= 0) {
			print_err("server read error:", errno);
			close(connfd);
			return NULL;
		}
        // printf("request num:%d\n", request_num);
        // printf("server receive: %s\n", readbuf);
		//nr = readline(data, writebuf+4, line+line_start); 
		// printf("line length:%d\n", n);
		// printf("l:%d, pos:%d, brick:%s",l, pos, brick);
		length = (line > 0) ? (md[line+request_num-1].end_pos - md[line-1].end_pos) : (md[line+request_num-1].end_pos + 1);
		// printf("length:%d\n", length);
		// length = md[line+request_num-1].end_pos - byte_write + 1;
		length_encode(length, writebuf);
		if (writen(connfd, writebuf, 4) <= 0) {
			print_err("server write length error:", errno);
			close(connfd);
			return NULL;
		}

		// printf("server send:%d, %d, %d, %d, data:%s", (int)*writebuf, (unsigned char)*(writebuf+1), (unsigned char)*(writebuf+2),(unsigned char)*(writebuf+3), writebuf+4);
		// TODO 数据的压缩
		if (writen(connfd, data, length) <= 0) {
			print_err("server write file data error:", errno);
			close(connfd);
			return NULL;
		}
		// byte_write += length;
		data += length;
		line += request_num;	
		// printf("send line num:%d\n", line);
    }

	printf("block transmit finished\n");
	//TODO shutdown只关闭写不关闭读
	// shutdown(connfd, SHUT_WR);
	close(nc->connfd);
}

int main(void) {
    struct sockaddr_in sa, ca;
    int listensock;
    socklen_t addrlen;
    int connfd;
	
	load_file();
        
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
	int id = 1;
	pthread_t pid;
	while (1) {
		connfd = accept(listensock, (struct sockaddr *)&ca, &addrlen);
		if (connfd < 0) {
			print_err("server accpet error:", errno);
			return RETURN_FAILURE;
		}

		struct netConn *nc = (struct netConn*) malloc(sizeof(struct netConn));
		nc->id = id;
		nc->connfd = connfd;
		if (pthread_create(&pid, NULL, send_brick, (void *)nc) < 0) {
			print_err("create new send brick errror:", errno);
			return RETURN_FAILURE;
		}
		id++;
	}

    /*
    if (fcntl(connfd, F_SETFL, fcntl(connfd, F_GETFL) | O_NONBLOCK) < 0) {
        print_err("server set fd noblocking error:", errno);
        return RETURN_FAILURE;
    }
    */

    printf("file transmit end\n");

    return 0;
}
