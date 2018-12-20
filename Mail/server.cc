#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>
#include <string>
#include <iostream>
#include <pthread.h>
#include <vector>
#include <sys/socket.h>
#include <signal.h>
#include <dirent.h>
#include <set>
#include <fstream>
#include <algorithm>
#include <time.h>
#include <openssl/md5.h>

using namespace std;
int flag;
vector<pthread_t> thread_id;
vector<int> file_des;
pthread_mutex_t mutex;
//string backend_serv;
string MASTER;

const char* greets = "220 localhost service ready\r\n";
const char* ok = "250 OK\r\n";
const char* helo = "250 localhost\r\n";
const char* end_data = "354 end data with <CRLF>.<CRLF>\r\n";
const char* unknown = "502 command not implemented\r\n";
const char* bad_seq = "503 bad sequence of commands\r\n";
const char* goodbye = "221 localhost service disconnect\r\n";
const char* null_path = "501 null\r\n";
const char* invalid = "550 invalid mailbox\r\n";

void rm_bracket(char *output, char *input);

string parse_res(int fd){
	char buf[1000];
	memset(buf, 0, 1000);
	string response;
	char *curr = buf;
		read(fd, curr, 1000-strlen(buf));
		char *ptr = strstr(buf, "\r\n");
		while (ptr != NULL){
			ptr = ptr+2;
			response = string(buf, ptr-buf);
			break;
		}
	return response;
}

string tobackend(string& backend_serv, const char *message){
  int sock = socket(PF_INET, SOCK_STREAM, 0);
  if (sock < 0){
        fprintf(stderr, "Cannot open socket (%s)\n", strerror(errno));
        exit(1);
  }
  int opt = 1;
  setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
  struct sockaddr_in dest;
  bzero(&dest, sizeof(dest));
  dest.sin_family = AF_INET;
  char address[backend_serv.length()+1];
  strcpy(address, backend_serv.c_str());
  char *arg_ip = strtok(address, ":");
  inet_pton(AF_INET, arg_ip, &(dest.sin_addr));
  char *arg_port = strtok(NULL, ":");
  dest.sin_port = htons(atoi(arg_port));
  connect(sock, (struct sockaddr*)&dest, sizeof(dest));
  //write message
  const char* msg = message;
  int sent = 0;
  while (sent<strlen(message)){
                int m = write(sock, &msg[sent], strlen(message)-sent);
                sent += m;
  }
  cout<<"message:"<<message<<endl;
  //read response
  string response = parse_res(sock);
  cout<<"response:"<<response<<endl;
  close(sock);
  return response;
}

string ask_master(string& master_addr, string& user){
        string backend_msg = "PUT "+user+"\r\n";
        string backend_res = tobackend(master_addr, backend_msg.c_str());
	cout<<backend_res<<endl;
        return backend_res;
}

void computeDigest(char *data, int dataLengthBytes, unsigned char *digestBuffer)
{
  /* The digest will be written to digestBuffer, which must be at least MD5_DIGEST_LENGTH bytes long */
  MD5_CTX c;
  MD5_Init(&c);
  MD5_Update(&c, data, dataLengthBytes);
  MD5_Final(digestBuffer, &c);
}

string gethash(string& message){
	        int len = MD5_DIGEST_LENGTH;
                char id[len+1];
                unsigned char code[len];
                char arg[message.length()+1];
                strcpy(arg, message.c_str());
                computeDigest(arg, strlen(arg), code);
                for (int i=0;i<len;i++)
                        sprintf(id+i, "%02x", code[i]);
		return string(id);
}

bool do_write(int fd, char *buf, int* command, string& sender, vector<string>& recipient, string& data){
	char *ptr = strstr(buf, "\r\n");
	while (ptr != NULL){
		int len = ptr-buf+2;
		if (!strncasecmp(buf, "HELO ", 5)){
			if (*command>=2){//bad sequence
				write(fd, bad_seq, strlen(bad_seq));
                        	if (flag == 1){
                                	fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                	fprintf(stderr, "[%d] S: %s", fd, bad_seq);
				}
			}	
			else{//say hello
			write(fd, helo, strlen(helo));
			if (flag == 1){
				fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
				fprintf(stderr, "[%d] S: %s", fd, helo);
			}
			*command = 1;}
		}
		else if (!strncasecmp(buf, "MAIL FROM:", 10)){
			if (*command == 1){
				char send[50];
				rm_bracket(send, buf);
				string addr(send);
				size_t found = addr.find_last_of("@");
				if (addr.compare(found, 10, "@localhost")!=0){//validate the email address of sender
					write(fd, null_path, strlen(null_path));
                                	if (flag == 1){
                                        	fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        	fprintf(stderr, "[%d] S: %s", fd, null_path);
					}
				}
				else {
				sender = addr;
				write(fd, ok, strlen(ok));
                        	if (flag == 1){
					fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                	fprintf(stderr, "[%d] S: %s", fd, ok);
                        	}
				*command = 2;}
			}
			else {//bad sequence
				write(fd, bad_seq, strlen(bad_seq));
                                if (flag == 1){
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        fprintf(stderr, "[%d] S: %s", fd, bad_seq);
                                }
			}
		}
		else if (!strncasecmp(buf, "RCPT TO:", 8)){
			if (*command == 2||*command == 3){
				char recv[50];
				rm_bracket(recv, buf);
				string mbox(recv);
				recipient.push_back(mbox);
				write(fd, ok, strlen(ok));
                        	if (flag == 1){
                                	fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                	fprintf(stderr, "[%d] S: %s", fd, ok);
                        	}
				*command = 3;
			}
			else {//bad sequence
				write(fd, bad_seq, strlen(bad_seq));
                                if (flag == 1){
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        fprintf(stderr, "[%d] S: %s", fd, bad_seq);
                                }
			}
                }
		else if ((!strncasecmp(buf, "DATA", 4)) || *command == 4){
			if (*command != 3 && *command != 4){//bad sequence
				write(fd, bad_seq, strlen(bad_seq));
                                if (flag == 1){
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        fprintf(stderr, "[%d] S: %s", fd, bad_seq);
                                }
			}
			else if (*command == 3){
				write(fd, end_data, strlen(end_data));
                                if (flag == 1){
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        fprintf(stderr, "[%d] S: %s", fd, end_data);
                                }
				*command = 4;
			}
			else if (strcmp(buf, ".\r\n") != 0){//store the content of email
				string row(buf, len);
				data = data+row;
				if (flag == 1)
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
			}
			else {
				time_t now = time(0);
				string time = ctime(&now);
				time.pop_back();
				time = time + "\r\n";
				size_t f1 = data.find("Subject: ");//find subject
				size_t f2 = data.find("\r\n", f1+1);
				string subject = data.substr(f1+9, f2-f1-9);
				
				string hash = gethash(data);//generate hash
				string backend_msg;
				string backend_res;

				size_t found = sender.find_first_of("@");//find sender
				string user_s = sender.substr(0, found);
				string node_s = ask_master(MASTER, user_s);//ask for backend node
				
				//string head = time+subject+"\n"+sender+"\n";        
				backend_msg = "PUT("+user_s+",/mailbox/sent/"+hash+","+data+")\r\n";
                                //backend_msg = "GET(zives,mail1)\r\n";
				string value1 = data;
				int size_msg = value1.size();
                                //int size_msg = backend_msg.size();
				char send_msg[5000];
                                sprintf(send_msg, "%d -1 %s", size_msg, backend_msg.c_str());   
                                cout<<send_msg<<endl;
				backend_res = tobackend(node_s, send_msg);
				
				for (auto it:recipient){
					size_t found = it.find_first_of("@");
					string user = it.substr(0, found);
					//string head = time+subject+"\n"+sender+"\n";
					string node = ask_master(MASTER, user);
					
					backend_msg = "PUT("+user+",/mailbox/rcpt/"+hash+","+data+")\r\n";
					value1 = data;
					int size_msg = value1.size();
					char send_msg[5000];
					sprintf(send_msg, "%d -1 %s", size_msg, backend_msg.c_str());
					cout<<send_msg<<endl;
					backend_res = tobackend(node, send_msg);
				}

				write(fd, ok, strlen(ok));
                                if (flag == 1){
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        fprintf(stderr, "[%d] S: %s", fd, ok);
                                }
				sender.clear();
				recipient.clear();
				data.clear();	
				*command = 1;
			}	
                }
		else if (!strncasecmp(buf, "QUIT", 4)){
			write(fd, goodbye, strlen(goodbye));
			if (flag == 1){
                                fprintf(stderr, "[%d] C: QUIT\r\n", fd);
                                fprintf(stderr, "[%d] S: %s", fd, goodbye);
                        }
			return 1;
		}
		else if (!strncasecmp(buf, "RSET", 4)){
			if (*command == 0){
				write(fd, bad_seq, strlen(bad_seq));
                                if (flag == 1){
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        fprintf(stderr, "[%d] S: %s", fd, bad_seq);
                                }
			}
			else {
				write(fd, ok, strlen(ok));
                                if (flag == 1){
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        fprintf(stderr, "[%d] S: %s", fd, ok);
                                }
                                sender.clear();
                                recipient.clear();
                                data.clear();
				*command = 1;
			}
		}
		else if (!strncasecmp(buf, "NOOP", 4)){
			write(fd, ok, strlen(ok));
                                if (flag == 1){
                                        fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                        fprintf(stderr, "[%d] S: %s", fd, ok);
                                }
		}
		else {
			write(fd, unknown, strlen(unknown));
			if (flag == 1){
                                fprintf(stderr, "[%d] C: %.*s", fd, (int)len, buf);
                                fprintf(stderr, "[%d] S: %s", fd, unknown);
                        }
		}
		//for (int i=0;i<len;i++) 
                //      buf[i] = buf[i+len];
		char* curr = buf;
		ptr = ptr+2;
		while (*ptr != '\0') {
			*curr++ = *ptr;
			*ptr++ = '\0';
		}
		while (*curr != '\0')
			*curr++ = '\0';
		ptr = strstr(buf, "\r\n");
	}
        return 0;
}

bool do_read(int fd, char *buf, int len){
        char *ptr = strstr(buf, "\r\n");
        while(ptr == NULL){ 
                int n = read(fd, &buf[len], 1000-len);
                if (n<0)
                        return 0;
                len +=n;
		ptr = strstr(buf, "\r\n");
        }
        return 1;
}

void sig_handler(int signo){
	char message[] = "-ERR Server shutting down\r\n";
	for (int i=0;i<thread_id.size();i++){
		write(file_des[i+1], message, strlen(message));
		close(file_des[i+1]);
		pthread_kill(thread_id[i], 0);
	}
	close(file_des[0]);
	//delete directory;
	exit(1);
}

void *worker(void *arg){
	int comm_fd = *(int*)arg;
	char buf[1000] = "";
	int command = 0;
	vector<string> recipient;
	string sender;
	string data;
	signal(SIGINT, sig_handler);
	while (1){
		bool read_flag = do_read(comm_fd, buf, strlen(buf));
		bool write_flag;
		if (read_flag == 1)
			write_flag = do_write(comm_fd, buf, &command, sender, recipient, data);
		if (write_flag == 1)
			break;
	}
	if (flag == 1)
		fprintf(stderr, "[%d] Connection closed\r\n", comm_fd);
	close(comm_fd);
	pthread_exit(NULL);
}

void rm_bracket(char *output, char *input){
	int i = 0, j = 0;
	while(input[i] != '<')
		i++;
	i++;
	while(input[i] != '>')
		output[j++] = input[i++];
	output[j] = '\0';
}

int main(int argc, char *argv[]){
    flag = 0;
    char ch;
    unsigned short SERV_PORT = 2500;
    while((ch = getopt(argc, argv, "p:av")) != EOF){
        switch(ch){
		case 'p':
			SERV_PORT = atoi(optarg);
			break;
		case 'v':
                        flag = 1;
                        break;
                case 'a':
                        fprintf(stderr, "Author: Mengyu Lu / mengyulu\r\n");
                        exit(0);
	}
    }
    //argc -= optind;
    //argv += optind;
    //backend_serv = string(argv[optind]);
    MASTER = string(argv[optind]);

    signal(SIGINT, sig_handler);
    pthread_mutex_init(&mutex, NULL);
    int listen_fd = socket(PF_INET, SOCK_STREAM, 0);
    file_des.push_back(listen_fd);
    int opt = 1;
    setsockopt(listen_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

    struct sockaddr_in servaddr;
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htons(INADDR_ANY);
    servaddr.sin_port = htons(SERV_PORT);
    bind(listen_fd, (struct sockaddr*)&servaddr, sizeof(servaddr));
    listen(listen_fd, 100);
    while (1) {
    	struct sockaddr_in clientaddr;
	socklen_t clientaddrlen = sizeof(clientaddr);
	int *fd = (int*)malloc(sizeof(int));
	*fd = accept(listen_fd, (struct sockaddr*)&clientaddr, &clientaddrlen);        
	file_des.push_back(*fd);
	//char greets[] = "+OK Server ready (Author: Mengyu Lu / mengyulu)\r\n";
	write(*fd, greets, strlen(greets));
	if (flag == 1){
		fprintf(stderr, "[%d] New connection\r\n", *fd);
		fprintf(stderr, "[%d] S: %s", *fd, greets);
	}
	pthread_t thread;
	pthread_create(&thread, NULL, worker, fd);
	thread_id.push_back(thread);
    }
}
