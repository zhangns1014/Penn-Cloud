#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <set>
#include <errno.h>
#include <getopt.h>
#include <pthread.h>
#include <signal.h>
#include <fstream>
#include <iostream>
#include <cstdbool>
#include <sys/types.h>
#include <libgen.h>
#include <fcntl.h>
#include <ctime>
#include <cstring>
#include <vector>
#include <map>
#include <semaphore.h>
#include <openssl/md5.h>
#include <sys/file.h>
#include <algorithm>
//#include "Request_handler.h"
//#include "Response_handler.h"

#define MAX_HOLD 100
#define MAX_SIZE 1048576
// Testing
using namespace std;

vector<string>load_number;
vector<string>master_number;
string self_serv="127.0.0.1:9000";

int loadbalancer=8889;
int masternode=5556;
sem_t mutex;
struct package{
	int fd;
	int client_port;
};

string content;
void get_file_content(FILE * file_open, char* content, long int & size) {
	rewind(file_open);
	fread(content, 1, size, file_open);
	fclose(file_open);
}

long int get_file_size(FILE * file_open) {
	rewind(file_open);
	fseek(file_open, 0, SEEK_END);
	return ftell(file_open);
}

vector<int> check_punc(const char *s, int len, char punc, int first_n){
	vector<int> pos;
	for(int i=0; i<len; i++){
		if(s[i] == punc){
			pos.push_back(i);
			if(pos.size() == first_n){
				return pos;
			}
		}
	}
	return pos;
}



vector<string> split_string(string line, string delim) {
	vector < string > after_split;
	int prev = 0, end = line.length();
	while (prev < line.length() && end <= line.length()) {
		size_t end = line.find(delim, prev);
		if (end == string::npos) {
			end = line.length();
		}
		after_split.push_back(line.substr(prev, end - prev));
		prev = end + delim.length();
	}
	return after_split;
}

long int readlength(FILE *stream){
	int i;
	char cstr[100]={0};
	for (i = 0; ; i++) {
		int reading = fgetc(stream);
		if (reading == EOF) return -1; // no connection
		if ((char) reading == ' ') break; // end of size
		if (i >= 100-1) return 0; // size too large
		cstr[i] = (char)reading;
	}
	cstr[i] = 0;
	return stol(cstr);
}

sockaddr_in parse_addr(const char *s, int len){
	char IP[20];
	char port[10];
	vector<int> colon_pos = check_punc(s, len, ':', 1);
	strncpy(IP, s, colon_pos[0]);
	IP[colon_pos[0]] = '\0';
	strcpy(port, s+colon_pos[0]+1);
	struct sockaddr_in addr;
	bzero(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(atoi(port));
	inet_pton(AF_INET, IP, &(addr.sin_addr));
	return addr;
}

//bool do_read(int fd, char *buf) {
//	int rlen = 0;
//	while (n < rlen) {
//		int n = read(fd, &buf[n], rlen - n);
//		if (n < 0)
//			return false;
//		rlen += n;
//	}
//	return true;
//}

string change_template(char *content){
	string content_temp=string(content);
	if(master_number.size()==0 ){//if no content
		size_t found=content_temp.find("###");
		content_temp.replace(found,3," ");
	}
	else{
		string change_temp;
		string row= " <tr><td>#0</td><td>#1</td><td>#2</td><td>#3</td><td>#4</td></tr>";
		for (int i=0;i<master_number.size();i++){
			string temp=row;
			vector<string>pairs=split_string(master_number[i]," ");
			size_t found=temp.find("#"+to_string(0));
			temp.replace(found,2,pairs[0]);
			for(int j=1;j<5;j++){
				found=temp.find("#"+to_string(j),found+2);
				temp.replace(found,2,pairs[j]);
			}
			change_temp.append(temp);
		}
		size_t found=content_temp.find("###");
		content_temp.replace(found,3,change_temp);

	}

	if(load_number.size()==0 ){//if no content
		size_t found=content_temp.find("***");
		content_temp.replace(found,3," ");
	}
	else{
		string change_temp;
		string row= " <tr><td>#0</td><td>#1</td><td>#2</td></tr>";
		for (int i=0;i<load_number.size();i++){
			string temp=row;
			vector<string>pairs=split_string(load_number[i]," ");
			size_t found=temp.find("#0");
			temp.replace(found,2,pairs[0]);
			found=temp.find("#1",found+2);
			temp.replace(found,2,pairs[1]);
			found=temp.find("#2",found+2);
			temp.replace(found,2,pairs[2]);
			change_temp.append(temp);
		}
		size_t found=content_temp.find("***");
		content_temp.replace(found,3,change_temp);
	}
	return content_temp;
}

string get_time() {
	time_t rawtime;
	struct tm * timeinfo;
	char buffer[50] = { 0 };
	time(&rawtime);
	timeinfo = localtime(&rawtime);
	strftime(buffer, 50, "%a, %e %h %G %T GMT", timeinfo);
	return string(buffer);
}

bool do_write(int fd, const char *buf, long int len) {
	long int sent = 0;
	while (sent < len) {
		long int n = write(fd, &buf[sent], len - sent);
		if (n < 0)
			return false;
		sent += n;
	}
	if (len < 1000) {
		for (long int i = 0; i < len; i++) {
			cout << buf[i];
		}
		cout<<"\r\n"<<endl;
	}
	return true;
}


bool response_ok(int comm_fd, string mes){
	//	HTTP/1.1 200 OK
	//	Date: Mon, 27 Jul 2009 12:28:53 GMT
	//	Server: Apache/2.2.14 (Win32)
	//	Last-Modified: Wed, 22 Jul 2009 19:15:56 GMT
	//	Content-Length: 88

	string temp="";
	temp+="HTTP/1.1 200 OK\r\n";
	temp+="Date:"+ get_time()+"\r\n";
	temp+="Content-Length:"+to_string(mes.length())+"\r\n";
	temp+="Content-Type: text/html\r\n\r\n";
	temp+=mes;
	cout<<temp<<endl;
	if(!do_write(comm_fd,temp.c_str(),temp.length()))
		return false;
	return true;
}


bool response_file(FILE * file_open, int comm_fd) {
	long int size = get_file_size(file_open);
	char content[size+1]={0};
	get_file_content(file_open, content, size);
	string mes=change_template(content);
	cout << mes << endl;
	return response_ok(comm_fd,mes);
}


void *handler_load(void *arg) {
	int comm_fd= *(int *) arg;
	FILE *stream=fdopen(comm_fd,"r");
	char buf[1000]={0};
	while (true) {
		bool flag=(fgets(buf,1000,stream)!=NULL);
		if(!flag)
			fprintf(stderr,"failed to read from socket!");
		cout << "from load balancer: " << buf;
		vector<string>split=split_string(string(buf),"/");
		sem_wait(&mutex);
		load_number.clear();
		for(int i=0;i<split.size();i++){
			load_number.push_back(split[i]);
		}
		sem_post(&mutex);
	}
	fclose(stream);
	pthread_detach((unsigned long int) pthread_self());
	pthread_exit(NULL);
}

void *handler_master(void *arg) {
	int comm_fd= *(int *) arg;
	char buf[1000]={0};
	FILE *stream=fdopen(comm_fd,"r");
	while (true) {

		bool flag=(fgets(buf,1000,stream)!=NULL);
		if(!flag)
			fprintf(stderr,"failed to read from socket!");

		cout << "from master: " << buf;
		vector<string>split=split_string(string(buf),"/");
		sem_wait(&mutex);
		master_number.clear();
		for(int i=0;i<split.size();i++){
			master_number.push_back(split[i]);
		}
		sem_post(&mutex);
	}

	fclose(stream);
	pthread_detach((unsigned long int) pthread_self());
	pthread_exit(NULL);
}

void *handler_http(void *arg) {
	int comm_fd= *(int *) arg;
	char buf[1000]={0};
	FILE *stream=fdopen(comm_fd,"r");
	while (true) {
		if (fgets(buf, 1000, stream) == NULL) break;
		cout <<"from client: "<< buf;
		if (strncmp(buf, "GET ", 4) == 0) { // see a GET command
			sem_wait(&mutex);
			FILE * file_open = fopen("HTML/admin.html","r");
			if(!response_file(file_open,comm_fd))
				fprintf(stderr,"can't get response\r\n");
			sem_post(&mutex);
		}
	}

	fclose(stream);
	pthread_detach((unsigned long int) pthread_self());
	pthread_exit(NULL);
}


int main(int argc, char *argv[]) {
	//signal(SIGINT,ctrl);
	//string a=get_fig_binary();
	//	int PORT;
	//	int c;
	//
	//	while ((c = getopt(argc, argv, "vp:")) != -1) {
	//		switch (c) {
	//		case 'v':
	//			verbose = true;
	//			break;
	//		case 'p':
	//			PORT = atoi(optarg);
	//			break;
	//		case '?':
	//			if (optopt == 'p')
	//				fprintf(stderr, "Option -%c requires an argument.\n", optopt);
	//			else
	//				fprintf(stderr, "Unknown reason");
	//			return 1;
	//
	//		}
	//	}
	/*create a new socket*/
	if (sem_init(&mutex, 0, 1) == -1) {
		fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));
		exit(-1);
	}

	int listen_fd = socket(PF_INET, SOCK_STREAM, 0);
	if (listen_fd == -1) {
		fprintf(stderr, "failed to open a new socket\n");
		exit(-1);
	}

	// set the socket to be reusable
	int enable = 1;
	if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
		fprintf(stderr, "setsockopt: failed: %s\n", strerror(errno));

	// set up the server
	sockaddr_in self_addr=parse_addr(self_serv.c_str(),self_serv.length());
	int bind_not = bind(listen_fd, (struct sockaddr*) &self_addr,
			sizeof(self_addr));
	if (bind_not < 0) {
		fprintf(stderr, "Fail to bind the socket!\n");
		exit(-1);
	}

	if (listen(listen_fd, 100) < 0) {
		fprintf(stderr, "Fail to listen to socket!\n");
		exit(-1);
	}

	while (true) {
		struct sockaddr_in clientaddr;
		socklen_t clientaddrlen = sizeof(clientaddr);
		int comm_fd = accept(listen_fd, (struct sockaddr*) &clientaddr,
				&clientaddrlen);
		if (comm_fd < 0) {
			perror("failed to accept!\n");
			continue;
		}
		int client_port=ntohs(clientaddr.sin_port);
		cout << client_port <<" "<< loadbalancer << " " << masternode << endl;
		if(client_port==loadbalancer)
		{
			cout<<"load balancer in" << endl;
			pthread_t thread;
			pthread_create(&thread, NULL, handler_load, (void *) &comm_fd);

		}
		else if(client_port==masternode){
			cout<<"master node in" << endl;
			pthread_t thread;
			pthread_create(&thread, NULL, handler_master, (void *) &comm_fd);

		}
		else{
			cout<<"new client in" << endl;
			pthread_t thread;
			pthread_create(&thread, NULL, handler_http, (void *) &comm_fd);
		}


		//		socket_all.insert(comm_fd);
	}
	return 0;

}


