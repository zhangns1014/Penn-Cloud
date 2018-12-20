#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <unistd.h>
#include <cstring>
#include <string>
#include <arpa/inet.h>
#include <errno.h>
#include <set>
#include <pthread.h>
#include <map>
#include <vector>
#include <tuple>
#include <fstream>
#include <iterator>
#include <algorithm>
#include <semaphore.h>

using namespace std;

struct thread_pass_in {
	int comm_fd;
	int frontend_ind;
};

struct frontendserver{
	pair<sockaddr_in, sockaddr_in> frontend_addr;
	pair<string, string> str_addr;
	bool frontend_on = false;
	int working_threads_num = 0;
	pthread_t threadid;
	int comm_fd;
};

struct http_content{
	string first_line;
	map<string, string> header;
	char * content;
	long int size = 0;
};

const string http = "HTTP/1.1";
const string redirect301 = " 301 Moved Permanently";
const string redirect302 = " 302 Found";
const string redirect303 = " 303 See Other";
const string redirect307 = " 307 Temporary Redirect";
const string redirect308 = " 308 Permanent Redirect";

map<int, string> redirect_map = { { 301, redirect301 }, { 302, redirect302 }, { 303,
		redirect303 }, { 307, redirect307 }, { 308, redirect308 } };

const string master_addr = "127.0.0.1:8888";
const string master_addr_out = "127.0.0.1:8889";
const string admin_addr = "127.0.0.1:9000";
const string frontend_file = "frtrep.txt";
const string err_msg = "err\r\n";
const int frontend_num = 4;
vector<frontendserver> frontend;


bool verbose = false;


bool do_write(int fd, const char *buf, int len) {
	int sent = 0;
	while (sent < len) {
		int n = write(fd, &buf[sent],len-sent);
		if (n<0)
			return false;
		sent += n;
	}
	return true;
}

bool response2client(int comm_fd, http_content* msg) {
	char buf[1000] = { 0 };
	// write first line
	strcpy(buf, (msg->first_line + "\r\n").c_str());
	if (!do_write(comm_fd, buf, strlen(buf))) return false;
	// write header
	for (map<string,string>::iterator it=msg->header.begin(); it!=msg->header.end(); it++) {
		strcpy(buf, (it->first + ": " + it->second + "\r\n").c_str());
		if (!do_write(comm_fd, buf, strlen(buf))) return false;
	}
	strcpy(buf, "\r\n");
	if (!do_write(comm_fd, buf, strlen(buf))) return false;
	// write content
	if (!do_write(comm_fd, msg->content, msg->size)) return false;
	return true;
}

bool response_redirect(int comm_fd, string url, int redirect) {
	http_content msg;
	msg.size = 0;
	msg.first_line = http + redirect_map[redirect];
	msg.header.insert(pair<string,string>("Location", url));
	msg.header.insert(pair<string,string>("Content-Type", "text/html"));
	msg.header.insert(pair<string,string>("Content-Length", to_string(msg.size)));
	return response2client(comm_fd, &msg);
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

void parse_rep_file(string filename, vector<pair<sockaddr_in,sockaddr_in>> & rep_addrs,
		vector<pair<string, string>> &address){
	ifstream rep_file(filename);
	string line;
	if(rep_file.is_open()){
		while(getline(rep_file, line)){
			vector<int> semi_pos = check_punc(line.c_str(), line.size(), ';', 1);
			string addr_1_s = line.substr(0, semi_pos[0]);
			string addr_2_s = line.substr(semi_pos[0]+1);
			address.push_back(make_pair(addr_1_s, addr_2_s));
			rep_addrs.push_back(make_pair(parse_addr(addr_1_s.c_str(), addr_1_s.size()),
					parse_addr(addr_2_s.c_str(), addr_2_s.size())));
		}
	}else fprintf(stderr, "Cannot open rep file.\n");
	rep_file.close();
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

int find_frontend(sockaddr_in addr) {
	int result = -1;
	for (vector<frontendserver>::const_iterator
			i=frontend.begin(); i != frontend.end(); i++) {
		if (i->frontend_addr.second.sin_addr.s_addr == addr.sin_addr.s_addr &&
				i->frontend_addr.second.sin_port == addr.sin_port) {
			result = i-frontend.begin();
			return result;
		}
	}
	return result;
}

int assign_new_user(vector<frontendserver> frontend) {
	int result = -1; int min_load = 10000;
	for (vector<frontendserver>::const_iterator i = frontend.begin(); i != frontend.end(); i++) {
		if (i->frontend_on) {
			if (i->working_threads_num < min_load){
				min_load = i->working_threads_num;
				result = i - frontend.begin();
			}
		}
	}
	return result;
}

void* admin_worker(void *arg){
	int comm_fd = *(int*)arg;
	FILE * fileopen = fdopen(comm_fd, "r+");
	if(verbose){
		fprintf(stdout, "[%d] Balancer: new connection to administration server\n", comm_fd);
	}
	// do write ever 5s
	while (true) {
		//cout << "send to administer..."<< endl;
		for (int i = 0; i<frontend.size(); i++) {
			if (fprintf(fileopen, "%d %d %d/", i, frontend[i].frontend_on, frontend[i].working_threads_num) < 0) break;
		}
		if (fprintf(fileopen, "\n")<0) break;
		sleep(2);
	}
	fclose(fileopen);
	if(verbose){
		fprintf(stdout, "[%d] Balancer: connection to administration server failed\n", comm_fd);
	}
	pthread_detach(pthread_self());
	pthread_exit(NULL);
}

void* thread_worker_client(void *arg){
	int comm_fd = *(int*)arg;
	FILE * fileopen = fdopen(comm_fd, "r+");
	if(verbose){
		fprintf(stdout, "[%d] Balancer: new connection to client\n", comm_fd);
	}
	char buf[1000];
	while(1){
		if (fgets(buf, 1000, fileopen) == NULL) break;
		cout <<"from client: "<< buf;
		if (strncmp(buf, "GET ", 4) == 0) { // see a GET command
			vector<string> elements = split_string(string(buf), " ");
			int redirect_ind = assign_new_user(frontend);
			string url = "http://"+frontend[redirect_ind].str_addr.first+elements[1];
			cout << "redirect to: " <<url<<endl;
			response_redirect(comm_fd, url, 302);\
		}
	}
	fclose(fileopen);
	pthread_detach(pthread_self());
	pthread_exit(NULL);
}

void* thread_worker_frontend(void *arg){
	thread_pass_in pass_in = *(thread_pass_in*)arg;
	int comm_fd = pass_in.comm_fd;
	int ind = pass_in.frontend_ind;
	frontend[ind].threadid = pthread_self();
	frontend[ind].comm_fd = comm_fd;
	frontend[ind].frontend_on = true;
	FILE * fileopen = fdopen(comm_fd, "r+");

	if(verbose){
		fprintf(stdout, "[%d] Balancer: new connection to frontend\n", comm_fd);
	}

	char buf[1000];

	while(1){
		if (fgets(buf, 1000, fileopen) == NULL) break;
		cout << "from frontend: "<< buf;
		buf[strlen(buf)-2] = 0; // remove \r\n
		frontend[ind].working_threads_num = stoi(buf);
	}

	// exit
	fclose(fileopen);
	frontend[ind].frontend_on = false;
	frontend[ind].working_threads_num = 0;
	if(verbose){
		fprintf(stdout, "[%d] Balancer: thread exits\n", comm_fd);
	}
	pthread_detach(pthread_self());
	pthread_exit(NULL);
}


int main(int argc, char *argv[]){
	int opt = -1;
	while((opt = getopt(argc, argv, "v")) != -1){
		switch(opt){
		case('v'):
			verbose = true;
			break;
		case('?'):
			fprintf(stderr, "unknown option: -%c\n", optopt);
		exit(1);
		}
	}
	fprintf(stdout, "********************Initializing*********************\n");

	struct sockaddr_in admin_dest = parse_addr(admin_addr.c_str(), admin_addr.length());
	struct sockaddr_in my_out = parse_addr(master_addr_out.c_str(), master_addr_out.length());
	int admin_sock = socket(PF_INET, SOCK_STREAM, 0);
	int option = 1;
	setsockopt(admin_sock, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
	bind(admin_sock, (struct sockaddr*)&my_out, sizeof(my_out));
	if (connect(admin_sock, (struct sockaddr*) &admin_dest, sizeof(admin_dest))<0) {
		close(admin_sock);
		fprintf(stderr, "Cannot connect to administer node\n");
	}
	else {
		pthread_t master_thread;
		pthread_create(&master_thread, NULL, admin_worker, (void*)&admin_sock);
	}

	// read the directory to get all frontend addresses
	vector<pair<sockaddr_in, sockaddr_in>> rep_addrs;
	vector<pair<string, string>> str_addrs;
	parse_rep_file(frontend_file, rep_addrs, str_addrs);
	for (int j = 0; j < rep_addrs.size(); j++) {
		frontendserver b;
		b.frontend_addr = rep_addrs[j];
		b.str_addr = str_addrs[j];
		frontend.push_back(b);
	}

	int socket_fd = socket(PF_INET, SOCK_STREAM, 0);
	if (socket_fd<0) printf("socket() failed\n");
	int option_value = 1;
	if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, (char*)&option_value, sizeof(int)) < 0)
		fprintf(stderr,"setsockopt(SO_REUSEADDR) failed\n");
	sockaddr_in my_addr = parse_addr(master_addr.c_str(), master_addr.length());
	bind(socket_fd, (struct sockaddr*)&my_addr, sizeof(my_addr));
	listen(socket_fd, 1000);

	fprintf(stdout, "************************Done*************************\n");
	while(true){
		struct sockaddr_in frontend_addr;
		socklen_t frontend_add_rlen = sizeof(frontend_addr);
		int *p_comm_fd = (int*)malloc(sizeof(int)); // attention: memory leak
		*p_comm_fd = accept(socket_fd, (struct sockaddr*)&frontend_addr, &frontend_add_rlen);
		cout<< "new connection"<<endl;
		int frontend_ind = find_frontend(frontend_addr);
		pthread_t thread;
		if (frontend_ind<0) { // client connection
			cout<< "new client" <<endl;
			pthread_create(&thread, NULL, thread_worker_client, (void*)p_comm_fd);
		}
		else { // frontend connection
			cout<< "new frontend" <<endl;
			thread_pass_in pass_in;
			pass_in.frontend_ind = frontend_ind;
			pass_in.comm_fd = *p_comm_fd;
			pthread_create(&thread, NULL, thread_worker_frontend, (void*)&pass_in);
		}
	}

	return 0;
}

