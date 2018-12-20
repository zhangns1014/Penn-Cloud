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
	pair<int,int> backend_ind;
};

struct backendserver{
	pair<sockaddr_in, sockaddr_in> backend_addr;
	pair<string, string> str_addr;
	bool backend_on = false;
	int working_threads_num = 0;
	pthread_t threadid;
	int comm_fd;
	sem_t mutex;
};

const string master_addr = "127.0.0.1:5555";
const string master_addr_out = "127.0.0.1:5556";
const string admin_addr = "127.0.0.1:9000";
const string username_file = "username.txt";
const string primary_msg = "P\r\n";
const string nprimary_msg = "N\r\n";
const string err_msg = "err\r\n";
const int backend_num = 2;
vector<vector<backendserver>> backend;
vector<int> backend_primary;
map<string, int> username_map; // map the username to backend server

sem_t mutex; // for modifying primary node
sem_t username_mutex; // for modifying username file


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

void parse_rep_file(string filename, vector<sockaddr_in> & rep_addrs, vector<string> &address){
	rep_addrs.clear();
	ifstream file0(filename);
	string line;
	if(file0.is_open()){
		while(getline(file0, line)){
			address.push_back(line);
			rep_addrs.push_back(parse_addr(line.c_str(), line.size()));
		}
	}else fprintf(stderr, "Cannot open file %s.\n", filename.c_str());
	file0.close();
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


void update_user_from_line(string line, map<string, int> &username_map) {
	if (line.compare("") == 0) return;
	if (line.back() == '\n') line.pop_back();
	if (line.back() == '\r') line.pop_back();
	int found = line.find(' ', 0);
	if (found == string::npos) return;
	string fst = line.substr(0, found);
	string snd = line.substr(found+1);
	username_map[fst] = stoi(snd);
}

void update_user_file(string filename, map<string, int> username_map) {
	string tempfile = filename+".temp";
	FILE * temp = fopen(tempfile.c_str(), "w");
	for (map<string, int>::const_iterator i=username_map.begin(); i!=username_map.end(); i++) {
		fprintf(temp, "%s %d\n", i->first.c_str(), i->second);
	}
	fclose(temp);
	if(remove(filename.c_str()) == 0){
		rename(tempfile.c_str(), filename.c_str());
	}else fprintf(stderr, "Master fails to delete old files.\n");

}

pair<int, int> find_backend(sockaddr_in addr) {
	pair<int, int> result = make_pair(-1, -1);
	for (vector<vector<backendserver>>::const_iterator
			i=backend.begin(); i != backend.end(); i++) {
		for (vector<backendserver>::const_iterator j=i->begin(); j != i->end(); j++) {
			if (j->backend_addr.second.sin_addr.s_addr == addr.sin_addr.s_addr &&
					j->backend_addr.second.sin_port == addr.sin_port) {
				result = make_pair(i-backend.begin(), j-i->begin());
				return result;
			}
		}
	}
	return result;
}

int find_available_server(vector<backendserver> servers) {
	for (int i=0; i<servers.size(); i++) {
		if (servers[i].backend_on) return i;
	}
	return -1;
}

int assign_new_user(map <string, int>username_map) {
	int count[backend_num] ={0};
	for (map <string, int>::const_iterator i = username_map.begin(); i != username_map.end(); i++) {
		count[i->second]++;
	}
	return min_element(count,count+backend_num) - count;
}

void* admin_worker(void *arg){
	int comm_fd = *(int*)arg;
	FILE * fileopen = fdopen(comm_fd, "r+");
	if(verbose){
		fprintf(stdout, "[%d] Master: new connection to administration server\n", comm_fd);
	}
	// do write ever 5s
	while (true) {
		//cout << "send to administer..."<< endl;
		for (int i = 0; i<backend.size(); i++) {
			for (int j = 0; j < backend[i].size(); j++) {
				if (fprintf(fileopen, "%d %d %d %d %d/", i, j, backend[i][j].backend_on,
						backend[i][j].working_threads_num, (backend_primary[i]==j)) < 0) break;
			}

		}
		if (fprintf(fileopen, "\n")<0) break;
		sleep(2);
	}
	fclose(fileopen);
	if(verbose){
		fprintf(stdout, "[%d] Master: connection to administration server failed\n", comm_fd);
	}
	pthread_detach(pthread_self());
	pthread_exit(NULL);
}

void* thread_worker_frontend(void *arg){
	int comm_fd = *(int*)arg;
	FILE * fileopen = fdopen(comm_fd, "r+");
	if(verbose){
		fprintf(stdout, "[%d] master: new connection to frontend\n", comm_fd);
	}
	char buf[1000];
	while(1){
		if (fgets(buf, 1000, fileopen) == NULL) break;
		cout << "from frontend: "<<buf;
		buf[strlen(buf)-2] = 0; // remove \r\n
		string cmd = string(buf, 4);
		string username = string(buf+4);
		if (strcasecmp(cmd.c_str(), "CPU ") == 0 || strcasecmp(cmd.c_str(), "DEL ") == 0 ||
				strcasecmp(cmd.c_str(), "GET ") == 0 || strcasecmp(cmd.c_str(), "PUT ") == 0) {
			if (username_map.find(username) == username_map.end()) {
				if (strcasecmp(cmd.c_str(), "PUT ") == 0) { // put a new user
					int backend_ind = assign_new_user(username_map);
					sem_wait(&username_mutex);
					username_map[username] = backend_ind;
					update_user_file(username_file, username_map);
					sem_post(&username_mutex);
				}
				else {
					if (!do_write(comm_fd, err_msg.c_str(), err_msg.length())) break;
					continue;
				}
			}
			int ind = username_map[username];
			sem_wait(&mutex);
			int primary = backend_primary[ind];
			sem_post(&mutex);
			if (primary >= 0) {
				string msg = backend[ind][primary].str_addr.first + "\r\n";
				cout << "To frontend: " << msg;
				if (!do_write(comm_fd, msg.c_str(), msg.length()))
					break;
			}
			else { // not able to find a primary node
				if (!do_write(comm_fd, err_msg.c_str(), err_msg.length()))
					break;
			}
		}
	}
	fclose(fileopen);
	if(verbose){
		fprintf(stdout, "[%d] master: thread exits\n", comm_fd);
	}
	pthread_detach(pthread_self());
	pthread_exit(NULL);
}

void* thread_worker_backend(void *arg){
	thread_pass_in pass_in = *(thread_pass_in*)arg;
	int comm_fd = pass_in.comm_fd;
	int i = pass_in.backend_ind.first;
	int j = pass_in.backend_ind.second;
	backend[i][j].threadid = pthread_self();
	backend[i][j].comm_fd = comm_fd;
	backend[i][j].backend_on = true;
	FILE * fileopen = fdopen(comm_fd, "r+");

	if(verbose){
		fprintf(stdout, "[%d] master: new connection to backend\n", comm_fd);
	}
	sem_wait(&mutex);
	if (backend_primary[i] == -1) { // no primary node is assigned
		sem_wait(&backend[i][j].mutex);
		if (do_write(comm_fd, primary_msg.c_str(), primary_msg.length()))
			backend_primary[i] = j; // set this backend server as primary node;
		sem_post(&backend[i][j].mutex);
		sem_post(&mutex);
	}
	else { // don't assign primary node, and send the primary node address
		string msg = "N " + backend[i][backend_primary[i]].str_addr.first + "\r\n";
		do_write(comm_fd, msg.c_str(), msg.length());
		sem_post(&mutex);
	}

	char buf[1000];

	while(1){
		if (fgets(buf, 1000, fileopen) == NULL) break;
		cout <<"from backend: "<< buf;
		buf[strlen(buf)-2] = 0; // remove \r\n
		backend[i][j].working_threads_num = stoi(buf);
	}

	// exit
	fclose(fileopen);
	backend[i][j].backend_on = false;
	backend[i][j].working_threads_num = 0;
	sem_wait(&mutex);
	if (backend_primary[i] == j || backend_primary[i] == -1) {
		// primary node is off
		backend_primary[i] = -1;
		int new_primary = find_available_server(backend[i]);
		if (new_primary == -1) fprintf(stderr, "Not able to find a new primary node\n");
		else {
			sem_wait(&backend[i][new_primary].mutex);
			if (do_write(backend[i][new_primary].comm_fd, primary_msg.c_str(), primary_msg.length()))
				backend_primary[i] = new_primary;;
			sem_post(&backend[i][new_primary].mutex);
		}
		sem_post(&mutex);
	}
	else {
		sem_post(&mutex);
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
	if (sem_init(&mutex, 0, 1) == -1) {
		fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));
		exit(-1);
	}
	if (sem_init(&username_mutex, 0, 1) == -1) {
		fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));
		exit(-1);
	}

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


	// read the directory to get all backend addresses
	for (int i = 0; i < backend_num; i++) {
		string filename = "Backend_" +to_string(i) + "/addrs.txt";
		vector<pair<sockaddr_in, sockaddr_in>>  rep_addrs;
		vector<pair<string, string>>  str_add;

		parse_rep_file(filename, rep_addrs, str_add);
		vector <backendserver> b_collect;
		for (int j = 0; j < rep_addrs.size(); j++) {
			backendserver b;
			b.backend_addr = rep_addrs[j];
			b.str_addr = str_add[j];
			if (sem_init(&b.mutex, 0, 1)<0) fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));;
			b_collect.push_back(b);
		}
		backend.push_back(b_collect);
		backend_primary.push_back(-1);
	}


	FILE* userfile = fopen(username_file.c_str(), "r");
	char buf[100];
	while (fgets(buf, 100, userfile) != NULL) {
		update_user_from_line(string(buf), username_map);
	}
	fclose(userfile);


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
		struct sockaddr_in backend_addr;
		socklen_t backend_add_rlen = sizeof(backend_addr);
		int *p_comm_fd = (int*)malloc(sizeof(int)); // attention: memory leak
		*p_comm_fd = accept(socket_fd, (struct sockaddr*)&backend_addr, &backend_add_rlen);
		pair<int, int> backend_ind = find_backend(backend_addr);
		pthread_t thread;
		if (backend_ind.first<0 || backend_ind.second<0) { // frontend connection
			cout<< "new frontend" <<endl;
			pthread_create(&thread, NULL, thread_worker_frontend, (void*)p_comm_fd);
		}
		else { // backend connection
			cout<< "new backend " << backend_ind.first <<backend_ind.second<<endl;
			thread_pass_in pass_in;
			pass_in.backend_ind = backend_ind;
			pass_in.comm_fd = *p_comm_fd;
			pthread_create(&thread, NULL, thread_worker_backend, (void*)&pass_in);
		}
	}

	return 0;
}

