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
#include <sys/stat.h>
#include <fcntl.h>

#define MAX_BUF 11000000
#define DUMMY_FD 3131
#define UPDATE_THRESHOLD 100

using namespace std;

struct email{
	string ts, ts_order, subj, sender, colomn;
	vector<string> re;
	email(string _ts, string _ts_order, string _subj, string _sender, vector<string> _re, string _colomn){
		ts = _ts;
		ts_order = _ts_order;
		subj = _subj;
		sender = _sender;
		re = _re;
		colomn = _colomn;
	}
};

bool email_comp(email mail_1, email mail_2){
	if(mail_1.ts_order > mail_1.ts_order){
		return true;
	}else{
		return false;
	}
}

bool file_comp(string file_1, string file_2){
	if(file_1 < file_2){
		return true;
	}else{
		return false;
	}
}

const char ok[] = "OK\r\n";
const char err[] = "ERR\r\n";
const string master_ip = "127.0.0.1:5555";

bool verbose = false;
map<string,pair<long,long>> user_pos;
set<int*> p_comm_fds;
int master_sock;
map<string,map<string,pair<long,char*>>> bigtable;
string pos_filename;
string data_filename;
string log_filename;
int num_logs = 0;
bool if_primary = false;
int ins_id = 0;
int g_id = 0;
string addrs_filename;

vector<pair<sockaddr_in,sockaddr_in>> rep_addrs;
map<string,vector<email>> inbox_headers;
map<string,vector<email>> outbox_headers;
map<string,string> month_map;
map<string,vector<string>> file_headers;
char BUF[10000];

sem_t fd_mutex;    // for modifying p_comm_fds
sem_t log_mutex;   // for modifying log file
sem_t data_mutex;   // for modifying data and pos file
map<string, sem_t> user_sem; // each user one semaphore

void print_bigtable(){
	map<string,map<string,pair<long,char*>>>::iterator it_1;
	for(it_1=bigtable.begin(); it_1!=bigtable.end(); it_1++){
		map<string,pair<long,char*>>::iterator it_2;
		for(it_2=it_1->second.begin(); it_2!=it_1->second.end(); it_2++){
			fprintf(stdout, "%s,%s,%ld,",
					it_1->first.c_str(), it_2->first.c_str(), it_2->second.first);
			for (long i=0; i<it_2->second.first; i++) {
				fprintf(stdout, "%c", it_2->second.second[i]);
			}
			fprintf(stdout, "\n");
		}
	}
}

void print_user_pos(){
	map<string,pair<long,long>>::iterator it;
	for(it=user_pos.begin(); it!=user_pos.end(); it++){
		fprintf(stdout, "%s ", (it->first).c_str());
		fprintf(stdout, "%ld %ld ", it->second.first, it->second.second);
		fprintf(stdout, "\n");
	}
}

void print_mail_headers(){
	map<string,vector<email>>::iterator it;
	for(it=outbox_headers.begin(); it!=outbox_headers.end(); it++){
		fprintf(stdout, "%s ", it->first.c_str());
		sort(outbox_headers[it->first].begin(), outbox_headers[it->first].end(), email_comp);
		for(int i=0; i<it->second.size(); i++){
			fprintf(stdout, "%s ", it->second[i].subj.c_str());
			fprintf(stdout, "%s ", it->second[i].sender.c_str());
			for (int i = 0; i<it->second[i].re.size(); i++)
				fprintf(stdout, "%s ", it->second[i].re[i].c_str());
			fprintf(stdout, "%s ", it->second[i].ts.c_str());
			fprintf(stdout, "%s ", it->second[i].ts_order.c_str());
			fprintf(stdout, "%s ", it->second[i].colomn.c_str());
		}
		fprintf(stdout, "\n");
	}
}

bool read_until(FILE *f_comm_fd, char *buf, long int max_len, string c) {
	int i;
	for (i = 0; ; i++) {
		int reading = fgetc(f_comm_fd);
		if (reading == EOF) return false; // no connection
		if (c.find((char) reading) != string::npos) break; // end
		if (i >= max_len - 1) return false; // size too large
		buf[i] = (char)reading;
	}
	buf[i] = 0;
	return true;
}

bool do_read(FILE * stream, char *buf, long int len) {
	for (long int i = 0; i < len; i++) {
		int reading = fgetc(stream);
		if (reading == EOF) return false;
		buf[i] = (char)reading;
	}
	return true;
}

bool do_write(int fd, const char *buf, int len){
	int sent = 0;
	while (sent < len){
		int n = write(fd, &buf[sent],len-sent);
		if (n<0) return false; sent += n;
	}
	return true;
}

// one mutex per user
bool bigtable_has(string username, string column) {
	if (bigtable.find(username) == bigtable.end()) return false;
	if (bigtable[username].find(column) == bigtable[username].end()) return false;
	return true;
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

void generate_month_map(){
	month_map["Jan"] = "01";
	month_map["Feb"] = "02";
	month_map["Mar"] = "03";
	month_map["Apr"] = "04";
	month_map["May"] = "05";
	month_map["Jun"] = "06";
	month_map["Jul"] = "07";
	month_map["Aug"] = "08";
	month_map["Sep"] = "09";
	month_map["Oct"] = "10";
	month_map["Nov"] = "11";
	month_map["Dec"] = "12";
}

string generate_ts_order(string ts){
	vector<string>time_list = split_string(ts, " ");
	string day = time_list[1];
	string month = time_list[2];
	string year = time_list[3];
	string clock = time_list[4];
	vector<string>clock_list = split_string(clock, ":");
	string hour = clock_list[0];
	string min = clock_list[1];
	string sec = clock_list[2];
	string ts_order = year + month_map[month] + day + hour + min + sec;
	return ts_order;
}

email parse_mail(string username, string column){
	long len = bigtable[username][column].first;
	char mail_copy[len+1];
	strcpy(mail_copy, bigtable[username][column].second);
	mail_copy[len] = '\0';
	string content(mail_copy);

	size_t f1 = content.find("To: ");
	size_t f2 = content.find("\r\n", f1+1);
	string receiver = content.substr(f1+4, f2-f1-4);
	vector<string>receiver_list = split_string(receiver, ", ");

	size_t f3 = content.find("From: ");
	size_t f4 = content.find("\r\n", f3+1);
	string sender = content.substr(f3+6, f4-f3-6);

	size_t f5 = content.find("Subject: ");
	size_t f6 = content.find("\r\n", f5+1);
	string subject = content.substr(f5+9, f6-f5-9);

	size_t f7 = content.find("Date: ");
	size_t f8 = content.find("\r\n", f7+1);
	string time = content.substr(f7+6, f8-f7-6);
	string time_order = generate_ts_order(time);

	return email(time,time_order,subject,sender,receiver_list,column);
}

void generate_header_for_each_user_each_column(string username, string column){
	if(column.compare(0, 14, "/mailbox/rcpt/") == 0){
		email m = parse_mail(username, column);
		inbox_headers[username].push_back(m);
	}else if(column.compare(0, 14, "/mailbox/sent/") == 0){
		email m = parse_mail(username, column);
		outbox_headers[username].push_back(m);
	}else if(column.compare(0, 6, "/file/") == 0){
		file_headers[username].push_back(column);
	}
}

void del_header_for_each_user_each_column(string username, string column){
	if(column.compare(0, 14, "/mailbox/rcpt/") == 0){
		for(int i=0; i<inbox_headers[username].size(); i++){
			if(inbox_headers[username][i].colomn == column){
				inbox_headers[username].erase(inbox_headers[username].begin()+i);
				break;
			}
		}
	}else if(column.compare(0, 14, "/mailbox/sent/") == 0){
		for(int i=0; i<outbox_headers[username].size(); i++){
			if(outbox_headers[username][i].colomn == column){
				outbox_headers[username].erase(outbox_headers[username].begin()+i);
				break;
			}
		}
	}else if(column.compare(0, 6, "/file/") == 0){
		for(int i=0; i<file_headers[username].size(); i++){
			if(file_headers[username][i] == column){
				file_headers[username].erase(file_headers[username].begin()+i);
				break;
			}
		}
	}
}

string generate_maillist_response(int page_start, string username, map<string,vector<email>> headers){
	string re="";
	sort(headers[username].begin(), headers[username].end(), email_comp);
	for(int i=page_start-1; i<page_start-1+11; i++){
		string rcpt_list="";
		if(i + 1<= headers[username].size() && i >= 0){
			for(int j=0; j<headers[username][i].re.size(); j++){
				if(j + 1 == headers[username][i].re.size()){
					rcpt_list = rcpt_list + headers[username][i].re[j];
					break;
				}
				rcpt_list += headers[username][i].re[j] + ";";
			}
			re += headers[username][i].ts + "\n" + headers[username][i].subj +
					"\n" + headers[username][i].sender + "\n" +
					rcpt_list + "\n" +  headers[username][i].colomn + "\n";
		}else break;
	}
	return re;
}

string get_list(string username, string column){
	string res;
	if(column.compare(0, 10, "/maillist/") == 0){
		string num_s = column.substr(15);
		int page_start = atoi(num_s.c_str());
		if(column.compare(10, 5, "rcpt/") == 0){
			res = generate_maillist_response(page_start, username, inbox_headers);
		}else if (column.compare(10, 5, "sent/") == 0){
			res = generate_maillist_response(page_start, username, outbox_headers);
		}

	}else if(column.compare(0, 7, "/cloud/") == 0){
		string num_s = column.substr(7);
		int page_start = stoi(num_s.c_str());
		sort(file_headers[username].begin(), file_headers[username].end(), file_comp);
		for(int i=page_start-1; i<page_start-1+11; i++){
			if(i+1 <= file_headers[username].size() && i >= 0){
				res = res + file_headers[username][i] + "\n";
			}else break;
		}
	}

	return res;
}

void generate_mailheader_for_each_user(string username){
	map<string,pair<long,char*>>::iterator it;
	for(it=bigtable[username].begin(); it!=bigtable[username].end(); it++){
		generate_header_for_each_user_each_column(username, it->first);
	}
}

void generate_list(){
	generate_month_map();
	map<string,map<string,pair<long,char*>>>::iterator it;
	for(it=bigtable.begin(); it!=bigtable.end(); it++){
		generate_mailheader_for_each_user(it->first);
	}
}

bool del(string username, string column){
	bool result;
	bool sem_exist = user_sem.find(username) != user_sem.end();
	if (sem_exist) sem_wait(&user_sem[username]);
	if (!bigtable_has(username, column)) result = false;
	else {
		delete[] bigtable[username][column].second;
		bigtable[username][column].second = NULL;
		bigtable[username][column].first = 0;
		bigtable[username].erase(column);
		del_header_for_each_user_each_column(username, column);
		result = true;
	}
	if (sem_exist) sem_post(&user_sem[username]);
	return result;
}

bool put(string username, string column, char * buf, long int len){
	// note: buf should be a dynamic array
	bool result;
	bool sem_exist = user_sem.find(username) != user_sem.end();
	if (sem_exist) sem_wait(&user_sem[username]);
	if (bigtable_has(username, column)) result = false;
	else {
		pair<long int, char *> new_pair = make_pair(len, buf);
		bigtable[username][column] = new_pair;
		generate_header_for_each_user_each_column(username, column);
		result = true;
	}
	if (sem_exist) sem_post(&user_sem[username]);
	return result;
}

bool cpu(string username, string column, char * orig_str, long int olen, char * new_str, long int nlen){
	// note: new_str should be a dynamic array
	bool result;
	bool sem_exist = user_sem.find(username) != user_sem.end();
	if (sem_exist) sem_wait(&user_sem[username]);
	if (!bigtable_has(username, column)) return false;
	else if (!equal(orig_str, orig_str + olen, bigtable[username][column].second)) result = false;
	else {
		delete[] bigtable[username][column].second;
		bigtable[username][column].second = new_str;
		bigtable[username][column].first = nlen;
		del_header_for_each_user_each_column(username, column);
		generate_header_for_each_user_each_column(username, column);
		result = true;
	}
	if (sem_exist) sem_post(&user_sem[username]);
	return result;
}

long int get(string username, string column, char *& buf){
	long int size;
	bool sem_exist = user_sem.find(username) != user_sem.end();
	if (sem_exist) sem_wait(&user_sem[username]);
	if (column.compare(0, strlen("/maillist/"), "/maillist/") == 0 ||
			column.compare(0, strlen("/cloud/"), "/cloud/") == 0) {
		BUF[0] = '\0';
		strcpy(BUF, get_list(username, column).c_str());
		if (sem_exist) sem_post(&user_sem[username]);
		buf = BUF;
		return strlen(BUF);
	}
	if (!bigtable_has(username, column)) size = 0;
	else {
		buf = bigtable[username][column].second;
		size = bigtable[username][column].first;
	}
	if (sem_exist) sem_post(&user_sem[username]);
	return size;
}

void update_disk(){
	string temp_data_filename = data_filename + "_new";
	sem_wait(&data_mutex);
	ofstream data_new_file(temp_data_filename);
	map<string,map<string,pair<long int, char*>>>::iterator it_1;
	map<string,pair<long int, char*>>::iterator it_2;
	long int pos = 0, last_pos = 0;
	for(it_1=bigtable.begin(); it_1!=bigtable.end(); it_1++){
		for(it_2=it_1->second.begin(); it_2!=it_1->second.end(); it_2++){
			data_new_file << it_2->first << " " << it_2->second.first << " ";
			for (long int i=0; i<it_2->second.first; i++) data_new_file << it_2->second.second[i];
		}
		pos = data_new_file.tellp();
		user_pos[it_1->first]= pair<long int, long int>(last_pos, pos);
		last_pos = pos;
	}
	data_new_file.close();

	string temp_pos_filename = pos_filename + "_new";
	ofstream pos_new_file(temp_pos_filename);
	map<string,pair<long int, long int>>::iterator it_3;
	for(it_3=user_pos.begin(); it_3!=user_pos.end(); ++it_3){
		pos_new_file << it_3->first << " " << it_3->second.first << " " << it_3->second.second << "\n";
	}
	pos_new_file.close();
	if(remove(data_filename.c_str()) == 0 && remove(pos_filename.c_str()) == 0){
		rename(temp_data_filename.c_str(), data_filename.c_str());
		rename(temp_pos_filename.c_str(), pos_filename.c_str());
	}else fprintf(stderr, "Backend Server fails to delete old files.\n");
	sem_post(&data_mutex);

	sem_wait(&log_mutex);
	ofstream clear_log_file(log_filename, ios::out | ios::trunc);
	clear_log_file.close();
	sem_post(&log_mutex);
	if(verbose) fprintf(stdout, "Finish updating disk and clear log file.\n");
}

void write_log_file(long int len_1, long int len_2, string cmd, string username, string column,
	const char * value1, const char * value2){
	sem_wait(&log_mutex);
	ofstream log_file(log_filename, ios::app);
	log_file << len_1 << " " << len_2 << " " << cmd << "(" << username << "," << column;
	if (len_1 > 0) {
		log_file << ",";
		for (int i=0; i<len_1; i++) log_file << value1[i];
		if (len_2 > 0) {
			log_file << ",";
			for (int i=0; i<len_2; i++) log_file << value2[i];
		}
	}
	log_file << ")\r\n";
	log_file.close();
	num_logs++;
	if (num_logs >= UPDATE_THRESHOLD) {
		update_disk();
	}
	sem_post(&log_mutex);
}

int setup_connection(sockaddr_in addr){
	int sock = socket(PF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		return sock;
	}
	int opt = 1;
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	if (connect(sock, (struct sockaddr*) &addr, sizeof(addr))<0) {
		close(sock);
		return -1;
	}
	return sock;;
}

void send2backend(sockaddr_in addr, long int len_1, long int len_2, string cmd, string username, string column,
		const char * value1, const char * value2) {
	int sock = setup_connection(addr);
	if (sock < 0) return;
	string temp = to_string(len_1)+" "+to_string(len_2)+" "+cmd+"("+username+","+column;
	do_write(sock, temp.c_str(), temp.length());
	if (len_1 > 0) {
		do_write(sock, ",", 1);
		do_write(sock, value1, len_1);
		if (len_2 > 0) {
			do_write(sock, ",", 1);
			do_write(sock, value2, len_2);
		}
	}
	do_write(sock, ")\r\n", 3);
	close(sock);
}

void send2replica(long int len_1, long int len_2, string cmd, string username, string column,
		const char * value1, const char * value2){
	for (int i=0; i<rep_addrs.size(); i++){
		if (i == ins_id) continue;
		send2backend(rep_addrs[i].first, len_1, len_2, cmd, username, column, value1, value2);
	}
}

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

bool send_file(int sock_fd, string filename) {
	FILE * pos_file = fopen(filename.c_str(), "r");
	long int len = get_file_size(pos_file);
	char buf[len];
	get_file_content(pos_file, buf, len);
	string header = to_string(len) + " ";
	if (!do_write(sock_fd, header.c_str(), header.length())) return false;
	if (!do_write(sock_fd, buf, len)) return false;
	return true;
}


int read_file_and_excute(FILE *f_comm_fd, int sock_fd, bool response = true) {
	int result;
	char buf[100];
	if (!read_until(f_comm_fd, buf, 100, " ")) return -1;
	long len_1 = stol(buf);
	cout<< buf << " ";
	if (!read_until(f_comm_fd, buf, 100, " ")) return -1;
	long len_2 = stol(buf);
	cout<< buf << " ";
	if (!read_until(f_comm_fd, buf, 100, "(")) return -1;
	string cmd = string(buf);
	cout<< buf << "(";
	if (!read_until(f_comm_fd, buf, 100, ",")) return -1;
	string username = string(buf);
	cout<< buf << ",";
	if (!read_until(f_comm_fd, buf, 100, ",)")) return -1;
	string column = string(buf);
	cout<< buf << ")"<<endl;

	if (user_sem.find(username) == user_sem.end() && cmd.compare("PUT") == 0) { // new user, new put
		sem_t new_sem;
		sem_init(&new_sem, 0, 1);
		user_sem[username] = new_sem;
	}

	if (strcasecmp("DEL", cmd.c_str()) == 0) {
		if (!del(username, column)) {
			if (response) if (!do_write(sock_fd, err, strlen(err))) return -1;
			result = 0;
		}
		else {
			if (response) {
				if (!do_write(sock_fd, ok, strlen(ok))) return -1;
			}
			result = 1;
			if (response) {
				write_log_file(-1, -1, cmd, username, column, "", "");
				if (if_primary) send2replica(len_1, len_2, cmd, username, column, "", "");
			}
		}
	}
	else if (strcasecmp("PUT", cmd.c_str()) == 0) {
		if (len_1 <= 0) {
			if (response) if (!do_write(sock_fd, err, strlen(err))) return -1;
			result = 0;
		}
		else {
			char * value1 = new char[len_1];
			if (!do_read(f_comm_fd, value1, len_1)) {
				delete [] value1;
				return -1;
			}
			if (!put(username, column, value1, len_1)) {
				delete [] value1;
				if (response) if (!do_write(sock_fd, err, strlen(err))) return -1;
				result = 0;
			}
			else {
				if (response) if (!do_write(sock_fd, ok, strlen(ok))) {
					delete [] value1;
					return -1;
				}
				result = 1;
				if(verbose){
					fprintf(stdout, "Backend Server stores (%s,%s,%s)\r\n",
							username.c_str(), column.c_str(), value1);
				}
				if (response) {
					write_log_file(len_1, -1, cmd, username, column, value1, "");
					if (if_primary) send2replica(len_1, len_2, cmd, username, column, value1, "");
				}
			}
		}
	}
	else if (strcasecmp("CPU", cmd.c_str()) == 0) {
		if (len_1 <= 0 || len_2 <= 0) {
			if (response) if (!do_write(sock_fd, err, strlen(err))) return -1;
			result = 0;
		}
		else {
			char value1[len_1];
			if (!do_read(f_comm_fd, value1, len_1)) return -1;
			int inter_char = fgetc(f_comm_fd);
			if (inter_char == EOF) return -1;
			char * value2 = new char[len_2];
			if (!do_read(f_comm_fd, value2, len_2)) {
				delete [] value2;
				return -1;
			}
			if (!cpu(username, column, value1, len_1, value2, len_2)) {
				delete [] value2;
				if (response) if (!do_write(sock_fd, err, strlen(err))) return -1;
				result = 0;
			}
			else {
				if (response) if (!do_write(sock_fd, ok, strlen(ok))) {
					delete [] value2;
					return -1;
				}
				result = 1;
				if(verbose){
					fprintf(stdout, "Backend Server updates (%s,%s,%s)\r\n",
							username.c_str(), column.c_str(), value2);
				}
				if (response) {
					write_log_file(len_1, len_2, cmd, username, column, value1, value2);
					if (if_primary) send2replica(len_1, len_2, cmd, username, column, value1, value2);
				}

			}
		}
	}
	else if (strcasecmp("GET", cmd.c_str()) == 0) {
		char * buf;
		long int size = get(username, column, buf);
		if (size <= 0) {
			if (response) if (!do_write(sock_fd, err, strlen(err))) return -1;
			result = 0;
		}
		else {
			string header = to_string(size) + " ";
			if (response) if (!do_write(sock_fd, header.c_str(), header.length())) return -1;
			if (response) if (!do_write(sock_fd, buf, size)) return -1;
			result = 1;
			if(verbose){
				fprintf(stdout, "Backend Server sends (%s)\r\n", buf);
			}
		}
	}
	else if (strcasecmp("LOG", cmd.c_str()) == 0) {
		result = 1;
		// send file
		if (!send_file(sock_fd, pos_filename)) return -1;
		if (!send_file(sock_fd, data_filename)) return -1;
		if (!send_file(sock_fd, log_filename)) return -1;
		if(verbose){
			fprintf(stdout, "Backend Server send log files\r\n");
		}
	}
	else { // not a valid command
		if (response) if (!do_write(sock_fd, err, strlen(err))) return -1;
		result = 0;
	}
	if (fgets(buf, 100, f_comm_fd) == NULL) return -1; // read until next line
	return result;
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

void parse_pos_file(){
	ifstream pos_file(pos_filename);
	string line;
	if(pos_file.is_open()){
		while(getline(pos_file, line)){
			vector<int> space_pos = check_punc(line.c_str(), line.size(), ' ', 2);
			string user = line.substr(0, space_pos[0]);
			int start = atoi(line.substr(space_pos[0]+1, space_pos[1]-space_pos[0]-1).c_str());
			int end = atoi(line.substr(space_pos[1]+1).c_str());
			user_pos[user] = make_pair(start,end);
		}
		pos_file.close();
	}else fprintf(stderr, "Cannot open position file.\n");
}

void read_stream_till(ifstream &file, char terminator, char *buf){
	char c;
	int counter = 0;
	while(1){
		c = file.get();
		if(c == terminator) break;
		buf[counter] = c;
		counter++;
	}
	buf[counter] = '\0';
}

void read_stream(ifstream &file, long len, char *buf){
	char c;
	for(long i=0; i<len; i++){
		file.get(c);
		buf[i] = c;
	}
}

void parse_each_user_data_file(ifstream &file, string username, long start, long end){
	file.seekg(start);
	while(1){
		char col[100] ={0}, len_s[100]={0};
		read_stream_till(file, ' ', col);
		read_stream_till(file, ' ', len_s);
		long len = atol(len_s);
		char *value = new char[len];
		read_stream(file, len, value);
		bigtable[username][string(col)] = make_pair(len, value);
		if(file.tellg() == end) break;
	}
}

void parse_data_file(){
	ifstream data_file(data_filename);
	if(data_file.is_open()){
		map<string,pair<long,long>>::iterator it;
		for(it=user_pos.begin(); it!=user_pos.end(); ++it){
			parse_each_user_data_file(data_file, it->first,
					(it->second).first, (it->second).second);
		}
	}else fprintf(stderr, "Cannot open data file.\n");
	data_file.close();
}

void parse_log_file(){
	int fd = open(log_filename.c_str(), O_RDWR);
	FILE *fp_fd = fdopen(fd, "r");
	while (true) {
		if (read_file_and_excute(fp_fd, fd, false) < 0) break;
		num_logs++;
	}
	fclose(fp_fd);
	if (num_logs > UPDATE_THRESHOLD) {
		update_disk();
	}
}

void parse_rep_file(){
	ifstream rep_file(addrs_filename);
	string line;
	if(rep_file.is_open()){
		while(getline(rep_file, line)){
			vector<int> semi_pos = check_punc(line.c_str(), line.size(), ';', 1);
			string addr_1_s = line.substr(0, semi_pos[0]);
			string addr_2_s = line.substr(semi_pos[0]+1);
			rep_addrs.push_back(make_pair(parse_addr(addr_1_s.c_str(), addr_1_s.size()),
					parse_addr(addr_2_s.c_str(), addr_2_s.size())));
		}
	}else fprintf(stderr, "Cannot open rep file.\n");
	rep_file.close();
}

int connect2server(string address) {
	sockaddr_in dest = parse_addr(address.c_str(), address.length());
	int sock = socket(PF_INET, SOCK_STREAM, 0);
	if (sock < 0){
		return sock;
	}
	if (connect(sock, (struct sockaddr*)&dest, sizeof(dest)) < 0) {
		close(sock);
		return -1;
	}
	return sock;
}

int get_primary_sock(int master_sock){
	int new_sock = dup(master_sock);
	FILE* stream = fdopen(new_sock, "r+");
	char buf[100];
	if (fgets(buf, 100, stream) == NULL) {
		fclose(stream);
		return -1;
	}
	cout << buf;
	if (strncasecmp(buf, "P", 1) == 0) { // I am the primary node
		if_primary = true;
		cout << "I am the primary for partition " << g_id <<endl;
		fclose(stream);
		return -1;
	}
	else if (strncasecmp(buf, "N", 1) == 0) {
		buf[strlen(buf)-2]=0;
		string primary_addr = string(buf+2);
		cout << "get primary node address: " <<primary_addr<<endl;
		fclose(stream);
		return connect2server(primary_addr);
	}
	return -1;
}

void overwrite_file(char * buf, long int len, string filename) {
	cout << "overwriting file " << filename<<endl;
	string tempfile = filename + ".temp";
	FILE* file_open = fopen(tempfile.c_str(), "w");
	int size = fwrite(buf, 1, len, file_open);
	if (remove(filename.c_str()) == 0) {
		rename(tempfile.c_str(), filename.c_str());
	}
	fclose(file_open);
	cout << "done overwriting" <<endl;
}

void read_and_overwrite(FILE* stream, string filename) {
	char buf1[100];
	read_until(stream, buf1, 100, " ");
	long int len = stol(buf1);
	char buf2[len];
	do_read(stream, buf2, len);
	overwrite_file(buf2, len, filename);
}

void log_and_overwrite_from_primary(int sock){
	string msg = "-1 -1 LOG(,)\r\n";
	do_write(sock, msg.c_str(), msg.length());
	FILE* stream = fdopen(sock, "r+");
	read_and_overwrite(stream, pos_filename);
	read_and_overwrite(stream, data_filename);
	read_and_overwrite(stream, log_filename);
	fclose(stream);
}

void recover(){
	cout <<"recovering"<<endl;
	int sock = get_primary_sock(master_sock);
	if (sock >= 0) {
		cout <<"get primary sock: " << sock <<endl;
		log_and_overwrite_from_primary(sock);
	}
	parse_pos_file();
	parse_data_file();
	parse_log_file();
//	print_mail_headers();
}

void* thread_worker(void *arg){
	int comm_fd = *(int*)arg;

	sem_wait(&fd_mutex);
	string num = to_string(p_comm_fds.size()) +"\r\n";
	if (!do_write(master_sock, num.c_str(), num.length())) {
		fprintf(stderr, "Failed to write to master node\n");
	}
	sem_post(&fd_mutex);

	if(verbose){
		fprintf(stdout, "[%d] backend server: new connection\n", comm_fd);
	}

	FILE *f_comm_fd = fdopen(comm_fd, "r");
	while(1){
		if (read_file_and_excute(f_comm_fd, comm_fd) < 0) break;
	}
	fclose(f_comm_fd);
	sem_wait(&fd_mutex);
	p_comm_fds.erase((int*)arg);
	num = to_string(p_comm_fds.size());
	if (!do_write(master_sock, num.c_str(), num.length())) {
		fprintf(stderr, "Failed to write to master node\n");
	}
	sem_post(&fd_mutex);

	if(verbose){
		fprintf(stdout, "[%d] master: thread exits\n", comm_fd);
	}
	pthread_detach(pthread_self());
	pthread_exit(NULL);
}

void* master_worker(void *arg){
	int comm_fd = *(int*)arg;
	if(verbose){
		fprintf(stdout, "[%d] connected with master\n", comm_fd);
	}
	FILE *f_comm_fd = fdopen(comm_fd, "r");
	char buf[100];
	while(1){
		if (fgets(buf, 100, f_comm_fd) == NULL) break;
		cout << "From master: " << buf;
		if (strncmp(buf, "P", 1) == 0) if_primary = true;
		else if (strncmp(buf, "N", 1) == 0) if_primary = false;
	}
	fclose(f_comm_fd);
	if(verbose){
		fprintf(stdout, "[%d] disconnected with master\n", comm_fd);
	}
	pthread_detach(pthread_self());
	pthread_exit(NULL);
}

int main(int argc, char *argv[]){
	if(argc < 2){
		fprintf(stderr, "Backend Server requires at least 2 arguments.\n");
		exit(1);
	}
	int opt = -1;
	while((opt = getopt(argc, argv, "vg:i:")) != -1){
			switch(opt){
		    	case('v'):
				verbose = true;
		        	break;
		    	case('g'):
		    		g_id = atoi(optarg);
		    		break;
		    	case('i'):
		    		ins_id = atoi(optarg);
		    		break;
		    	case('?'):
				if(optopt == 'i') fprintf(stderr, "Option -i requires an instance ID.\n");
				else if(optopt == 'g') fprintf(stderr, "Option -g requires an group ID.\n");
				else fprintf(stderr, "unknown option: -%c\n", optopt);
		    		exit(1);
			}
	}
	// initialize semaphores
	if (sem_init(&fd_mutex, 0, 1) == -1) {
		fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));
		exit(-1);
	}
	if (sem_init(&log_mutex, 0, 1) == -1) {
		fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));
		exit(-1);
	}
	if (sem_init(&data_mutex, 0, 1) == -1) {
		fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));
		exit(-1);
	}
	string dir = string("Backend_") +  to_string(g_id) + string("/");
	pos_filename = dir + string("pos_") + to_string(ins_id)+".txt";
	data_filename = dir + string("data_") + to_string(ins_id)+".txt";
	log_filename = dir + string("log_") + to_string(ins_id)+".txt";
	addrs_filename = dir + string("addrs.txt");

	parse_rep_file();

	fprintf(stdout, "********************Preloading*********************\n");
//	if (verbose) print_bigtable();
	fprintf(stdout, "**********************Done*************************\n");

	// master thread
	struct sockaddr_in master_dest = parse_addr(master_ip.c_str(), master_ip.length());
	master_sock = socket(PF_INET, SOCK_STREAM, 0);
	int option = 1;
	setsockopt(master_sock, SOL_SOCKET, SO_REUSEADDR, &option, sizeof(option));
	bind(master_sock, (struct sockaddr*)&rep_addrs[ins_id].second, sizeof(rep_addrs[ins_id].second));
	if (connect(master_sock, (struct sockaddr*) &master_dest, sizeof(master_dest))<0) {
		close(master_sock);
		fprintf(stderr, "Cannot connect to master node\n");
		exit(-1);
	}

	recover();

	pthread_t master_thread;
	pthread_create(&master_thread, NULL, master_worker, (void*)&master_sock);

	int socket_fd = socket(PF_INET, SOCK_STREAM, 0);
	if (socket_fd<0) printf("socket() failed\n");
	int option_value = 1;
	if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, (char*)&option_value, sizeof(int)) < 0)
		fprintf(stderr,"setsockopt(SO_REUSEADDR) failed\n");
	bind(socket_fd, (struct sockaddr*)&rep_addrs[ins_id].first, sizeof(rep_addrs[ins_id].first));
	listen(socket_fd, 1000);


	while(true){
		struct sockaddr_in front_addr;
		socklen_t front_add_rlen = sizeof(front_addr);
		int *p_comm_fd = (int*)malloc(sizeof(int)); // attention: memory leak
		*p_comm_fd = accept(socket_fd, (struct sockaddr*)&front_addr, &front_add_rlen);
		sem_wait(&fd_mutex);
		p_comm_fds.insert(p_comm_fd);
		sem_post(&fd_mutex);
		pthread_t thread;
		pthread_create(&thread, NULL, thread_worker, (void*)p_comm_fd);
	}

	return 0;
}

