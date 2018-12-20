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

void* handler(void* arg);

const string http = "HTTP/1.1";
const string ok = " 200 OK";
const string error400 = " 400 Bad Request"; //http grammar
const string error404 = " 404 Not Found"; //request link fail
const string error405 = " 405 Method Not Allowed"; //get or post
const string error500 = " 500 Internal Server Error";
const string html400 = "<HTML><TITLE>Bad Request</TITLE>\r\n<BODY><P>"
		"400 Bad Request.</BODY></HTML>\r\n";
const string html404 = "<HTML><TITLE>Not Found</TITLE>\r\n<BODY><P>"
		"404 Not Found. The server could not fulfill your request because the resource specified "
		"is unavailable or nonexistent.</BODY></HTML>\r\n";
const string html405 = "<HTML><TITLE>Method Not Allowed</TITLE>\r\n<BODY><P>"
		"405 Method Not Allowed.</BODY></HTML>\r\n";
const string html500 = "<HTML><TITLE>Internal Server Error</TITLE>\r\n<BODY><P>"
		"500 Internal Server Error.</BODY></HTML>\r\n";
const string redirect301 = " 301 Moved Permanently";
const string redirect302 = " 302 Found";
const string redirect303 = " 303 See Other";
const string redirect307 = " 307 Temporary Redirect";
const string redirect308 = " 308 Permanent Redirect";
const string host_name = "localhost";
string addrs_filename="frtrep.txt";
const string cookie_name = "name";
string master_node="127.0.0.1:5555";
string balancer_node ="127.0.0.1:8888";
int sock_load;
struct http_content{
	string first_line;
	map<string, string> header;
	char * content;
	long int size = 0;
};

map<int, string> error_map = { { 400, error400 }, { 404, error404 }, { 405,
		error405 }, { 500, error500 }};
map<int, string> error_html_map = { { 400, html400 }, { 404, html404 }, { 405,
		html405 }, { 500, html500 } };
map<int, string> redirect_map = { { 301, redirect301 }, { 302, redirect302 }, { 303,
		redirect303 }, { 307, redirect307 }, { 308, redirect308 } };

map<string, string> cookie_map;
sockaddr_in self_addr;
sockaddr_in to_load_addr;

int listen_fd;
bool verbose;
set<int> socket_all;
sem_t mutex;
int PORT = 80;	//set default port to 80
sem_t mqueue_mutex;


// for modify socket_all
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


void parse_addr_file(int i){
	ifstream rep_file(addrs_filename);
	string line;
	if(rep_file.is_open()){
		int counter= 0;
		while(getline(rep_file, line)){
			if(counter == i){
				vector<int> semi_pos = check_punc(line.c_str(), line.size(), ';', 1);
				string addr_1_s = line.substr(0, semi_pos[0]);
				string addr_2_s = line.substr(semi_pos[0]+1);
				self_addr = parse_addr(addr_1_s.c_str(), addr_1_s.size());
				to_load_addr = parse_addr(addr_2_s.c_str(), addr_2_s.size());
				break;
			}
			counter++;
		}
	}else fprintf(stderr, "Cannot open rep file.\n");
	rep_file.close();
}

void computeDigest(const char *data, long long int dataLengthBytes, unsigned char *digestBuffer)
{
	/* The digest will be written to digestBuffer, which must be at least MD5_DIGEST_LENGTH bytes long */

	MD5_CTX c;
	MD5_Init(&c);
	MD5_Update(&c, data, dataLengthBytes);
	MD5_Final(digestBuffer, &c);
}

string unsigned_char_to_hex(unsigned char *buffer) {
	string str;
	for (int i = 0; i < MD5_DIGEST_LENGTH; i++)
	{
		str += "0123456789ABCDEF"[buffer[i] / 16];
		str += "0123456789ABCDEF"[buffer[i] % 16];
	}
	return str;
}

string generate_hash(string content) {
	unsigned char digestBuffer[100];
	computeDigest(content.c_str(), content.length(), digestBuffer);
	return unsigned_char_to_hex(digestBuffer);
}

int write2mqueue(string content) {
	string dir = "dir/mqueue";
	sem_wait(&mqueue_mutex); // hold the file

	int fd = open(dir.c_str(), O_WRONLY | O_APPEND);
	if (fd == -1) {
		fprintf(stderr, "[%d] Error, cannot open the file directory\n", fd);

		sem_post(&mqueue_mutex); // release the file
		return -1;
	}
	if (flock(fd, LOCK_EX) == -1) { // lock the mqueue file
		fprintf(stderr, "[%d] Error, cannot lock the file descriptor\n", fd);
		close(fd);
		sem_post(&mqueue_mutex); // release the file
		return -1;
	}
	FILE *stream;
	if ((stream = fdopen(fd, "a")) == NULL) {
		fprintf(stderr, "[%d] Error, cannot open the file descriptor\n", fd);
		fclose(stream);
		sem_post(&mqueue_mutex); // release the file
		return -1;
	}
	fprintf(stream, "%s\r\n", content.c_str());
	fclose(stream);
	sem_post(&mqueue_mutex); // release the file
	return 1;
}

void print_http_msg(http_content* msg) {
	cout << msg->first_line << "\r\n";
	for (map<string,string>::iterator it=msg->header.begin(); it!=msg->header.end(); it++) {
		cout << it->first + ": " + it->second + "\r\n";
	}
	cout << "\r\n";
	if (msg->size < 1000){
		for (long int i=0; i<msg->size; i++) cout << msg->content[i];
	}
	cout << "\r\n";
}

void print_map(map<string, string> mymap) {
	map<string,string>::iterator it;
	for (it=mymap.begin(); it!=mymap.end(); ++it)
		std::cout << it->first << " => " << it->second << '\n';
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

void remove_spaces(string & str) {
	while(str.at(0) == ' ') str.erase(0, 1);
	while(str.at(str.length()-1) == ' ') str.erase(str.length()-1);
}

vector <string> split_and_remove_spaces(string line, string delim) {
	vector < string > after_split = split_string(line, delim);
	for (vector<string>::iterator i = after_split.begin(); i != after_split.end(); i++) {
		remove_spaces(*i);
	}
	return after_split;
}

string url_decode(string &str) {
	string ret;
	char ch;
	int i, format;
	for (i = 0; i < str.length(); i++) {
		if (int(str[i]) == 37) {
			sscanf(str.substr(i + 1, 2).c_str(), "%x", &format);
			ch = static_cast<char>(format);
			ret += ch;
			i = i + 2;
		}
		else if (str[i] == '+') {
			ret += " ";
		}
		else {
			ret += str[i];
		}
	}
	return (ret);
}

string cookie_generator(string name, map<string, string>&cookie_map) {
	srand(time(NULL));
	string cookie;
	do {
		cookie = name + to_string(rand());
	} while (cookie_map.count(cookie) != 0);
	return cookie;
}

string get_id_from_cookies(string cookies, string & cookie_value) {
	vector <string> cookie_split = split_string(cookies, "; ");
	string userid = "";
	for (vector<string>::const_iterator i = cookie_split.begin(); i != cookie_split.end(); i++) {
		size_t found = i->find_first_of('=', 0);;
		if (found == string::npos) continue;
		string fst = i->substr(0, found);
		string snd = i->substr(found + 1);
		if (fst.compare(cookie_name) == 0) {
			if (cookie_map.count(snd) != 0) { // cookie exists in memory
				cookie_value = snd;
				userid = cookie_map[snd];
				break;
			}
		}
	}
	return userid;
}

string get_filename_from_disposition(string disposition) {
	vector <string> disposition_split = split_string(disposition, "; ");
	string filename = "";
	for (vector<string>::const_iterator i = disposition_split.begin(); i != disposition_split.end(); i++) {
		size_t found = i->find_first_of('=', 0);;
		if (found == string::npos) continue;
		string fst = i->substr(0, found);
		string snd = i->substr(found + 1);
		if (fst.compare("filename") == 0) {
			filename = snd;
			break;
		}
	}
	return filename;
}

bool is_valid_id(string id) {
	return id.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ01234567890_")
			== string::npos;
}

string generate_full_email(string sender, vector<string> recievers, string subject, string content, string & time, string & id) {
	// TODO: anti-spam
	time = get_time();
	string msg = "";

	msg += "To: ";
	for (vector<string>::const_iterator i = recievers.begin(); i != recievers.end(); i++)
		msg += *i + ", ";
	if (recievers.size()) msg.erase(msg.length() - 2); // remove the last ", "
	msg += "\r\n";

	msg += "From: " + sender + "\r\n";
	msg += "Subject: " + subject + "\r\n";

	id = generate_hash(msg);
	msg += "Message-ID: " + id + "@PennCloud\r\n";
	msg += "Date: " + time + "\r\n";
	msg += "User-Agent: PennCloud v1.0\r\n";
	msg += "MIME-Version: 1.0\r\n";
	msg += "Content-Type: text/plain; charset=utf-8; format=flowed\r\n";
	msg += "Content-Transfer-Encoding: 7bit\r\n";
	msg += "Content-Language: en-US\r\n\r\n";
	msg += content;
	return msg;
}




string parse_res(int fd){
	char buf[1000];
	memset(buf, 0, 1000);
	string response;
	char *curr = buf;
	if (read(fd, curr, 1000-strlen(buf))==-1) return "";
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
		return "";
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
		if (m == -1) {
			close(sock);
			return "";
		}
		sent += m;
	}
	//  cout<<"message:"<<message<<endl;
	//read response
	string response = parse_res(sock);
	//  cout<<"response:"<<response<<endl;
	close(sock);
	return response;
}

string ask_master(string& master_node, string& userid, string cmd){
	string backend_msg = cmd + " " +userid+"\r\n";
	string backend_serv = tobackend(master_node, backend_msg.c_str());
	//	cout<<backend_res<<endl;
	return backend_serv;
}


int backend_setup(string& master_node, string& userid, string cmd) {
	string backend_serv=ask_master(master_node,userid,cmd);
	cout << "master response: "<<backend_serv<<endl;
	if (backend_serv.compare(0, 3, "err") == 0) return 0;
	if (backend_serv.compare("") == 0) return -1;
	int sock = socket(PF_INET, SOCK_STREAM, 0);
	if (sock < 0) {
		return sock;
	}
	int opt = 1;
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	struct sockaddr_in dest;
	bzero(&dest, sizeof(dest));
	dest.sin_family = AF_INET;
	char address[backend_serv.length() + 1] = { 0 };
	strcpy(address, backend_serv.c_str());
	char *arg_ip = strtok(address, ":");
	inet_pton(AF_INET, arg_ip, &(dest.sin_addr));
	char *arg_port = strtok(NULL, ":");
	dest.sin_port = htons(atoi(arg_port));
	if (connect(sock, (struct sockaddr*) &dest, sizeof(dest)) < 0){
		close(sock);
		return -1;
	}
	return sock;
}


bool do_read(FILE * stream, char *buf, long int len) {
	for (long int i = 0; i < len; i++) {
		int reading = fgetc(stream);
		if (reading == EOF) return false;
		buf[i] = (char)reading;
	}
	return true;
}

bool do_read(int fd, char *buf, long int len) {
	long int rcvd = 0;
	while (rcvd < len) {
		int n = read(fd, &buf[rcvd], len-rcvd);
		if (n<0)
			return false;
		rcvd += n;
	}
	return true;
}

bool do_write(int fd, const char *buf, long int len) {
	long int sent = 0;
	while (sent < len) {
		long int n = write(fd, &buf[sent], len - sent);
		if (n < 0)
			return false;
		sent += n;
	}
	return true;
}

int read_from_client(int comm_fd, http_content * msg, char * buffer) {
	// return -1 if communication ends, return 1 if ok, return 0 if there is some format problem
	FILE *stream;
	if ((stream = fdopen (comm_fd, "r+")) == NULL) {
		fprintf(stderr, "[%d] Error, cannot open the file descriptor\n", comm_fd);
		return -1;
	}

	// get first line
	if ((fgets(buffer, MAX_SIZE, stream)) == NULL) return -1;
	if (strcmp(&buffer[strlen(buffer)-2], "\r\n") == 0) buffer[strlen(buffer)-2] = 0; // remover "\r\n"
	msg->first_line = string(buffer);

	while (true) { // get all headers
		if ((fgets(buffer, MAX_SIZE, stream)) == NULL) return -1;
		if (strcmp(buffer, "\r\n") == 0) { // the end of header
			break;
		}
		if (strcmp(&buffer[strlen(buffer)-2], "\r\n") == 0) buffer[strlen(buffer)-2] = 0; // remover "\r\n"

		string fst, snd; string line = string(buffer);
		size_t found = line.find_first_of(':', 0);
		if (found == string::npos) return 0;
		fst = line.substr(0, found);
		snd = line.substr(found + 2);
		msg->header[fst] = snd;
	}

	// get contents
	if (msg->header.count("Content-Length") != 0) msg->size = stol(msg->header["Content-Length"]);
	bool is_multi_part = false;
	char bound[100] = {0};
	if (msg->header.count("Content-Type") != 0)  {
		string header = msg->header["Content-Type"];
		is_multi_part = (header.compare(0, strlen("multipart/form-data"), "multipart/form-data") == 0);
		string boundary = header.substr(strlen("multipart/form-data; boundary="));
		strcpy(bound, boundary.c_str());
	}

	if (!is_multi_part) {
		if (!do_read(stream, buffer, msg->size)) {
			return -1;
		}
	}
	else {
		char new_buf [msg->size] = {0};
		if (!do_read(stream, new_buf, msg->size)) {
			return -1;
		}
		char * it_1 = find(new_buf, new_buf + msg->size, '\n') + 1;
		it_1 = find(it_1, new_buf + msg->size, '\n') + 1;
		it_1 = find(it_1, new_buf + msg->size, '\n') + 1;
		it_1 = find(it_1, new_buf + msg->size, '\n') + 1;
		char * it_2 = find_end(it_1, new_buf + msg->size, bound, bound + strlen(bound)) - 1;
		memcpy(buffer, it_1, it_2 - it_1);
		msg->size = it_2 - it_1;
	}
	msg->content = buffer;
	return 1;
}

int expect(int sock, string expectation) {
	char buffer[100];
	FILE * pFile;
	pFile = fdopen(sock,"r");
	if (fgets(buffer, 100, pFile) == NULL) return -1;
	if(strncasecmp(buffer,expectation.c_str(),expectation.length())==0)
		return 1;
	else
		return 0;
}

int backend_put(string key, string userid, const char* content, long int size){
	int sock=backend_setup(master_node,userid,"PUT");
	if(sock<=0){
		return sock;
	}
	char buf[size+key.length()+userid.length()+20]={0};
	string temp=to_string(size)+" -1 PUT("+userid+","+key+",";
	char *pos=buf;
	memcpy(pos,temp.c_str(),temp.length());
	pos=pos+temp.length();
	memcpy(pos,content,size);
	pos+=size;
	memcpy(pos,")\r\n",3);
	pos=pos+3;
	if (!do_write(sock,buf,pos-buf)) {
		close(sock);
		return -1;
	}
	cout << buf<<endl;
	int result = expect(sock,"ok");
	close(sock);
	return result;
}


int backend_cput(string key, string userid,
		const char * orig_content, long int orig_size, const char * content, long int size) {
	return false;
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

int backend_dele(string key, string userid) {
	int sock=backend_setup(master_node,userid, "DEL");
	if(sock<=0){
		return sock;
	}
	string temp="-1 -1 DEL("+userid+","+key+")\r\n";
	if (!do_write(sock,temp.c_str(),temp.length())) return -1;
	cout << temp<<endl;
	int result = expect(sock,"ok");
	close(sock);
	return result;
}

long int backend_readlength(FILE * stream){
	int i;
	char cstr[100] = {0};
	for (i = 0; ; i++) {
		int reading = fgetc(stream);
		if (reading == EOF) return -1; // no connection
		if ((char) reading == ' ') break; // end of size
		if (i >= 100-1) return 0; // size too large
		cstr[i] = (char)reading;
		if (i == 2 && strncasecmp(cstr, "ERR", 3) == 0) {
			char buf[100];
			fgets(buf, 100, stream);
			return 0; // see an error
		}
	}
	cstr[i] = 0;
	return stol(cstr);
}


long int backend_get(string key, string userid, char * buf) {
	int page;
	string key2;
	if(key.compare(0,6,"/cloud")==0 || key.compare(0,9,"/maillist")==0){
		size_t found = key.find_last_of('/');
		page=(stoi(key.substr(found+1))-1)*10+1;
		key2=key.substr(0,found)+"/"+to_string(page);
	}
	else{
		key2=key;
	}

	int sock=backend_setup(master_node,userid,"GET");
	if(sock<=0){
		return sock;
	}
	string temp="-1 -1 GET("+userid+","+key2+")\r\n";
	if (!do_write(sock,temp.c_str(),temp.length())) {
		return -1;
	}
	cout << temp<<endl;
	FILE *stream=fdopen(sock,"r+");
	long int get_length=backend_readlength(stream);
	if (get_length <= 0) {
		fclose(stream);
		return get_length;
	}
	bool result = do_read(stream, buf, get_length);
	if (!result) {
		fclose(stream);
		return -1;
	}
	fclose(stream);
	return get_length;
	//	if(key.compare("password")==0 && userid.compare("zzz")==0)
	//			{strcpy(buf,"12345");
	//		}
	//	else if(key.compare("/maillist/rcpt/1")==0 && userid.compare("zzz")==0)
	//			{strcpy(buf,"111\nhello\nsender1\nhash1\n222\nhi\nsender2\nhash2\n333\nhow are you\nsender3\nhash3");
	//			}
	//	else if(key.compare("/maillist/rcpt/2")==0 && userid.compare("zzz")==0)
	//	{strcpy(buf,"Saturday\nhello\nsender1\nhash1");
	//	}
	//	else if(key.compare("/mailbox/rcpt/hash1")==0){
	//			strcpy(buf,"To:zhang\r\nFrom: zzz\r\nSubject: test\r\nMessage-ID:djsf\r\nDate: 12/24\r\nUser-aGENT\r\nMime\r\nContent-type:\r\n\r\nmessage body");
	//		}
	//	else if(key.compare("/cloud/1")==0)
	//	{
	//		strcpy(buf,"111\ntitle3.png");
	//	}
	//	else if(key.compare("/file/title3.png")==0){
	//		FILE *file_open=fopen("HTML/title3.png","r");
	//		long int size=get_file_size(file_open);
	//		get_file_content(file_open,buf,size);
	//		//cout<<"picture size"<<size<<endl;
	//		return size;
	//
	//	}
	//		return strlen(buf);
}

string change_template(string backend, FILE * file_open, string key){
	vector<string> split_key;
	split_key.clear();
	size_t pos=key.find_first_of("/",1);
	string frt=key.substr(0,pos);
	string snd=key.substr(pos+1);
	split_key=split_string(snd,"/");


	int page1=1,page2=1;
	long int size=get_file_size(file_open);
	char content[size+1]={0};
	get_file_content(file_open, content, size);
	string content_temp=string(content);


	if(frt.compare("/maillist")==0)//show maillist
	{
		string content_change;
		string row_template= "<tr><td>#0</td><td><a href=\"#1\">#2</a></td> <td>#3</td> <td><input type=\"submit\" id=\"#4\" value=\"delete\" onclick=\"deleteclick(id)\"/></td></tr>";
		int i=0,j=0;
		if(split_key[0].compare("rcpt")==0){

			size_t header=content_temp.find("$$$");
			content_temp.replace(header,3,"Sender");
			size_t choose=content_temp.find("???");
			content_temp.replace(choose,3,"Inbox");
			if(backend.length()>0)
			{
				vector<string>backend_list=split_string(backend,"\n");
				while(i<backend_list.size()-1){
					string temp=row_template;
					size_t pos=temp.find("#0");
					temp.replace(pos,2,backend_list[i/5*5]); // time stamp
					pos=temp.find("#1",pos+2);
					temp.replace(pos,2,backend_list[i/5*5+4]);
					pos=temp.find("#2",pos+2);
					temp.replace(pos,2,backend_list[i/5*5+1]); // subject
					pos=temp.find("#3",pos+2);
					temp.replace(pos,2,backend_list[i/5*5+2]); // sender
					pos=temp.find("#4",pos+2);
					temp.replace(pos,2,"/dele"+backend_list[i/5*5+4]); // hash key
					i+=5;
					j++;
					if(j<=10)
						content_change.append(temp);
				}
				size_t p=content_temp.find("***");
				content_temp.replace(p,3,content_change);
			}
			else{
				size_t p=content_temp.find("***");
				content_temp.replace(p,3," ");
			}
		}
		else if(split_key[0].compare("sent")==0){
			size_t header=content_temp.find("$$$");
			content_temp.replace(header,3,"Receiver");
			size_t choose=content_temp.find("???");
			content_temp.replace(choose,3,"Sentbox");

			if(backend.length()>0){
				vector<string>backend_list=split_string(backend,"\n");
				while(i<backend_list.size()-1){
					string temp=row_template;
					size_t pos=temp.find("#0");
					temp.replace(pos,2,backend_list[i/5*5]); // time stamp
					pos=temp.find("#1",pos+2);
					temp.replace(pos,2,backend_list[i/5*5+4]);
					pos=temp.find("#2",pos+2);
					temp.replace(pos,2,backend_list[i/5*5+1]); // subject
					pos=temp.find("#3",pos+2);
					temp.replace(pos,2,backend_list[i/5*5+3]); // receiver
					pos=temp.find("#4",pos+2);
					temp.replace(pos,2,"/dele"+backend_list[i/5*5+4]); // hash key
					i+=5;
					j++;
					if(j<=10)
						content_change.append(temp);


				}
				size_t p=content_temp.find("***");
				content_temp.replace(p,3,content_change);
			}
			else {
				size_t p=content_temp.find("***");
				content_temp.replace(p,3," ");
			}
		}
		page1=stoi(split_key[1]);
		if(page1!=1){
			size_t q=content_temp.find("###");
			content_temp.replace(q,3,"/maillist/"+split_key[0]+"/"+to_string(page1-1));
		}
		else{
			size_t q=content_temp.find("###");
			content_temp.replace(q,3,"/maillist/"+split_key[0]+"/"+to_string(page1));
		}
		if(j==11){
			size_t h=content_temp.find("&&&");
			content_temp.replace(h,3,"/maillist/"+split_key[0]+"/"+to_string(page1+1));
		}
		else{
			size_t h=content_temp.find("&&&");
			content_temp.replace(h,3,"/maillist/"+split_key[0]+"/"+to_string(page1));
		}

		return content_temp;
	}


	else if(frt.compare("/mailbox")==0){ //show mail content
		//cout<<backend<<endl;
		vector<string>backend_list=split_string(backend,"\r\n");
		string content_temp=string(content);
		size_t found = content_temp.find("#1");
		content_temp.replace(found,2,backend_list[0]);
		found = content_temp.find("#2",found+2);
		content_temp.replace(found,2,backend_list[1]);
		found = content_temp.find("#3",found+2);
		content_temp.replace(found,2,backend_list[2]);
		found = content_temp.find("#4",found+2);
		content_temp.replace(found,2,backend_list[4]);
		found = content_temp.find("#5",found+2);
		vector<string>::const_iterator i;
		for (i = backend_list.begin(); i != backend_list.end(); i++)
			if (i->compare("") == 0) break;
		string msg = "";
		if (i != backend_list.end()) {
			for (i=i+1; i != backend_list.end(); i++)
				msg += *i;
		}
		content_temp.replace(found,2, msg);
		return content_temp;
	}

	else if(frt.compare("/cloud")==0)//show file list
	{
		page2=stoi(split_key[0]);
		string content_change;
		int i=0,j=0;
		if(backend.length() > 0){
			string row_template= "<tr><td><a href=\"#1\">#2</a></td> <td><input type=\"submit\" id=\"#3\" value=\"delete\" onclick=\"deleteclick(id)\"/></td></tr>";

			vector<string>backend_list=split_string(backend,"\n");
			//		cout<<backend_list.size()<<endl;
			while(i<backend_list.size()){
				//			cout<<backend_list[i]<<endl;
				size_t fn=backend_list[i].find_last_of("/");
				string filename=backend_list[i].substr(fn+1);
				string temp=row_template;
				size_t pos=temp.find("#1");
				temp.replace(pos,2,backend_list[i]);
				pos=temp.find("#2",pos+2);
				temp.replace(pos,2,filename); // file name
				pos=temp.find("#3",pos+2);
				temp.replace(pos,2,"/dele"+backend_list[i]); // delete file name
				// hash key
				j++;
				i++;
				if(j<=10)
					content_change.append(temp);
			}
			size_t p=content_temp.find("***");
			content_temp.replace(p,3,content_change);
		}
		else{
			size_t p=content_temp.find("***");
			content_temp.replace(p,3," ");
		}
		if(page2!=1){
			size_t q=content_temp.find("###");
			content_temp.replace(q,3,"/cloud/"+to_string(page2-1));
		}
		else{
			size_t q=content_temp.find("###");
			content_temp.replace(q,3,"/cloud/"+to_string(page2));
		}

		if(j==11){
			size_t h=content_temp.find("&&&");
			content_temp.replace(h,3,"/cloud/"+to_string(page2+1));
		}
		else{
			size_t h=content_temp.find("&&&");
			content_temp.replace(h,3,"/cloud/"+to_string(page2));
		}
		//		cout<<"content_temp "<<endl<<content_temp<<endl;
		return content_temp;
	}
}

string login_wrong_template(FILE * file_open, string errormessage)
{
	long int size=get_file_size(file_open);
	char content[size+1]={0};
	get_file_content(file_open, content, size);
	string temp=string(content);
	size_t pos=temp.find("***");
	temp.replace(pos,3,errormessage);
	return temp;
}

string get_file_type(string file_type) {
	if (file_type.compare("jpg") == 0) return "image/jpeg";
	if (file_type.compare("png") == 0) return "image/png";
	if (file_type.compare("css") == 0) return "text/css";
	if (file_type.compare("html") == 0) return "text/html";
	return "";
}

FILE * find_file(string file, string & type) {
	size_t found = file.find_last_of('.');
	string file_type;
	if (found == string::npos) {
		file += ".html";
		file_type = "html";
	}
	else file_type = file.substr(found + 1);
	type = get_file_type(file_type);
	file = "HTML" + file;
	if (file_type.compare("jpg") == 0 || file_type.compare("png") == 0) {
		return fopen(file.c_str(), "rb");
	}
	else {
		FILE * a=fopen(file.c_str(), "r");
		if (a == NULL)fprintf(stderr, "fopen: failed: %s\n", strerror(errno));
		return a;
	}
}

void remove_html_extn(string & word) {
	size_t found = word.find_last_of('.');
	if (found == string::npos) return;
	string file_type = word.substr(found + 1);
	if (file_type.compare("html") == 0) word = word.substr(0, found);
}

FILE * seperate_word_and_open(string & word, int & mode, string & type) {
	// mode 0: invalid format
	// mode 1: only need to response an existing FILE object
	// mode 2: need to ask backend for more contents and response a merge of content
	// mode 3: only need to response information from backend
	string fst, snd; type =""; mode = 0;
	size_t found = word.find_first_of('/', 1);
	if (found == string::npos) fst = word;
	else {
		fst = word.substr(0, found);
		snd = word.substr(found + 1);
	}
	//cout<<"fst "<<fst<<endl;
	remove_html_extn(fst);
	if (snd.compare("") == 0) word = fst;
	else word = fst + "/" + snd;
	// check formatting
	if (fst.compare("/login") == 0 || fst.compare("/welcome") == 0 || fst.compare("/logout") == 0 ||
			fst.compare("/writemail") == 0 || fst.compare("/register") == 0) { // do not need any more information
		if (snd.compare("") != 0) return NULL;
		mode = 1;
	}
	else if (fst.compare("/maillist") == 0 ) {
		if (snd.compare("") == 0) {
			snd = "rcpt/1";
			word += "/rcpt/1";
		}
		size_t found = snd.find_first_of('/', 0);
		string snd_fst, snd_snd;
		if (found == string::npos) {
			snd_fst = snd;
			snd_snd = "/1";
			word += "1";
		}
		else {
			snd_fst = snd.substr(0, found);
			snd_snd = snd.substr(found + 1);
		}

		if (!(snd_fst.compare("rcpt")==0 || snd_fst.compare("sent") || snd_fst.compare("trash"))) return NULL;
		if (snd_snd.find_first_not_of("0123456789") != string::npos || snd_snd.compare("0") == 0) return NULL;
		mode = 2;

	}
	else if (fst.compare("/cloud") == 0) {
		if (snd.compare("") == 0) word += "/1";
		if (snd.find_first_not_of("0123456789") != string::npos || snd.compare("0") == 0) return NULL;
		mode = 2;
	}
	else if (fst.compare("/mailbox") == 0) { // need mail hash here
		if (snd.compare("") == 0) {
			fst = "/maillist";
			word = "/maillist/rcpt/1";
		}
		mode = 2;
	}
	else if (fst.compare("/file") == 0) { // need file name here
		if (snd.compare("") == 0) return NULL;
		mode = 3;
		return NULL;
	}
	else if (fst.compare("/dele") == 0) {
		word = "/" + snd;
		mode = 4;
		return NULL;
	}
	else {
		mode = 1;
	}
	FILE * file_open = find_file(fst, type);
	if (file_open == NULL) mode = 0;
	return file_open;
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
	print_http_msg(msg);
	return true;
}

bool response_ok(int comm_fd, string type, char * content, long int size, string cookie = "", string filename = "") {
	http_content msg;
	msg.size = size;
	msg.first_line = http + ok;
	msg.header.insert(pair<string,string>("Date", get_time()));
	if (type.compare("") != 0) msg.header.insert(pair<string,string>("Content-Type", type));
	msg.header.insert(pair<string,string>("Content-Length", to_string(msg.size)));
	if (cookie.compare("") != 0) msg.header.insert(pair<string,string>("Set-Cookie", cookie_name+"="+cookie));
	if (filename.compare("") != 0) msg.header.insert(pair<string,string>("Content-Disposition", "attachment; filename=\""+filename+"\""));
	msg.content = content;
	return response2client(comm_fd, &msg);
}

bool response_file(FILE * file_open, int comm_fd, string type, string cookie = "") {
	long int size = get_file_size(file_open);
	char content[size];
	get_file_content(file_open, content, size);
	return response_ok(comm_fd, type, content, size, cookie);
}

bool response_file_direct(int comm_fd, string file, string cookie = "") {
	// If you are sure that 'file' is a valid html file in path, use this function other than the one above
	// used in POST condition
	string type = "/text/html";
	FILE * file_open = find_file(file, type);
	if (file_open == NULL) return false;
	return response_file(file_open, comm_fd, type, cookie);
}

bool response_file_edit(int comm_fd, string file, string errormessage = "") {
	// If you are sure that 'file' is a valid html file in path, use this function other than the one above
	// change *** in file to error message
	string type = "/text/html";
	FILE * file_open = find_file(file, type);
	if (file_open == NULL) return false;
	string content = login_wrong_template(file_open, errormessage);
	long int size = content.length();
	char buf[size];
	strcpy(buf, content.c_str());
	return response_ok(comm_fd, type, buf, size);
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

bool response_err(int comm_fd, int err) {
	char buf[1000] = { 0 };
	http_content msg;
	msg.size = error_html_map[err].length();
	msg.first_line = http + error_map[err];
	msg.header.insert(pair<string,string>("Date", get_time()));
	msg.header.insert(pair<string,string>("Content-Type", "text/html"));
	msg.header.insert(pair<string,string>("Content-Length", to_string(msg.size)));
	strcpy(buf, error_html_map[err].c_str());
	msg.content = buf;
	return response2client(comm_fd, &msg);
}

bool response_combined_file(FILE * file_open, int comm_fd, string type, string key, string userid) {
	char buf[MAX_SIZE]={0};
	long int size = backend_get(key, userid, buf);
	if (size < 0) return response_err(comm_fd, 500);
	if (size == 0) buf[0] = 0;
	cout<<"get "<<size<<" bytes from backend" <<endl;
	string content = change_template(string(buf), file_open, key);
	size = content.length();
	strcpy(buf, content.c_str());
	return response_ok(comm_fd, type, buf, size);
}

bool response_cloud_file(int comm_fd, string key, string userid) {
	char buf[MAX_SIZE]={0};
	long int size = backend_get(key, userid, buf);
	if (size < 0) return response_err(comm_fd, 500);
	if (size == 0) return response_err(comm_fd, 404);
	cout<<"get "<<size<<" bytes from backend" <<endl;
	string filename = key.substr(key.find_last_of('/') + 1);
	return response_ok(comm_fd, "", buf, size, "", filename);
}


bool response_get(int comm_fd, string word, string userid = "") {
	bool logged_in = userid.compare("");
	if (word.compare("/") == 0 || word.compare("") == 0 || word.compare("/index") == 0) {
		if (logged_in)
			return response_redirect(comm_fd, "http://localhost:"+to_string(PORT)+"/welcome.html", 302);
		else
			return response_redirect(comm_fd, "http://localhost:"+to_string(PORT)+"/login.html", 302);
	}
	int mode; string type;
	FILE * file_open = seperate_word_and_open(word, mode, type);
	cout << "Mode: " << mode << ", key: " << word << ", type: " << type << endl;
	if (!logged_in && type.compare("text/html") == 0 &&
			word.compare(0, 9, "/register") != 0 && word.compare(0, 6, "/login") != 0) {
		return response_redirect(comm_fd, "http://localhost:"+to_string(PORT)+"/login.html", 302);
	}
	switch (mode) {
	case 0:
		return response_err(comm_fd,404);
	case 1:
		return response_file(file_open, comm_fd, type);
	case 2:
		return response_combined_file(file_open, comm_fd, type, word, userid);
	case 3:
		return response_cloud_file(comm_fd, word, userid);
	case 4: // dele
		int result = backend_dele(word, userid);
		if (result < 0) return response_err(comm_fd, 500);
		if (result == 0) return response_file_direct(comm_fd, "/dele_fail.html");
		if (word.compare(0, strlen("/mailbox"), "/mailbox")==0) { // delete a mail
			string mail_directory = word.substr(strlen("/mailbox"), 5);
			return response_redirect(comm_fd, "http://localhost:"+to_string(PORT)+
					"/maillist.html"+mail_directory+"/1", 302); // redirect to the first page
		}
		else // delete a file
			return response_redirect(comm_fd, "http://localhost:"+to_string(PORT)+"/cloud.html/1", 302); // redirect to the first page

	}
	return false;
}

bool response_post(int comm_fd, string word, http_content *msg, string userid) {
	char * content = msg->content;
	content[msg->size] = 0;
	if (word.compare("/register") == 0) {
		vector < string > log_info = split_string(content, "&");
		string name = split_string(url_decode(log_info[0]), "=")[1];
		string password = split_string(url_decode(log_info[1]), "=")[1];
		cout << "name: " << name << " password: " << password<<endl;
		if (!is_valid_id(name) || !is_valid_id(password))
			return response_file_edit(comm_fd, "/login_wrong.html","name or password is not valid");
		if (password.length() < 6)
			return response_file_edit(comm_fd, "/login_wrong.html","password must be at least 6 characters");
		int result = backend_put("password", name, password.c_str(), password.length());
		if (result < 0) return response_err(comm_fd, 500);
		if (result == 0) return response_file_edit(comm_fd, "login_wrong.html","username exists");
		return response_file_direct(comm_fd, "/register_succeed.html");
	} else if (word.compare("/log_in") == 0) {
		vector < string > log_info = split_string(content, "&");
		string name = split_string(url_decode(log_info[0]), "=")[1];
		string password = split_string(url_decode(log_info[1]), "=")[1];
		cout << "name: " << name << " password: " << password<<endl;
		if (!is_valid_id(name) || !is_valid_id(password))
			return response_file_edit(comm_fd, "/login_wrong.html","name or password is not valid");
		char buf[100];
		long int size = backend_get("password", name, buf);
		if (size < 0) return response_err(comm_fd, 500);
		if (size == 0) return response_file_edit(comm_fd, "/login_wrong.html","Not registered yet!");
		cout<<"get "<<size<<" bytes from backend" <<endl;
		if (password.compare(buf) != 0) return response_file_edit(comm_fd, "/login_wrong.html","Incorrect password!");
		// password correct
		string cookie = cookie_generator(name, cookie_map); // cookie generator
		cookie_map[cookie] = name;
		return response_file_direct(comm_fd, "/welcome.html", cookie);
	} else if (word.compare("/send_mail") == 0) {
		vector < string > log_info = split_string(string(content), "&");
		string sender = userid + "@" + host_name;
		string receivers = split_string(url_decode(log_info[0]), "=")[1];
		vector<string> receiver = split_and_remove_spaces(receivers, ";");
		string title = split_string(url_decode(log_info[1]), "=")[1];
		string message = split_string(url_decode(log_info[2]), "=")[1];
		string mail_hash;
		string time;
		string full_email = generate_full_email(sender, receiver, title, message, time, mail_hash);
		// put mail in "sent" TODO: put the mail into "sent" after successfully delivering the mail
		int result = backend_put("/mailbox/sent/" + mail_hash, userid, full_email.c_str(), full_email.length());
		if (result < 0) return response_err(comm_fd, 500);
		if (result == 0) return response_err(comm_fd, 500); // should never appear
		bool send_success = true;

		for (vector<string>::const_iterator i = receiver.begin(); i != receiver.end(); i++) {
			// send the mail out
			vector <string> reciever_mail = split_string(*i, "@");
			if (reciever_mail[1].compare(host_name) == 0) {
				result = backend_put("/mailbox/rcpt/" + mail_hash, reciever_mail[0], full_email.c_str(), full_email.length());
				continue;
			} else {
				//add mqueue
				string mqueue = "From <" + sender + "> to <" + *i +"> " + time + "\r\n" + full_email;
				//cout<<mqueue<<endl;
				int result = write2mqueue(mqueue);
			}
			if (result < 0) return response_err(comm_fd, 500);
			if (result == 0) {
				send_success = false;
			}
		}
		if (!send_success) return response_file_direct(comm_fd, "/send_email_failed.html");; // no receiver
		return response_file_direct(comm_fd, "/send_email_successful.html");
	} else if (word.compare(0, strlen("/upload/"), "/upload/") == 0) {
		if (msg->size > MAX_SIZE) return response_file_direct(comm_fd, "/upload_fail.html");
		string filename = word.substr(strlen("/upload/"));
		// find filename
		if (filename == "") return response_file_direct(comm_fd, "/upload_fail.html");
		int result = backend_put("/file/" + filename, userid, content, msg->size);
		if (result < 0) return response_err(comm_fd, 500);
		if (result == 0) {
			//			char buffer[MAX_SIZE];
			//			long int size = backend_get("/file/" + filename, userid, buffer);
			//			if (size < 0) return response_err(comm_fd, 500);
			//			if (size == 0) return response_file_direct(comm_fd, "/upload_fail.html");
			//			result = backend_cput("/file/" + filename, userid, buffer, size, content, msg->size);
			//			if (result < 0) return response_err(comm_fd, 500);
			//			if (size == 0) return response_file_direct(comm_fd, "/upload_fail.html");

			return response_file_direct(comm_fd, "/upload_fail.html"); // file exists, need cput TODO (optional)
		}
		return response_redirect(comm_fd, "http://localhost:"+to_string(PORT)+"/cloud.html/1", 302); // redirect to the first page

	}/* else if (word.compare("/download") == 0) {

	}*/
	else return response_err(comm_fd, 404);
}

int main(int argc, char *argv[]) {
	//signal(SIGINT,ctrl);
	//string a=get_fig_binary();

	int c;
	int i;

	while ((c = getopt(argc, argv, "vi:")) != -1) {
		switch (c) {
		case 'v':
			verbose = true;
			break;
		case 'i':
			i=atoi(optarg);
			break;
		case '?':
			if (optopt == 'p')
				fprintf(stderr, "Option -%c requires an argument.\n", optopt);
			else
				fprintf(stderr, "Unknown reason");
			return 1;

		}
	}
	// initialize semaphore
	if (sem_init(&mutex, 0, 1) == -1) {
		fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));
		exit(-1);
	}
	if (sem_init(&mqueue_mutex, 0, 1) == -1) {
		fprintf(stderr, "sem_init: failed: %s\n", strerror(errno));
		exit(-1);
	}

	parse_addr_file(i);
	PORT = ntohs(self_addr.sin_port);

	sock_load = socket(PF_INET, SOCK_STREAM, 0);
	if (sock_load < 0) {
		return sock_load;
	}
	int opt = 1;
	setsockopt(sock_load, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	bind(sock_load, (struct sockaddr*)&to_load_addr, sizeof(to_load_addr));
	struct sockaddr_in balancer_addr = parse_addr(balancer_node.c_str(), balancer_node.length());
	int success=connect(sock_load, (struct sockaddr*) &balancer_addr, sizeof(balancer_addr));
	if(success!=0)
		fprintf(stderr,"Failed to connect with load balance!\n");


	/*create a new socket*/
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
	//	struct sockaddr_in servaddr;
	//	bzero(&self_addr, sizeof(self_addr));
	//	self_addr.sin_family = AF_INET;
	//	self_addr.sin_addr.s_addr = htons(INADDR_ANY);
	//	self_addr.sin_port = htons(PORT);
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
		sem_wait(&mutex);
		socket_all.insert(comm_fd);
		sem_post(&mutex);
		fprintf(stdout, "Accept the fd: %d!\n", comm_fd);

		pthread_t thread;
		pthread_create(&thread, NULL, handler, (void *) &comm_fd);

	}
	//	char buf[100]="HTML/login.css";
	//	string mes=get_file_content(buf);
	//	cout<<mes<<endl;

	return 0;

}

void *handler(void *arg) {
	int comm_fd = *(int *) arg;
	sem_wait(&mutex);
	string temp=to_string(socket_all.size())+"\r\n";
	do_write(sock_load,temp.c_str(),temp.length());
	sem_post(&mutex);
	while (true) {
		char buf[MAX_SIZE] = { 0 };
		http_content msg;
		int result = read_from_client(comm_fd, &msg, buf);
		if (result == -1) break;
		print_http_msg(&msg);
		if (result == 0) {
			fprintf(stderr, "request unreadable\n");
			if (!response_err(comm_fd, 400)) break;
		}
		// get username from cookies
		string userid = "";
		string cookie_value = "";
		if (msg.header.count("Cookie") != 0) {
			userid = get_id_from_cookies(msg.header["Cookie"], cookie_value);
		}

		// parse the first line
		string delim = " ";
		vector < string > word = split_string(msg.first_line, delim);

		if (word[2].compare("HTTP/1.1") != 0
				&& word[2].compare("HTTP/1.0") != 0) {
			if (!response_err(comm_fd, 400)) break;
			continue;
		}
		if (word[0].compare("GET") == 0) {
			if ((word[1].compare("/logout") == 0) || (word[1].compare("/logout.html") == 0)) { // clear cookies
				if (cookie_map.count(cookie_value) != 0) {
					cookie_map.erase(cookie_value);
				}
			}
			if (!response_get(comm_fd, word[1], userid)) break;
		} else if (word[0].compare("POST") == 0) {
			if (!response_post(comm_fd, word[1], &msg, userid)) break;
		} else
			if (!response_err(comm_fd, 405)) break;
	}
	cout << "Thread exits for fd: " << comm_fd << endl;
	sem_wait(&mutex);
	socket_all.erase(comm_fd);
	temp=to_string(socket_all.size())+"\r\n";
	do_write(sock_load,temp.c_str(),temp.length());
	sem_post(&mutex);
	close(comm_fd);
	pthread_detach((unsigned long int) pthread_self());
	pthread_exit(NULL);
}

