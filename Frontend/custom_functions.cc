/*
 * custom_functions.cc
 *
 *  Created on: Oct 13, 2018
 *      Author: Zihang Zhang
 */

#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <cstring>
#include <unistd.h>
#include <ctype.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <semaphore.h>
#include <errno.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <dirent.h>
#include <vector>
#include <algorithm>
#include <ctime>
#include <sys/file.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <openssl/md5.h>

using namespace std;

const int MAX_CONNECTION = 10;
const int MAX_CMD_SIZE = 1000;
const char local_domain[] = "localhost";
const char password[] = "cis505";

struct thread_pass_in {
	int id; // thread id
	int fd; // file descriptor
};

struct mail_address {
	string username; // username
	string domain; // domain
};

struct email { // the structure of an email
	mail_address sender; // sender
	vector <mail_address> receiver; // receivers
	string message; // message
};

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
	for (int i = 0; buffer[i] != 0; i++)
	{
		str += "0123456789ABCDEF"[buffer[i] / 16];
		str += "0123456789ABCDEF"[buffer[i] % 16];
	}
	return str;
}

bool need_update(vector <bool> keep) {
	// return whether the mailbox need update recursively
	if (keep.size() == 0) return false;
	if (keep.back()) {
		keep.pop_back();
		return need_update(keep);
	}
	else return true;
}

void total_size(const vector <long long int> & msg_bytes, const vector <bool> & keep, int& msg, long long int& size) {
	// calculate the total length and total message size
	size = 0;
	msg = 0;
	for (int i=0; i<msg_bytes.size(); i++) {
		if (keep[i]) {
			size += msg_bytes[i];
			msg++;
		}
	}
}

void print_vector(vector<string> v) {
	for (vector<string>::const_iterator i = v.begin(); i != v.end(); ++i)
		fprintf(stdout, "%s\n", (*i).c_str());
}

void clear_email(email * e) {
	// clear the values in e
	e->sender.username = "";
	e->sender.domain = "";
	e->receiver.clear();
	e->message = "";
}

bool do_write(int fd, const char *buf, int len) {
	int strn = strlen(buf);
	if (strn < len) len = strn; // do not include the null character
	int sent = 0;
	while (sent < len) {
		int n = write(fd, &buf[sent],len-sent);
		if (n<0)
			return false;
		sent += n;
	}
	return true;
}

bool compare_with_space(const char * str1, const char * str2) {
	// compare if str1 == str2 with case sensitivity, str1 is supposed to have /r/n at end and is allowed to have spaces before /r/n
	int len1 = strlen(str1);
	int len2 = strlen(str2);
	if (strncasecmp(str1, str2, len2) != 0) return 1; // the first few characters do not match
	if (strcmp(&str1[len1-2], "\r\n") != 0) return 1; // the last two characters are not "\r\n"
	for (int i = len2; i < len1 -2; i++) {
		if (str1[i] != ' ') return 1;
	}
	return 0;
}

bool is_vaild_mail_address(const char * str, mail_address * add) {
	// check whether the string contains a valid mail address, and write it in add
	int n = strlen(str);
	if (n < 5) return false; // it must contains at least 5 characters, eg. <p@q>
	if (str[0] != '<' || str[n-1] != '>') return false;
	const char *at = strchr(str,'@');
	if (at == NULL) return false; // it must contain @
	if (at - str == 1 || &str[n-1] - at == 1) return false; // not a valid address if no char before or after @
	string l = string(str);
	add->username = l.substr(1, at - str - 1);
	add->domain = l.substr(at - str + 1, n - (at - str) - 2);
	return true;
}


string read_mailbox(int fd, int n_msg) {
	// read the mailbox to get the message [n_msg] string
	FILE *stream;
	string msg = "";
	if ((stream = fdopen (dup(fd), "r")) == NULL) {
		fclose (stream);
		return msg;
	}
	rewind(stream); // read from start
	char buffer[MAX_CMD_SIZE+1];
	int n = 0;
	while (fgets(buffer, MAX_CMD_SIZE+1, stream) != NULL) {
		if (strncmp(buffer, "From ", 5) == 0) { // a new message is found
			n++;
			continue;
		}
		if (n == n_msg) msg.append(buffer);
		else if (n > n_msg) break;
	}

	fclose(stream);
	return msg;
}

bool scan_mailbox(int fd, vector <long long int> & msg_bytes, vector <bool> & keep, vector <string> & uid) {
	// read the mailbox to get the message number, size, uid
	msg_bytes.clear();
	keep.clear();
	uid.clear();
	FILE *stream;
	if ((stream = fdopen (dup(fd), "r")) == NULL) {
		fclose (stream);
		return false;
	}
	rewind(stream); // read from start
	char buffer[MAX_CMD_SIZE+1];
	int i = -1;
	string str = "";
	while (fgets(buffer, MAX_CMD_SIZE+1, stream) != NULL) {
		if (strncmp(buffer, "From ", 5) == 0) { // a new message is found
			i++;
			msg_bytes.push_back(0);
			keep.push_back(true);
			if (i>0) { // finish reading a message, find its uid
				unsigned char digestBuffer[MAX_CMD_SIZE+1];
				computeDigest(str.c_str(), msg_bytes[i-1], digestBuffer);
				string id = unsigned_char_to_hex(digestBuffer);
				uid.push_back(id);
				str="";
			}
			continue;
		}
		if (i>=0) {
			msg_bytes[i] += strlen(buffer);
			str += buffer;
		}
	}
	if (i>=0) { // calculate uid for the last message
		unsigned char digestBuffer[MAX_CMD_SIZE+1];
		computeDigest(str.c_str(), msg_bytes[i], digestBuffer);
		string id = unsigned_char_to_hex(digestBuffer);
		uid.push_back(id);
	}

	fclose(stream);
	return true;
}

bool update_mailbox(int fd, const vector <bool> & keep, string dir, string username) {
	// update the mailbox according the keep vector
	if (!need_update(keep)) { // do not need update
		return true;
	}
	string file_name;
	if (username == "") { // for convenience, empty username means to update mqueue file
		file_name = dir + "mqueue";
	}
	else {
		file_name = dir + username + ".mbox";
	}
	string temp_file = file_name + ".temp";
	int fd_temp = open(temp_file.c_str(), O_CREAT | O_RDWR);
	if (fd_temp == -1) return false;
	FILE *stream = fdopen(dup(fd), "r+");
	FILE *stream_temp = fdopen(fd_temp, "w+");
	if (stream == NULL || stream_temp == NULL) {
		fclose (stream);
		fclose (stream_temp);
		return false;
	}
	rewind(stream); // read from start
	char buffer[MAX_CMD_SIZE+1];
	int i = -1;
	while (fgets(buffer, MAX_CMD_SIZE+1, stream) != NULL) {
		if (strncmp(buffer, "From ", 5) == 0) { // a new message is found
			i++;
		}
		if (i >= 0 && keep[i]) { // the message is kept
			fprintf(stream_temp, "%s", buffer); // copy the contents to temp file
		}
	}

	fclose(stream);
	fclose(stream_temp);

	// delete the original file and rename the temp file
	if (remove(file_name.c_str()) != 0) return false;
	if (rename(temp_file.c_str(), file_name.c_str()) != 0) return false;
	chmod(file_name.c_str(), S_IRUSR|S_IRGRP|S_IROTH|S_IWUSR|S_IWGRP);

	return true;
}

