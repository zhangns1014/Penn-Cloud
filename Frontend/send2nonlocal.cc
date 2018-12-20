/*
 * send2nonlocal.cc
 * Program for delivering emails stored in mqueue file to non-local domains
 *
 *  Created on: Oct 15, 2018
 *      Author: Zihang Zhang
 */

#include "custom_functions.cc"
#include <netinet/in.h>
#include <arpa/nameser.h>
#include <resolv.h>
#include <sys/inotify.h>
#include <netdb.h>
#include <iostream>

bool aflag=0;
bool vflag=0;
char* directory; // the directory of all local emails

int communicate(FILE* stream, const char * send_msg, const char* expected_msg, char* buffer) {
	// send message, and check whether the response is expected
	if (fprintf(stream, "%s", send_msg) < 0) {
		return 1; // send message
	}
	if (vflag) fprintf(stderr, "C: %s", send_msg);
	if (fgets(buffer, MAX_CMD_SIZE+1, stream) == NULL) {
		return 2; // read a new line
	}
	if (vflag) fprintf(stderr, "S: %s", buffer);
	if (strncmp(buffer, expected_msg, strlen(expected_msg)) != 0) {
		return 3; // check message
	}
	return 0;
}

bool try_send_email(string sender, string receiver, string message) {
	mail_address s;
	mail_address r;
	if (!is_vaild_mail_address(sender.c_str(), &s) || !is_vaild_mail_address(receiver.c_str(), &r)) {
		return false; // check the validity of the address, and split the domain and username
	}

	// use res_query() to find the address
	const int size = 1024;
	unsigned char answer[size];
	int response = res_query(r.domain.c_str(), C_IN, T_MX, answer, size);
	if (response == -1) {
		if (vflag) fprintf(stderr, "Error with res_query()\n");
		return false;
	}
	else if (response == sizeof(answer)) {
		if (vflag) fprintf(stderr, "Buffer size too small, reply truncated\n");
		return false;
	}

	// parse the answer from server
	ns_msg m_handle;
	if (ns_initparse(answer, response, &m_handle) == -1) {
		if (vflag) fprintf(stderr, "Error with ns_initparse()\n");
		return false;
	}
	ns_rr parsed_record;
	unsigned char temp_name[size];
	char name[size];
	if (ns_parserr(&m_handle, ns_s_an, 0, &parsed_record) == -1) { // we only need the first answer
		if (vflag) fprintf(stderr, "Error with ns_parserr()\n");
		return false;
	}
	// get the server name
	const unsigned char *data = ns_rr_rdata(parsed_record);
	ns_name_unpack(answer, answer + response, data + sizeof(u_int16_t), temp_name, size);
	ns_name_ntop(temp_name, name, size);

	// connect to the server
	// Create a socket
	int fd = socket(PF_INET, SOCK_STREAM, 0);
	if (fd < 0) {
		if (vflag) fprintf(stderr, "Error with socket()\n");
		return false;
	}

	// Use the SMTP default port
	uint16_t port = 25;
	// Setup a Socket Address structure
	struct sockaddr_in SockAddr;
	SockAddr.sin_family = AF_INET;
	SockAddr.sin_port = htons(port);
	// use server name to find the IP address
	hostent * hp = gethostbyname(name);
	if (hp==(struct hostent *) 0)
	{
		fprintf(stderr, "Error with gethostbyname()\n");
		close(fd);
		return false;
	}
	memcpy((char *) &SockAddr.sin_addr, (char *) hp->h_addr, hp->h_length);

	// Connect the Socket
	if (connect(fd, (struct sockaddr*) &SockAddr, sizeof(SockAddr))){
		if (vflag) fprintf(stderr, "Error with connect()\n");
		close(fd);
		return false;
	}
	if (vflag) fprintf(stderr, "[%d] Connected to: %s\n", fd, name);

	// start streaming
	FILE *stream  = fdopen (fd, "r+");
	if (stream == NULL) {
		if (vflag) fprintf(stderr, "[%d] Error, cannot open the file descriptor\n", fd);
		fclose (stream);
		return false;
	}

	// read greeting message
	char buffer[MAX_CMD_SIZE+1];
	if (fgets(buffer, MAX_CMD_SIZE+1, stream) == NULL) { // read a new line
		fclose (stream);
		return false;
	}
	if (vflag) fprintf(stderr, "S: %s", buffer);
	if (strncmp(buffer, "220", 3) != 0) { // expected 220
		fclose (stream);
		return false;
	}

	int err;
	if ((err = communicate(stream, "HELO localhost\r\n", "250", buffer)) != 0) { // send HELO
		if (err == 3) communicate(stream, "QUIT\r\n", "221", buffer);
		fclose(stream);
		return false;
	}
	if ((err = communicate(stream, ("MAIL FROM:"+sender+"\r\n").c_str(), "250", buffer)) != 0) { // send MAIL
		if (err == 3) communicate(stream, "QUIT\r\n", "221", buffer);
		fclose(stream);
		return false;
	}
	if ((err = communicate(stream, ("RCPT to:"+receiver+"\r\n").c_str(), "250", buffer)) != 0) { // send RCPT
		if (err == 3) communicate(stream, "QUIT\r\n", "221", buffer);
		fclose(stream);
		return false;
	}
	if ((err = communicate(stream, "DATA\r\n", "354", buffer)) != 0) { // send DATA
		if (err == 3) communicate(stream, "QUIT\r\n", "221", buffer);
		fclose(stream);
		return false;
	}
	if ((err = communicate(stream, (message+"\r\n.\r\n").c_str(), "250", buffer)) != 0) { // send message
		if (err == 3) communicate(stream, "QUIT\r\n", "221", buffer);
		fclose(stream);
		return false;
	}
	communicate(stream, "QUIT\r\n", "221", buffer); // send QUIT
	fclose(stream);

	return true;
}

bool take_info_from_first_line(char * buffer, string & sender, string & receiver) {
	// from the first line of a message (format: From <sender> to <receiver> time), store the information
	int n = strlen(buffer);
	string buffer_str = string(buffer);
	size_t sender_s = buffer_str.find('<', 0); // the start of sender
	if (sender_s == string::npos) return false;
	size_t sender_e = buffer_str.find('>', sender_s + 1); // the end of sender
	if (sender_e == string::npos) return false;
	sender = buffer_str.substr(sender_s, sender_e - sender_s + 1);

	size_t receiver_s = buffer_str.find('<', sender_e + 1); // the start of receiver
	if (receiver_s == string::npos) return false;
	size_t receiver_e = buffer_str.find('>', receiver_s + 1); // the end of receiver
	if (receiver_e == string::npos) return false;
	receiver = buffer_str.substr(receiver_s, receiver_e - receiver_s + 1);

	return true;
}

int main(int argc, char *argv[])
{
	int arg_name;
	opterr = 0;
	while ((arg_name = getopt(argc, argv, "av")) != -1)
	{
		switch (arg_name)
		{
		case 'a':
			aflag = 1;
			break;
		case 'v':
			vflag = 1;
			break;
		case '?':
			if (isprint(optopt))
				fprintf (stderr, "Error! Unknown option `-%c'.\n", optopt);
			else
				fprintf (stderr, "Error! Unknown option character `\\x%x'.\n", optopt);
			exit(-1);
			break;
		default:
			abort();
		}
	}

	if (aflag) {
		fprintf(stderr, "Author: Zihang Zhang / zzhangau\n");
		exit(1);
	}

	// read other arguments (directory)
	int ind = optind;
	if (ind >= argc) {// no more arguments
		if (vflag) {
			fprintf(stderr, "Please specify the file directory for local host\n");
		}
		exit(1);
	}
	else {
		directory = argv[ind];
	}

	string filename = string(directory)+"mqueue"; // mqueue directory

	do {
		// read the mqueue file
		int fd = open(filename.c_str(), O_RDWR);
		if (fd == -1) {
			if (vflag) fprintf(stderr, "[%d] Error, cannot open the file directory\n", fd);
			exit(-1);
		}
		if (flock(fd, LOCK_EX) == -1) { // if smtp server is holding the lock, it will return wait
			if (vflag) fprintf(stderr, "[%d] Error, cannot lock the file descriptor\n", fd);
			close(fd);
			exit(-1);
		}
		FILE *stream  = fdopen(fd, "r+");
		if (stream == NULL) {
			if (vflag) fprintf(stderr, "[%d] Error, cannot open the file descriptor\n", fd);
			close(fd);
			exit(-1);
		}

		// read the email and try to send it
		char buffer[MAX_CMD_SIZE+1];
		string sender = "";
		string receiver = "";
		string message = "";
		vector <bool> keep;
		keep.clear();
		int n_msg = -1; // record which message it is
		while (fgets(buffer, MAX_CMD_SIZE+1, stream) != NULL) {
			if (strncmp(buffer, "From ", 5) == 0) { // a new message is found
				if (n_msg >= 0) { // not the first message
					if (try_send_email(sender, receiver, message)) {
						if (vflag) fprintf(stderr, "Mail from %s to %s sent successfully\n", sender.c_str(), receiver.c_str());
						keep.push_back(false);
					}
					else {
						if (vflag) fprintf(stderr, "Fail to send mail from %s to %s\n", sender.c_str(), receiver.c_str());
						keep.push_back(true);
					}
				}
				message = ""; // clear old information and add new information
				take_info_from_first_line(buffer, sender, receiver);
				n_msg++;
				continue;
			}
			message.append(buffer);
		}
		if (n_msg >= 0) { // send the last message
			if (try_send_email(sender, receiver, message)) {
				if (vflag) fprintf(stderr, "Mail from %s to %s sent successfully\n", sender.c_str(), receiver.c_str());
				keep.push_back(false);
			}
			else {
				if (vflag) fprintf(stderr, "Fail to send mail from %s to %s\n", sender.c_str(), receiver.c_str());
				keep.push_back(true);
			}
		}

		if (!update_mailbox(fd, keep, directory, "") && vflag) {
			fprintf(stderr, "[%d] Error, some mails are not deleted\n", fd);
		}

		fclose(stream);

		// monitor the file change
		int fd_monitor = inotify_init();
		if (fd_monitor < 0 && vflag) {
			fprintf(stderr, "[%d] Error, inotify_init() fails\n", fd_monitor);
			exit(-1);
		}
		int wd = inotify_add_watch(fd_monitor, filename.c_str(), IN_MODIFY);
		if (wd < 0 && vflag) {
			fprintf(stderr, "[%d] Error, inotify_add_watch() fails\n", fd_monitor);
			exit(-1);
		}
		char buffer_monitor[1025];
		int read_len = read(fd_monitor, buffer_monitor, 1024); // monitor the file change
		if (read_len < 0) {
			if (vflag) fprintf(stderr, "[%d] Error in reading fd_monitor\n", fd_monitor);
			exit(-1);
		}
		if (vflag) fprintf(stderr, "[%d] File modification detected, start trying sending emails\n", fd_monitor);
		close(fd_monitor);

	} while(true);
	return 0;
}
