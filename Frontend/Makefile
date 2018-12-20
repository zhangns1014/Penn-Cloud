TARGETS = Frontendserver server balancer AdminConsole

all: $(TARGETS)

Frontendserver: Frontendserver.cc
	g++ $^ -I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib -lcrypto -lpthread -g -o $@

server: send2nonlocal.cc
	g++ $^ -I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib -lresolv -lcrypto -lpthread -g -o $@

balancer: balancer.cc
	g++ $^ -lpthread -g -o $@
	
AdminConsole: AdminConsole.cc
	g++ $^ -lpthread -g -o $@

pack:
	rm -f submit-hw2.zip
	zip -r submit-hw2.zip *.cc README Makefile

clean::
	rm -fv $(TARGETS) *~

realclean:: clean
	rm -fv cis505-hw2.zip