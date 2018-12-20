TARGETS = backend master

all: $(TARGETS)

backend: backend.cc
	g++ -g -pthread $< -o $@
	
master: master.cc
	g++ $^ -lpthread -g -o $@
	
#pack:
#	rm -f submit-hw0.zip
#	zip -r submit-hw0.zip hello-world.cc README Makefile
#
#clean::
#	rm -fv $(TARGETS) submit-hw0.zip
