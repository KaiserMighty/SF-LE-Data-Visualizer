ROOTNAME=lawprocessor
FOPTION=_main
FOPTION2=_csv

RUNOPTIONS=Law100K.csv 1 police_district BAYVIEW MISSION TENDERLOIN

CC=gcc
CFLAGS= -g -I.
# To add libraries, add "-l <libname>", for multiple repeat prior for each lib.
LIBS =-l pthread
DEPS = 
ARCH = $(shell uname -m)


OBJ = $(ROOTNAME)$(FOPTION).o $(ROOTNAME)$(FOPTION2).o
CSVOBJ =$(ROOTNAME)$(FOPTION2).o CSVTest.o 

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

$(ROOTNAME)$(FOPTION): $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)  $(LIBS)

clean:
	rm $(ROOTNAME)$(FOPTION).o $(ROOTNAME)$(FOPTION)

run: $(ROOTNAME)$(FOPTION)
	./$(ROOTNAME)$(FOPTION) $(RUNOPTIONS)

vrun: $(ROOTNAME)$(FOPTION)
	valgrind ./$(ROOTNAME)$(FOPTION) $(RUNOPTIONS)

$(ROOTNAME)$(FOPTION2): $(CSVOBJ)
	$(CC) -o $@ $^ $(CFLAGS)  $(LIBS)

test: $(ROOTNAME)$(FOPTION2) $(CSVOBJ)
	./$(ROOTNAME)$(FOPTION2) 
