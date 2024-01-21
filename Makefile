#  Makefile, Dnyandeep Dhok--Cheetah  Seattle University, CPSC5300, Winter 2024

#  Compiler flags for C++ code
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c

#  Path to Berkeley DB installation
COURSE      = /usr/local/db6

#  Directory containing header files
INCLUDE_DIR = $(COURSE)/include

#  Directory containing library files
LIB_DIR     = $(COURSE)/lib

# following is a list of all the compiled object files needed to build the sql5300 executable
OBJS       = sql5300.o


#  General rule for compiling C++ files into object files
#   - %.o: %.cpp means "to make any .o file, find a corresponding .cpp file"
#   - g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<" compiles the .cpp file with the specified flag
%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"


#  Rule for linking object files into the executable 'sql5300'
#    - g++ -L$(LIB_DIR) -o $@ $^ -ldb_cxx -lsqlparser links the object files,
#     includes libraries from LIB_DIR, and links against libdb_cxx and libsqlparser
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser


# # Rule for cleaning up generated files (executable and object files)
# #   - rm -f sql5300 *.o removes the executable and all .o files
clean:
	rm -f sql5300 *.o
