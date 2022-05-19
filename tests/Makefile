FLAGS=-O3
INCLUDE_PATH=-I$(HOME)/deploy/include -I../src/
LIB_PATH=-L$(HOME)/deploy/lib64
LIBS=-lpmemobj -lsqlite3
DEFS=-D__INFO -D__BENCHMARK -D__ASSERT

all: threaded_test mpi_naive mpi_opt
mpi_naive:
	mpic++ $(FLAGS) $(INCLUDE_PATH) $(LIB_PATH) -std=c++17 $(DEFS) mpi_naive_merge.cpp -o mpi_naive_merge $(LIBS) -fopenmp
mpi_opt:
	mpic++ $(FLAGS) $(INCLUDE_PATH) $(LIB_PATH) -std=c++17 $(DEFS) mpi_opt_merge.cpp -o mpi_opt_merge $(LIBS) -fopenmp
threaded_test:
	g++ $(FLAGS) $(INCLUDE_PATH) $(LIB_PATH) -std=c++17 $(DEFS) threaded_test.cpp -o threaded_test $(LIBS) -fopenmp
clean:
	rm -rf mpi_naive_merge mpi_opt_merge threaded_test *~
