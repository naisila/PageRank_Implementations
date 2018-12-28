/**
    PageRank in Parallel
    Original code in https://github.com/papadopoulosc/pagerank/blob/master/1st_parallel/pagerank_openmp.c
    Modified by
    @author Endi Merkuri
*/
#include <cstdio>
#include <cmath>
#include <cstring>
#include <map>
#include <vector>
#include <omp.h>
using namespace std;

struct Node
{
	int index;
	double p_t0;
	double p_t1;
	vector<int> links;
	int outLinkNo;
};

// Number of nodes
int N;
// Convergence threshold and algorithm's parameter beta
double threshold, beta;
// Graph's nodes
vector<Node> *nodes;
int* indices;

bool readFile(char* filename) {
	FILE *fid;
	FILE *file;
	int from, to;

	file = fopen(filename, "r");
	if (file == NULL) {
		printf("File exception!\n");
		return false;
	}
	int a, b;
	int no = 0;
	// First pass needed to find the max node index
	while ( !feof(file)) {
		if ( fscanf(file, "%d\t%d\n", &a, &b)) {
			int max = (a > b) ? a : b;
			if ( max > no )
				no = max+1;
		}
	}
	fclose(file);
	indices = new int[no];
	for ( int i = 0; i < no; i++) {
		indices[i] = 0;
	}
	// Populate the nodes list
	fid = fopen(filename, "r");
	if (fid == NULL) {
		printf("File exception!\n");
		return false;
	}
	int i = 0;
	int temp;
	while (!feof(fid)) {
		if (fscanf(fid,"%d\t%d\n", &from,&to)) {
			// If the first node in the current edge
         // has not been seen before store its index and create its entry
			if ( indices[from] == 0 ) {
				indices[from] = i + 1;
				nodes->at(i).index = from;
				nodes->at(i).links.push_back(to);
				nodes->at(i).outLinkNo++;
				i++;
			} else {
			// If the node is already created add the destination node at the at of 
         // this nodes' outgoing links list.
				nodes->at(indices[from]-1).links.push_back(to);
				nodes->at(indices[from]-1).outLinkNo++;
			}

			// If the second node is a dead-end create an entry for it
			if ( indices[to] == 0 ) {
				nodes->at(i).index = to;
				indices[to] = i + 1;
				i++;
			}
		}
	}
	fclose(fid);
	return true;
}

int main(int argc, char** argv) {
// Check if the right number of input arguments is provided
	if (argc < 5) {
		printf("usage: <file> <nodeNumber> <tolerance> <beta>\n");
		return 0;
	}

	// Parse the input arguments to their appropriate types
	char filename[256];
	strcpy(filename, argv[1]);
	N = atoi(argv[2]);
	threshold = atof(argv[3]);
	beta = atof(argv[4]);

	// Allocate the vector of nodes in the heap
	nodes = new vector<Node>(N);
	// Initialize the properties of each of the nodes
	for (int i = 0; i < N; i++) {
		nodes->at(i).outLinkNo = 0;
		nodes->at(i).links.assign(0, 0);
		nodes->at(i).p_t0 = 0;
		nodes->at(i).p_t1 = (double) 1 / N;
	}

	double pptime = omp_get_wtime();
	// Read the edges from the file given in the arguments
	bool readSuccess = readFile(filename);
	pptime = omp_get_wtime() - pptime;
	printf("Preprocessing time: %.10f seconds.\n",pptime);

	if ( !readSuccess )
		return 0;

	int iterations = 0;
	int index;

	double error = 1;
	double temp;
	double time = omp_get_wtime();
	while (error > threshold) {
		error = 0;
		// Set the new ranks of each node to 0
		for (int i = 0; i < N; i++) {
			nodes->at(i).p_t0 = nodes->at(i).p_t1;
			nodes->at(i).p_t1 = 0;
		}

		// Update the ranks of the neighbours of each of the nodes
		for (int i = 0; i < N; i++) {
			// Do not do anything for dead ends
			if (nodes->at(i).outLinkNo != 0) {
				temp = nodes->at(i).outLinkNo;
				for (int j = 0; j < temp; j++) {
					index = indices[nodes->at(i).links[j]] - 1;
					nodes->at(index).p_t1 += (double) nodes->at(i).p_t0 / temp;
				}
			}
		}

		// Calculate the leakage in the ranks
		double sum = 0.0;
		for (int i = 0; i < N; i++ ) {
			nodes->at(i).p_t1 = beta * nodes->at(i).p_t1;
			sum += nodes->at(i).p_t1;
		}

		double teleport = (double) (1 - sum) / N;
		// Add the teleportation leakage to all the nodes
		for (int i = 0; i < N; i++ ) {
			nodes->at(i).p_t1 += teleport;
			// Calculate the difference between the old and new ranks using L1-norm
			error += fabs(nodes->at(i).p_t1 - nodes->at(i).p_t0);
		}

		printf("Difference in iteration %d = %f.\n", iterations+1, error);
		iterations++;
	}
	time = omp_get_wtime() - time;
	printf("\n");

	int min = N > 10 ? 10 : N;
	/*for (int i = 0; i < 10; i++) {
		printf("Rank of %d = %.20f.\n",nodes->at(i).index,nodes->at(i).p_t1);
	}*/
	printf("\n");
	printf("Number of iterations: %d.\n", iterations);
	printf("Elapsed time: %.10f seconds.\n", time);
	return 1;
}
