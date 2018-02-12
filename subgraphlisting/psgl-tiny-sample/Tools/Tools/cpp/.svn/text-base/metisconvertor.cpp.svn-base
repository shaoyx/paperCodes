#include <stdio.h>
#include <map>
#include <stdlib.h>
#include <vector>

using namespace std;

map<int, int> vidmap;
vector<int> neighbour;

int main(int argc, char** args) {
	/*The input graph must be undirected.*/
	char* input = args[1];
	char* output = args[2];
	FILE* in;
    FILE* out;
	in = fopen(input, "r");
	out = fopen(output, "w");

	vidmap.clear();
	int s,e;
	int id = 1;
	int firstvid = -1;
	int vnum = 0;
	long long edgenum = 0;

	while(fscanf(in, "%d %d", &s, &e) != EOF) {
		if(vidmap.find(s) == vidmap.end() ) vidmap[s] = id++;
		edgenum++;
		//if(vidmap.find(e) == vidmap.end() ) vidmap[e] = id++;
		if(firstvid == -1) firstvid = s;
	}
	vnum = id - 1;
	fseek(in, 0, SEEK_SET);
	neighbour.clear();

	int size;	
	int i;
	while(fscanf(in, "%d %d", &s, &e) != EOF) {
		if(s != firstvid) {
			size = neighbour.size();
			for(i = 0; i < size; i++) {
				fprintf(out,"%d ", vidmap[neighbour[i]]);
			}
			fprintf(out,"\n");
			neighbour.clear();
			firstvid = s;
		}
		neighbour.push_back(e);
	}
	size = neighbour.size();
	for(i = 0; i < size; i++) {
		fprintf(out,"%d ", vidmap[neighbour[i]]);
	}
	fprintf(out,"\n");
	printf("total vertex=%d edge=%lld\n", vnum, edgenum>>1L);
	fclose(in);
	fclose(out);
	return 0;
}
