#include <stdio.h>
#include <map>
#include <stdlib.h>
#include <vector>

using namespace std;

map<int, int> vidmap;
vector<int> neighbour;

int remain[60000000] = {0};

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
	int id = 0;
	int firstvid = -1;
	int vnum = 0;
	long long edgenum = 0;
//	fscanf(in, "%d %d", &s,&e);
	while(fscanf(in, "%d %d", &s, &e) != EOF) {
		if(vidmap.find(s) == vidmap.end()) {vidmap[s] = id; remain[id] = s; id++;}
		if(vidmap.find(e) == vidmap.end()) {vidmap[e] = id; remain[id] = e; id++;}
		if(firstvid == -1) firstvid = s;
	}
	fseek(in, 0, SEEK_SET);

	neighbour.clear();
	long long size;	
	int i;
	vnum = 1;
//	for(i = 0; i < id; i++){
//		printf("%d\n", remain[i]);
//	}
//fscanf(in, "%d %d", &s,&e);
	long long tot = 0;
	while(fscanf(in, "%d %d", &s, &e) != EOF) {
		if(s != firstvid) {
			size = neighbour.size();
			fprintf(out,"%d", firstvid);
			tot = tot + size * size;
			
			for(i = 0; i < size; i++) {
				fprintf(out," %d", neighbour[i]);
			}
			
			fprintf(out,"\n");
			neighbour.clear();
			remain[vidmap[firstvid]] = 0;
			firstvid = s;
			vnum++;
		}
		edgenum++;
		neighbour.push_back(e);
	}
	size = neighbour.size();
    remain[vidmap[firstvid]] = 0;
	//fprintf(out,"%d", firstvid);
	tot = tot + size * size;
	
	fprintf(out,"%d", firstvid);
	for(i = 0; i < size; i++) {
		fprintf(out," %d", neighbour[i]);
	}
	fprintf(out,"\n");
	
	for(i = 0; i < id; i++){
	//	printf("%d\n", remain[i]);
		if(remain[i])
			fprintf(out, "%d\n", remain[i]);
	}

	printf("disticd vid=%d total vertex=%d edge=%lld capacity=%lld\n", id, vnum, edgenum, tot);
	fclose(in);
	fclose(out);
	return 0;
}
