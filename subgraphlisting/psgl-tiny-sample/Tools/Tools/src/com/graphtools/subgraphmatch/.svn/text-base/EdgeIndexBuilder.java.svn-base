package com.graphtools.subgraphmatch;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;

import com.graphtools.GenericGraphTool;
import com.graphtools.utils.GraphAnalyticTool;
import com.graphtools.utils.bloom.BloomFilter;

@GraphAnalyticTool(
		name = "Edge Index Builder",
		description = "Build Edge Index in Bloom Filter. And support split into several parts"
)
public class EdgeIndexBuilder  
implements GenericGraphTool{

	protected static final Pattern SEPERATOR =  Pattern.compile("[\t ]");
	
	private ArrayList<BloomFilter> bfList;
	private int factor = 16;
	private int nhash = 8;
	private long size = 0;
	private int hashType = 0;
	
	private int npart = 1;
	
	public void setSize(long size){
		this.size = size;
	}
	
	public void setFactor(int factor){
		this.factor = factor;
	}
	
	public void setNHash(int nhash){
		this.nhash = nhash;
	}
	
	public void setHashType(int hashType){
		this.hashType = hashType;
	}
	
	public void setNumPartition(int npart){
		this.npart = npart;
	}
	
	public void loadGraph(String graphFilePath){
		try {
			FileInputStream fin = new FileInputStream(graphFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			bfList = new ArrayList<BloomFilter>();
			int singleSize = (int)((size*factor+npart)/npart);
			System.out.println("Build "+npart+" bloom filter with each size is "+singleSize);
			for(int i = 0; i < npart; i++){
				bfList.add(new BloomFilter(singleSize, nhash, hashType));
			}
			String line;
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				int vid = Integer.valueOf(values[0]);
//				ArrayList<Integer> al = new ArrayList<Integer>();
				for(int i = 1; i < values.length; ++i){
//					al.add(Integer.valueOf(values[i]));
					int vid2 = Integer.valueOf(values[i]);
					int pid = ((vid < vid2) ? vid : vid2) % npart;
					addEdge(pid, vid, vid2);
				}
//				vertexList.add(vid);
//				graph.put(vid, al);
			}
			fbr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private void addEdge(int pid, long s, long e){
		byte[] edgeKey = ByteBuffer.allocate(8).putLong((s < e) ? (s << 32L) | e : (e << 32L) | s).array();
		bfList.get(pid).add(new com.graphtools.utils.bloom.Key(edgeKey));
	}
	
	public boolean checkEdge(int pid, long s, long e){
		byte[] edgeKey = ByteBuffer.allocate(8).putLong((s < e) ? (s << 32L) | e : (e << 32L) | s).array();
		return bfList.get(pid).membershipTest(new com.graphtools.utils.bloom.Key(edgeKey));
	}
	
	/**
	 * save binary stream
	 * @param path
	 */
	public void saveEdgeIndex(String path){
			DataOutputStream out;
			try {
				for(int i = 0; i < npart; i++){
					out = new DataOutputStream(new FileOutputStream(path+"_"+i));
					bfList.get(i).write(out);
					out.flush();
					out.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
	}

	@Override
	public void run(CommandLine cmd) {
		size = Long.valueOf(cmd.getOptionValue("ne"));
		String graphPath = cmd.getOptionValue("i");
		String savePath = cmd.getOptionValue("sp");
		if(cmd.hasOption("nh"))
			nhash = Integer.valueOf(cmd.getOptionValue("nh"));
		if(cmd.hasOption("f"))
			factor = Integer.valueOf(cmd.getOptionValue("f"));
		if(cmd.hasOption("np"))
			npart = Integer.valueOf(cmd.getOptionValue("np"));
		if(cmd.hasOption("ht")){
			this.hashType = Integer.valueOf(cmd.getOptionValue("ht"));
			if(hashType != 0 && hashType != 1){
				System.out.println("Failed with the hashType = "+hashType);
				return ;
			}
		}
		System.out.println("nhash="+nhash+"\nhashtype="+hashType+"\nfactor="+factor+"\nnpart="+npart+"\nedgesize="+size);
		this.loadGraph(graphPath);
		this.saveEdgeIndex(savePath);
	}

	@Override
	public boolean verifyParameters(CommandLine cmd) {
		if(!cmd.hasOption("sp") || !cmd.hasOption("ne")){
			return false;
		}
		return true;
	}

}
