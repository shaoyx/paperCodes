package com.graphtools.subgraphmatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;

import com.graphtools.GenericGraphTool;
import com.graphtools.utils.GraphAnalyticTool;


@GraphAnalyticTool(
name = "RelabelByDegree",
description = "order the graph based on vertex's degree, " +
		"and relabel the graph by the order"
)
public class RelabelByDegree  
implements GenericGraphTool{
	protected static final Pattern SEPERATOR =  Pattern.compile("[\t ]");
	
	private HashMap<Integer,Integer> remap;
	
	public RelabelByDegree(){
		remap = new HashMap<Integer,Integer>();
	}
	
	public void loadLabel(String labelPath){
		try {
			FileInputStream fin = new FileInputStream(labelPath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			String line;
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				remap.put(Integer.valueOf(values[0]), Integer.valueOf(values[1]));
			}
			fbr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void relabelGraph(String graphFilePath, String savePath){
		try {
			FileInputStream fin = new FileInputStream(graphFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));	
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter( 
					new FileOutputStream(savePath)));
			String line;
			int cnt = 0;
			System.out.println((long)(Runtime.getRuntime().freeMemory() / 1000/1000)+ " " 
					+ (long)(Runtime.getRuntime().totalMemory()/ 1000/1000)+" "
					+(long)(Runtime.getRuntime().maxMemory() / 1000/1000));
			int dropCnt = 0;
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				cnt++;
				
				StringBuilder ans = new StringBuilder();
				if(cnt % 100000 == 0)
					System.out.println((long)(Runtime.getRuntime().freeMemory() / 1000/1000)+ " " 
							+ (long)(Runtime.getRuntime().totalMemory()/ 1000/1000)+" "
							+(long)(Runtime.getRuntime().maxMemory() / 1000/1000));
				int element = 0;
				
				for(int i = 0; i < values.length; ++i){
//					if(i != 0)
//						ans +=" ";
//					ans += remap.get(Integer.valueOf(values[i]));
					
					if(remap.get(Integer.valueOf(values[i])) == null){
						if(i == 0) {dropCnt++; break;} /* drop the line */
						continue; /* drop the neighbor */
					}
					
					if(i != 0){
						ans.append(" ");
					}
					ans.append(remap.get(Integer.valueOf(values[i])));
					element++;
//					bw.write(remap.get(Integer.valueOf(values[i])).toString());
				}
				if(element > 1){
					bw.write(ans.toString());
					bw.newLine();
				}
			}
			
			System.out.println("Drop Vertex: "+ dropCnt);
			
			fbr.close();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run(CommandLine cmd) {
		loadLabel(cmd.getOptionValue("lp"));
		relabelGraph(cmd.getOptionValue("i"), cmd.getOptionValue("sp"));		
	}

	@Override
	public boolean verifyParameters(CommandLine cmd) {
		if(!cmd.hasOption("lp") || !cmd.hasOption("sp")){
			return false;
		}
		return true;
	}
}
