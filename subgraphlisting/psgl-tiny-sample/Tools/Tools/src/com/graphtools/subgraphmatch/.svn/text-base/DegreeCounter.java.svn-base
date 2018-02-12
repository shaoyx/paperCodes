package com.graphtools.subgraphmatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;

import com.graphtools.GenericGraphTool;
import com.graphtools.utils.GraphAnalyticTool;

@GraphAnalyticTool(
		name = "Degree Counter",
		description = "Counting the Degree. format: (#vid degrees bigD smallD equalD bigL smallL)"
)
public class DegreeCounter 
implements GenericGraphTool{

	protected static final Pattern SEPERATOR =  Pattern.compile("[\t ]");
	
	private HashMap<Integer, Integer> degrees;
//	private String graphFilePath;
	
	
	public void loadDegree(String graphFilePath){
		try {
			FileInputStream fin = new FileInputStream(graphFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			String line;
			degrees = new HashMap<Integer, Integer>();
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				int vid = Integer.valueOf(values[0]);
				degrees.put(vid, Integer.valueOf(values[1]));
			}
			fbr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void saveDegreeInfo(String graphFilePath, String savePath){
		try {
			FileInputStream fin = new FileInputStream(graphFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(savePath));
			bw.write("#vid degrees bigD smallD equalD bigL smallL");
			bw.newLine();
			String line;
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				int vid = Integer.valueOf(values[0]);
				int smallDegree = 0;
				int bigDegree = 0;
				int equalDegree = 0;
				int smallLabel = 0;
				int bigLabel = 0;
				int nid;
				for(int i = 1; i < values.length; i++){
					nid = Integer.valueOf(values[i]);
					if(vid < nid) smallLabel++;
					if(vid > nid) bigLabel++;
					if(degrees.get(vid) < degrees.get(nid)){
						smallDegree++;
					}
					if(degrees.get(vid) > degrees.get(nid)){
						bigDegree++;
					}
					if(degrees.get(vid).equals(degrees.get(nid))){
						equalDegree++;
					}
				}
				bw.write(vid+" "+degrees.get(vid)+" " + bigDegree
						+" "+smallDegree+" "+equalDegree
						+" "+bigLabel+" "+smallLabel);
				bw.newLine();
			}
			fbr.close();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run(CommandLine cmd) {
		loadDegree(cmd.getOptionValue("dp"));
		saveDegreeInfo(cmd.getOptionValue("i"), cmd.getOptionValue("sp"));	
		
	}

	@Override
	public boolean verifyParameters(CommandLine cmd) {
		if(!cmd.hasOption("dp") || !cmd.hasOption("sp")){
			return false;
		}
		return true;
	}
}
