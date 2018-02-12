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
		name = "AdjformateWithDegree",
		description = "Transform the adj into adj with degree count. format: (#vid degrees neighborlist)"
)
public class AdjformateWithDegree 
implements GenericGraphTool{

	protected static final Pattern SEPERATOR =  Pattern.compile("[\t ]");
	
	public void saveGraph(String graphFilePath, String savePath){
		try {
			FileInputStream fin = new FileInputStream(graphFilePath);
			BufferedReader fbr = new BufferedReader(new InputStreamReader(fin));
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(savePath));
			String line;
			while((line = fbr.readLine()) != null){
				String [] values = SEPERATOR.split(line);
				int vid = Integer.valueOf(values[0]);
				bw.write(vid+" "+(values.length-1));
				for(int i = 1; i < values.length; i++){
					bw.write(" "+values[i]);
				}
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
		saveGraph(cmd.getOptionValue("i"), cmd.getOptionValue("sp"));	
		
	}

	@Override
	public boolean verifyParameters(CommandLine cmd) {
		if(!cmd.hasOption("sp")){
			return false;
		}
		return true;
	}
}
