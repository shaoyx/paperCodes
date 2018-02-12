package com.graphtools;

import java.util.List;
import java.util.Scanner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.graphtools.subgraphmatch.DegreeCounter;
import com.graphtools.subgraphmatch.EdgeIndexBuilder;
import com.graphtools.subgraphmatch.NeighborLabelExtractor;
import com.graphtools.subgraphmatch.QueryEnumerator;
import com.graphtools.subgraphmatch.QuerySymmetryBroker;
import com.graphtools.subgraphmatch.RandomQueryGenerator;
import com.graphtools.subgraphmatch.RelabelByDegree;
import com.graphtools.utils.AnnotationUtils;
import com.graphtools.utils.GraphAnalyticTool;

public class GraphTools {

	private static Options OPTIONS;
	  static {
		    OPTIONS = new Options();
		    OPTIONS.addOption("h", "help", false, "Help");
		    OPTIONS.addOption("lt", "listTools", false, "List supported tools");
		    OPTIONS.addOption("tc", "toolClass", true, "Specifiy the tool class");
		    OPTIONS.addOption("i", "input", true, "Path of the input graph");
		    
		    /** Bahmani Method **/
		    OPTIONS.addOption("e", "error", true, "The error rate for the greedy approximation algortihm by Bahmani.");
		    
		    /** partition Method **/
		    OPTIONS.addOption("k", "ksize", true, "Size of the densest subgraph");
		    
		    /** relabel graph by degree order */
		    OPTIONS.addOption("lp","labelPath", true, "the path of the new label file");
		    OPTIONS.addOption("sp", "savePath", true, "the path of the save file");
		    
		    /** degree counter **/
		    OPTIONS.addOption("dp", "degreePath", true, "the path of the degree file");
		    
		    /** k truss */
		    OPTIONS.addOption("th", "threshold", true, "threshold for the ktruss");
		    OPTIONS.addOption("t", "type", true, "verification or execution");
		    
		    /** edge index build*/
		    OPTIONS.addOption("np", "npart", true, "number of partition. [default: 1]");
		    OPTIONS.addOption("nh", "nhash", true, "number of hash function. [default: 8]");
		    OPTIONS.addOption("ne", "nedge", true, "number of edges to be indexed");
		    OPTIONS.addOption("f", "factor", true, "factor. [default: 16]");
		    OPTIONS.addOption("ht", "htype", true, "hash type. [default: 0 (0: JenkinsHash; 1: MurmurHash)]");
		  }
	

	private static void run(CommandLine cmd) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		if(cmd.hasOption("tc") == false || cmd.hasOption("i") == false){
			printHelp();
			return;
		}
		
		String className = cmd.getOptionValue("tc");
//		System.out.println(cmd.getOptionValue("tc"));
//		System.out.println(cmd.getOptionValue("i"));
//		System.out.println("ClassName="+className);
		GenericGraphTool graphTool = (GenericGraphTool) Class.forName(className).newInstance();
		
		if(graphTool.verifyParameters(cmd) == false){
			printHelp();
			return;
		}
		long startTime = System.currentTimeMillis();
		graphTool.run(cmd);
		System.out.println("Runtime: "+ (System.currentTimeMillis() - startTime)+" ms");
	}

	private static void printHelp() {
	    HelpFormatter formatter = new HelpFormatter();
	    formatter.printHelp(GraphTools.class.getName(), OPTIONS, true);
		
	}

	private static void listTools() { 
		List<Class<?>> classes = AnnotationUtils.getAnnotatedClasses(
		      GraphAnalyticTool.class, "com.graphtools");
		    System.out.print("  Supported tools:\n");
		    for (Class<?> clazz : classes) {
		    	GraphAnalyticTool tool = clazz.getAnnotation(GraphAnalyticTool.class);
		        StringBuilder sb = new StringBuilder();
		        sb.append(tool.name()).append(" - ").append(clazz.getName())
		            .append("\n");
		        if (!tool.description().equals("")) {
		          sb.append("    ").append(tool.description()).append("\n");
		        }
		        System.out.print(sb.toString());
		    }
	}

	public static void main(String[] args) throws Exception{
		
		/* 1. parse the args */
	    CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(OPTIONS, args);
		
		if(cmd.hasOption("h")){
			printHelp();
			return ;
		}
		
		if(cmd.hasOption("lt")){
			listTools();
			return ;
		}

		/* 2. run the proper tool */
		run(cmd);
	}

	/***************************old interface***********************************/
	/*
	if("randquery".equals(args[0].toLowerCase())){
		new GraphTools().randomQueryGen(args, 1);
	}
	else if("extractlabel".equals(args[0].toLowerCase())){
		new GraphTools().extractLabel(args, 1);
	}
	else if("edgeindex".equals(args[0].toLowerCase())){
		new GraphTools().buildEdgeIndex(args, 1);
	}
	else if("automorphism".equals(args[0].toLowerCase())){
		new GraphTools().findAutomorphism(args, 1);
	}
	else if("ordergraph".equals(args[0].toLowerCase())){
		new GraphTools().orderGraph(args, 1);
	}
	else if("degreecounter".equals(args[0].toLowerCase())){
		new GraphTools().degreeCounter(args, 1);
	}
	else{
		System.out.println("Not supporeted Tools: "+ args[0]
				+"\nValid Input: randquery, extractlabel, edgeindex, " +
				"automorphism, ordergraph, degreecounter");
	}
	*/
//	public void randomQueryGen(String[] args, int shift){
//
//		RandomQueryGenerator rqg = new RandomQueryGenerator();
//		
//		System.out.println("Please input: <graph, label, Step, E, V> savePath");
//		
//		for(int i = 0; i < args.length; i++){
//			System.out.println(i+": "+args[i]);
//		}
//		
//		String graphFilePath = args[0+shift];
//		String labelFilePath = args[1+shift];
//		
//		int stepLimit = Integer.valueOf(args[2+shift]);
//		int edgeLimit = Integer.valueOf(args[3+shift]);
//		int vertexLimit = Integer.valueOf(args[4+shift]);
//		
//		String queryPath = args.length > (5+shift) ? args[5+shift] : "testquery";
//		String type = args.length > (6+shift) ? args[6+shift] : "dfs";
//		
//		System.out.println("graphFilePath="+graphFilePath);
//		System.out.println("labelFilePath="+labelFilePath);
//		System.out.println("stepLimit="+stepLimit);
//		System.out.println("edgeLimit="+edgeLimit);
//		System.out.println("vertexLimit="+vertexLimit);
//		
//		rqg.setStepLimit(stepLimit);
//		rqg.setEdgeLimit(edgeLimit);
//		rqg.setVertexLimit(vertexLimit);
//		rqg.setGeneratorType(type);
//		
//		rqg.loadGraph(graphFilePath);
//		rqg.loadLabel(labelFilePath);
//		
//		rqg.generateRandomQuery();
//		
//		rqg.saveRandomQuery(queryPath);
//	}
//
//
//	public void extractLabel(String[] args, int shift) {
//		System.out.println("Please input: <graph, label, labelNumber, savePath");
//		
//		for(int i = 0; i < args.length; i++){
//			System.out.println(i+": "+args[i]);
//		}
//		
//		String graphFilePath = args[0+shift];
//		String labelFilePath = args[1+shift];
//		int labelNumber = Integer.valueOf(args[2+shift]);
//		String savePath = args.length > (3+shift) ? args[3+shift] : "testneighborLabel";
//		
//		NeighborLabelExtractor nle = new NeighborLabelExtractor(labelNumber);
//		nle.loadGraph(graphFilePath);
//		nle.loadLabel(labelFilePath);
//		
//		nle.getNeighborLabel();
//		nle.saveNeighborLabels(savePath);
//	}
//	
//	/**
//	 * LiveJournal: edgesize = 42851237
//	 * @param args
//	 * @param shift
//	 */
//	public void buildEdgeIndex(String[] args, int shift){
//		System.out.println("Please input: <graph, edgesize, savepath, useCheck, isIndexExisted, factor, nhash, hashtype>");
//		
//		for(int i = 0; i < args.length; i++){
//			System.out.println(i+": "+args[i]);
//		}
//		
//		int index = 0;
//		String graphFilePath = args[index+shift];
//		index++;
//		int edgeSize = Integer.valueOf(args[index+shift]);
//		index++;
//		String savePath = args.length > (index+shift) ? args[index+shift] : "edgeindex";
//		index++;
//		boolean useCheck = args.length > (index+shift) ? Boolean.valueOf(args[index+shift]) : true;
//		index++;
//		boolean isIndexExisted = args.length >(index+shift) ? Boolean.valueOf(args[index+shift]): false;
//		index++;
//		int factor = args.length > (index + shift) ? Integer.valueOf(args[index+shift]) : 16;
//		index++;
//		int nhash = args.length > (index + shift) ? Integer.valueOf(args[index+shift]) : 8;
//		index++;
//		int hashType = args.length > (index + shift) ? Integer.valueOf(args[index+shift]) : 0;
//		
//		EdgeIndexBuilder edgeIndexBuilder = new EdgeIndexBuilder(edgeSize, factor, nhash, hashType);
//		
//		if(isIndexExisted){
//			edgeIndexBuilder.loadIndex(savePath);
//		}
//		else{
//			edgeIndexBuilder.loadGraph(graphFilePath);
//		}
//		
//		if(useCheck){
//			System.out.println("Testing Index: please input query edge (-1,-1) for return");
//		
//			Scanner scan = new Scanner(System.in);
//		
//			do{
//				if(scan.hasNext()){
//					int s = scan.nextInt();
//					int e = scan.nextInt();
//					if(s == -1 || e == -1){
//						break;
//					}
//					System.out.println(edgeIndexBuilder.checkEdge(s, e));
//				}
//			}while(true);
//		}
//		if(!isIndexExisted){
//			edgeIndexBuilder.saveEdgeIndex(savePath, true);
//		}
//	}
//
//	public void findAutomorphism(String[] args, int shift) {
//		
//		System.out.println("Please input: <graph, savePath, savetype={-1,0,1,2,...}, qeType, minmal(0,1)>");
//		
//		for(int i = 0; i < args.length; i++){
//			System.out.println(i+": "+args[i]);
//		}
//		
//		String graphFilePath = args[0+shift];
//		String savePath = args[1+shift];
//		int saveType = Integer.valueOf(args[2+shift]);
//		int qeType = (args.length > 3+ shift) ? Integer.valueOf(args[3+shift]) : 0;
//		int enable = (args.length > 4+ shift) ? Integer.valueOf(args[4+shift]) : 0;
//		
//
////		TopoQueryEnumerator qe = new TopoQueryEnumerator();
//		QueryEnumerator qe = (qeType == 0) ? new QueryEnumerator() : new QuerySymmetryBroker();
//		
//		qe.loadGraph(graphFilePath);
//		if(enable > 0){
//			qe.enableMinimal();
//		}
//		qe.doEnumeration();
//		/* save two files: query graph + edgeOrientation list */
//		qe.setSaveType(saveType);
//		qe.saveIndependentQuery(savePath);
//		
//	}
//
//	public void orderGraph(String[] args, int shift) {
//		System.out.println("Please input: <graph, relabelPath, savePath");
//		
//		for(int i = 0; i < args.length; i++){
//			System.out.println(i+": "+args[i]);
//		}
//		
//		String graphFilePath = args[0+shift];
//		String relabelPath = args[1+shift];
//		String savePath = args[2+shift];
//		
//		RelabelByDegree rbd = new RelabelByDegree();
//		rbd.loadLabel(relabelPath);
//		rbd.relabelGraph(graphFilePath, savePath);
//		
//	}
//	
//	public void degreeCounter(String[] args, int shift){
//		System.out.println("Please input: <graph, savePath");
//		
//		for(int i = 0; i < args.length; i++){
//			System.out.println(i+": "+args[i]);
//		}
//		
//		String graphFilePath = args[0+shift];
//		String savePath = args[1+shift];
//		
//		DegreeCounter dc = new DegreeCounter();
//		dc.loadGraph(graphFilePath);
//		dc.saveDegreeInfo(savePath);
//	}
//*/
}
