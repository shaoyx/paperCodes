package org.apache.giraph.graph;

import org.apache.giraph.utils.BitMap;
import org.apache.log4j.Logger;

public class VertexProperty {
	
	private static final Logger LOG = Logger.getLogger(VertexProperty.class);
	
	private int label;
	private int degree;
	private int bigDegree;
	private int smallDegree;
	private BitMap neighborLabelMap; /* only record the label exist or not */
	
	public VertexProperty(){
	}
	
	public VertexProperty(int label, int degree, int bigDegree, int smallDegree){
		this.label = label;
		this.degree = degree;
		this.bigDegree = bigDegree;
		this.smallDegree = smallDegree;
	}
	
	public int getLabel(){
		return label;
	}
	
	public int getDegree(){
		return degree;
	}
	
	public void setLabel(int label){
		this.label = label;
	}
	
	public void setDegree(int degree){
		this.degree = degree;
	}
	
	public void setLabelMap(BitMap bm){
		neighborLabelMap = bm;
	}
	
	public boolean checkneighborLabel(int label){
		return neighborLabelMap.get(label);
	}
	
	public boolean checkNeighborLabel(BitMap bm){
//		LOG.info("label="+label);
//		LOG.info("degree="+degree);
//		LOG.info("this bitMap: "+ neighborLabelMap.toString());
//		LOG.info("other bitMap: "+ bm.toString());
//		LOG.info("Cover result: "+ neighborLabelMap.cover(bm));
		return neighborLabelMap.cover(bm);
	}

	public int getSmallDegree() {
		return smallDegree;
	}

	public void setSmallDegree(int degree){
		this.smallDegree = degree;
	}
	
	public int getBigDegree() {
		return bigDegree;
	}
	
	public void setBigDegree(int degree){
		this.bigDegree = degree;
	}

}
