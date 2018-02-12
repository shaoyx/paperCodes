package org.apache.giraph.comm.messages;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.giraph.subgraph.graphextraction.PartialPath;

public class RawVertexMessageStore{

	protected HashMap<Integer, ArrayList<PartialPath> > messages = new HashMap<Integer, ArrayList<PartialPath>> ();
	
	public RawVertexMessageStore() {}
	
	public ArrayList<PartialPath> getMessageByQuery(int qid) {
		return messages.get(qid);
	}
	
	public boolean isEmpty() {
		return messages.isEmpty();
	}

	public void addMessage(int qid, PartialPath pp) {
		if(messages.get(qid) == null) {
			messages.put(qid, new ArrayList<PartialPath>());
		}
		PartialPath nPp = new PartialPath(pp.getMaxLen());
		nPp.copy(pp);
		messages.get(qid).add(nPp);
	}

	public void clear() {
		messages.clear();
	}

}
