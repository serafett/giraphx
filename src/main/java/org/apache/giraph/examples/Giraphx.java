/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.examples;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.VertexCombiner;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.graph.WorkerContext;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.giraph.lib.TextVertexInputFormat.TextVertexReader;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.giraph.lib.TextVertexOutputFormat.TextVertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;
import org.json.JSONException;
import org.mortbay.log.Log;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
public class Giraphx extends
        EdgeListVertex<LongWritable, Text,
        Text, Text> implements Tool {
    

	/** Configuration */
    private Configuration conf;
    /** The shortest paths id */
    public static String SOURCE_ID = "Giraphx.sourceId";
    /** Default shortest paths id */
    public static long SOURCE_ID_DEFAULT = 1;
    
    public static int MAX_SUPERSTEPS_DEFAULT = 90;
    public static String MAX_SUPERSTEPS_CONF = "Giraphx.maxSupersteps";
    public static String VERSION_OF_JOB_DEFAULT = "empty";
    public static String VERSION_OF_JOB_CONF = "Giraphx.versionOfJob";
    public static String NUM_VERTICES_CONF = "giraph.numVertices";
	public static String PARTITIONER_TYPE = "giraph.partitionerType";

	
    public int MAX_SUPERSTEPS = MAX_SUPERSTEPS_DEFAULT;
    public String VERSION_OF_JOB = VERSION_OF_JOB_DEFAULT;
    public int NUM_VERTICES = -1;


	private int colorsLength;
	private boolean available[];
	
	private static double tolerance = 0.000001;  // Used for pageRank termination condition
	
	boolean ovcFlag=false;
	private boolean token=false;

    private List<LongWritable> incomingEdgeIndexList = new ArrayList<LongWritable>();
	private Map<Long,String> myForkMap = new HashMap<Long,String>();
	private List<Long> inReqList = new ArrayList<Long>();
	private List<LongWritable> allEdgeIndexList = new ArrayList<LongWritable>();

	private int nonlocalNeighborCount=0;
	private Map<LongWritable,Boolean> isLocalMap = new HashMap<LongWritable,Boolean>();
	private Map<LongWritable,Boolean> outReqMap = new HashMap<LongWritable,Boolean>();
	private Map<Long,String> msgListMainMap = new HashMap<Long,String>();
	private Map<LongWritable, Text> vertexValues = new HashMap<LongWritable, Text>();
	
	private List<Long> fakeIdList  = new ArrayList<Long>();
	long fakeId;
	int length;
	MutableInt lastUpdateSuperstep;


	@Override  
    public void compute(Iterator<Text> msgIterator) {
		this.token = WorkerContext.token;  // This will be used for tGiraphx
		//Log.info("SEREF: Token is: " + token);

		if(getSuperstep()==0){  // Initialize configuration parameters
    		MAX_SUPERSTEPS = getContext().getConfiguration().getInt(MAX_SUPERSTEPS_CONF,
    				MAX_SUPERSTEPS_DEFAULT);
    		NUM_VERTICES = getContext().getConfiguration().getInt(NUM_VERTICES_CONF,
    				-1);
    		VERSION_OF_JOB = getContext().getConfiguration().get(VERSION_OF_JOB_CONF,
    	    		VERSION_OF_JOB_DEFAULT);
    		length = String.valueOf(NUM_VERTICES).length();
		}
		
		if(getSuperstep() >= MAX_SUPERSTEPS){
			voteToHalt();
			needsOperation = false;
			return;
		}
		
		if(VERSION_OF_JOB.contains("dGiraph")){
			compute_dGiraph(msgIterator);
		}
		else if(VERSION_OF_JOB.equals("tGiraphx_coloring")){
			compute_tGiraphx_coloring(msgIterator,token);
		}
		else if(VERSION_OF_JOB.equals("orig_pagerank")){
			compute_original_pagerank(msgIterator);
		}
		else if(VERSION_OF_JOB.equals("giraphx_pagerank")){
			compute_giraphx_pagerank(msgIterator);
		}
		else{
			Log.info("SEREF:\t ERROR:INVALID JOB TYPE: "+VERSION_OF_JOB);
			voteToHalt();
		}
    }
	
	public void compute_original_pagerank(Iterator<Text> msgIterator) {
		// Get the number of vertices that have a change in pagerank greater than tolerance
		LongSumAggregator prsumAggreg = (LongSumAggregator) getAggregator("prsum");    	
    	long prDiffSum = -1;
    	if(getSuperstep() > 1){
    		prDiffSum = GiraphxWorkerContext.prDifferenceSum;
    		if(getVertexId().get() == 0){
    			Log.info("SEREF Pagerank difference is: " + prDiffSum);
    		}
    	}
    	
    	// Halt if max superstep is reached or no vertex has a significant value change
        if (getSuperstep() > MAX_SUPERSTEPS || prDiffSum == 0) {
        	voteToHalt();
        	return;
        }

		if(getSuperstep()==0){  // Init pageRank value at ss=0
			needsOperation = false;
			double init = 1.0f;// / (double)NUM_VERTICES;
			//tolerance = init / 100.0;
			setVertexValue(new Text(init+""));
		}
		
        if (getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {
                sum += Double.parseDouble(msgIterator.next().toString());
            }
            DoubleWritable vertexValue =
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum);
            double diff = Math.abs(	Double.parseDouble(getVertexValue().toString()) 
            		- Double.parseDouble(vertexValue.toString()) );
//            Log.info("SEREF: Old and new pagerank values are: " + getVertexValue() +" -> " + vertexValue);
            
            String ctr_name = "PageRank difference in superstep "+getSuperstep();
            getContext().getCounter("Giraph Stats", ctr_name).increment((long) (diff/tolerance));
            if(diff>tolerance){
            	prsumAggreg.aggregate(1);
            }
            setVertexValue(new Text(vertexValue.toString()));
       }

        if (getSuperstep() < MAX_SUPERSTEPS && prDiffSum != 0) {
            sendMsgToAllEdges(
                new Text((Double.parseDouble(getVertexValue().toString()) 
                		/ getNumOutEdges())+""));
        } 
    }

	private void compute_giraphx_pagerank(Iterator<Text> msgIterator) {
		// Get the number of vertices that have a change in pagerank greater than tolerance
		LongSumAggregator prsumAggreg = (LongSumAggregator) getAggregator("prsum");    	
    	long prDiffSum = -1;
    	if(getSuperstep() > 1){
    		prDiffSum = GiraphxWorkerContext.prDifferenceSum;
    		if(getVertexId().get() == 0){
    			Log.info("SEREF Pagerank difference is: " + prDiffSum);
    		}
    	}

        if (getSuperstep() > MAX_SUPERSTEPS || prDiffSum == 0) {
            voteToHalt();
    		vertexValues.clear();
            return;
        }
        
		if(getSuperstep()==0){
			needsOperation = false;
			double init = 1.0f;// / (double)NUM_VERTICES;
			//tolerance = init / 100.0;
			setVertexValue(new Text(init+""));
			
        	destEdgeIndexList.remove(getVertexId());
            Text value =
                    new Text((Double.parseDouble(getVertexValue().toString()) 
                    		/ getNumOutEdges())+"");

    		sendMsgToAllEdges(new Text("M1:"+getVertexId()+":"+value));
    		return;
    	}  
 
        double diff=-1;
        
        if (getSuperstep() >= 1) {
            double sum = 0;
            while (msgIterator.hasNext()) {  // in this loop nonlocal neighbor pagerank values are read
            	String tmp=msgIterator.next().toString();
            	long id = Long.parseLong(tmp.split(":")[1]);
        		double val = Double.parseDouble(tmp.split(":")[2]);
                vertexValues.put(new LongWritable(id), new Text(val+""));
                if(getSuperstep()==1){
                	incomingEdgeIndexList.add(new LongWritable(id));
                }
            }
            if(getSuperstep()==1){
            	incomingEdgeIndexList.remove(getVertexId());
            }else{
            	readPagerankFromNeighbors(vertexValues,incomingEdgeIndexList);
            }
            Iterator<Entry<LongWritable, Text>> vit = vertexValues.entrySet().iterator();
         	while(vit.hasNext()){
         		Entry<LongWritable, Text> e = vit.next();
         		double value = Double.parseDouble(e.getValue().toString());
         		sum += value;
         	}	
            DoubleWritable vertexValue =
                new DoubleWritable((0.15f / getNumVertices()) + 0.85f * sum);
            diff = Math.abs(	Double.parseDouble(getVertexValue().toString()) 
            		- Double.parseDouble(vertexValue.toString()) );

            String ctr_name = "PageRank difference in superstep "+getSuperstep();
            getContext().getCounter("Giraph Stats", ctr_name).increment((long) (diff/tolerance));
            if(diff>tolerance){
            	prsumAggreg.aggregate(1);
                setVertexValue(new Text(vertexValue.toString()));
            }
       }
       
        if (getSuperstep() < MAX_SUPERSTEPS && diff > tolerance) {
            long edges = getNumOutEdges();
            String vval = (Double.parseDouble(getVertexValue().toString()) / edges)+"";
            String msg = "M1:"+getVertexId()+":"+vval;
            sendMsgToDistantEdges(new Text(msg), destEdgeIndexList);
        }
        needsOperation = false;
        voteToHalt();
        
	}

	private void compute_tGiraphx_coloring(Iterator<Text> msgIterator,
			boolean token2) {
		
        if (getSuperstep() > MAX_SUPERSTEPS) {   
            voteToHalt();
    		vertexValues.clear();
    		available=null;
    		needsOperation = false;
            return;
        }
		
        MaxAggregator maxAggreg = (MaxAggregator) getAggregator("max");
    	
    	//Log.info("SEREF Vertex "+getVertexId()+" started at superstep "+getSuperstep());
        if(getSuperstep()==0){  // So that each vertex also learns its incoming edge IDs
        	destEdgeIndexList.remove(getVertexId());
    		sendMsgToAllEdges(new Text("M1:"+getVertexId()+":"+getVertexValue()));
    		return;
    	}  
        
    	Iterator<Text> it = msgIterator;
    	while (it.hasNext()) {
    		String tmp=it.next().toString();
    		//Log.info("SEREF Incoming Message is:\t"+tmp);
    		long neighbor_id = Long.parseLong(tmp.split(":")[1]);
    		
    		if(getSuperstep()==1){
    			incomingEdgeIndexList.add(new LongWritable(neighbor_id));
    		}
    		else{
    			needsOperation = true;
    			msgListMainMap.put(neighbor_id, tmp);
    		}
    	}
        
		//Log.info("SEREF msgListMainMap size is:\t"+msgListMainMap.size());
        if(getSuperstep()==1){  // initialize all edges list at superstep 1
        	incomingEdgeIndexList.remove(getVertexId());
        	initAllEdgeIndexList();        	
        	isInternal = checkIsInternal(allEdgeIndexList);
			if(isInternal){
    			getContext().getCounter("Giraph Stats", "Local vertex count").increment(1);
			}
        	sendMsgToDistantEdges(new Text("M1:"+getVertexId()+":"+getVertexValue()), allEdgeIndexList);
        	maxAggreg.aggregate(new DoubleWritable(allEdgeIndexList.size()));
        	return;
        }
        
        if(getSuperstep()==2){
        	int degree = (int) GiraphxWorkerContext.maxDegree;
        	colorsLength = 2*degree+5;
        }

        //Log.info("SEREF isInternal and token values Message is:\t"+isInternal+"\t"+token);    
    	boolean isUpdated=false;    	
        if(isInternal || (!isInternal && token) ){
        	isUpdated=operate_tGiraph_coloring(msgListMainMap);
        }
           	
    	if(isUpdated){
        	String msg = "M1:"+getVertexId()+":"+getVertexValue().toString();
        	//Log.info("SEREF sent message "+msg);
        	sendMsgToDistantEdges(new Text(msg), allEdgeIndexList);
    	}
    	
		available=null;   	    
        voteToHalt();
	}
		
    public void compute_dGiraph(Iterator<Text> msgIterator) {  
    	
        if (getSuperstep() == MAX_SUPERSTEPS) {   
            voteToHalt();
            vertexValues.clear();
    		available=null;
    		needsOperation = false;
            return;
        }
        
        MaxAggregator maxAggreg = (MaxAggregator) getAggregator("max");  
        
        long my_id = getVertexId().get();
    	

    	//Log.info("---- SEREF Vertex "+getVertexId()+" started at superstep "+getSuperstep()+" ----");
    	if(getSuperstep()==0){  // So that each vertex also learns its incoming edge IDs
    		destEdgeIndexList.remove(getVertexId());
    		sendMsgToAllEdges(new Text("M1:"+getVertexId()+":"+getVertexValue()));
    		return;
    	}    
    	    	    
        // Split incoming messages: vertex value:M1, incoming fork:M2, fork request:M3, fakeID:M4
    	splitIncomingMessages(msgIterator);
		//Log.info("SEREF msgListMainMap size is:\t"+msgListMainMap.size());
    	
    	
    	if(getSuperstep()==1){  // initialize all edges list at superstep 1
    		incomingEdgeIndexList.remove(getVertexId());
        	initAllEdgeIndexList();       
        	//Log.info("SEREF allEdgeIndexList size is:\t"+allEdgeIndexList.size());
        	
	    	if(VERSION_OF_JOB.contains("Giraphx")){
				isInternal = checkIsInternal(allEdgeIndexList);
				if(isInternal){
	    			getContext().getCounter( "Giraph Stats", "Internal vertex count").increment(1);
				}
			}
			
	    	int random = (int) (Math.random()*NUM_VERTICES);
	    	fakeId = (long) (random * Math.pow(10, length) + getVertexId().get());
	    	sendMsgToAllVersioned(new Text("M4:"+fakeId));
        	//Log.info("SEREF sent the fake ID "+fakeId);

        	Text msg=new Text("M1:"+getVertexId()+":"+getVertexValue());
        	sendMsgToAllVersioned(msg);
        	    	
        	maxAggreg.aggregate(new DoubleWritable(allEdgeIndexList.size()));
        	return;
        }
        
        if(getSuperstep()==2){
        	int maxDegree = (int) GiraphxWorkerContext.maxDegree;
        	if(!isInternal){
	        	isNeighborsLocal(isLocalMap,allEdgeIndexList);
	    	}
        	else{
    			getContext().getCounter("Giraph Stats",
                    "Local edge count").increment(allEdgeIndexList.size());
			}
        	initAvailableColors(maxDegree); 
        	initForks(my_id);
        	fakeIdList=null;
        }

    	//Log.info("SEREF nonlocalNeighborCount size is:\t"+nonlocalNeighborCount);
    	//Log.info("SEREF myForkMap size is:\t"+myForkMap.size());
    	
    	boolean hasAllForks=false;
    	boolean hasAllCleanOrNoRequest=false;
    	if(myForkMap.size()==nonlocalNeighborCount && !isInternal){
    		hasAllForks=true;
    		hasAllCleanOrNoRequest=detectHasAllCleanOrNoRequest();
    	}
        //Log.info("SEREF hasAllForks and isInternal values Message is:\t"+hasAllForks+"\t"+isInternal);
    	
    	boolean isUpdated=false;    
    	if(isInternal){  // only this block is used for a local vertex
    		isUpdated=operateVersioned();
    	}
    	else if(hasAllForks && hasAllCleanOrNoRequest){
    		isUpdated=operateVersioned();    		
    		for(long key : myForkMap.keySet()){
    			myForkMap.put(key, "DIRTY");
    		}
    		if(getSuperstep()==2){ //In ss=2 there is no incoming request, thus send forks immediately
    			Text m2msg = new Text("M2:"+my_id);
    			for (Long key : myForkMap.keySet()) {
    				sendMsg(new LongWritable(key), m2msg);
					//Log.info("SEREF sent message "+m2msg+" to "+key);
    			}
    			myForkMap.clear();
    		}
    	}

    	if(isUpdated){
        	String msg = "M1:"+getVertexId()+":"+getVertexValue().toString();
        	//Log.info("SEREF sent message "+msg);
        	sendMsgToAllVersioned(new Text(msg));
    	}
    	    
    	if(!isInternal){
        	handleForkRequestMessages(my_id);
    	}
    	
    	hasAllForks=false;
    	if(myForkMap.size()==nonlocalNeighborCount && !isInternal){
    		hasAllForks=true;
    	}
    	if(!isInternal && !hasAllForks && needsOperation){
        	requestMissingForks(my_id);
    	}
    	
    	available=null;
    	voteToHalt();
        
    	//haltIfAllNodesPassive(hasOperatedNew, isUpdated);
    	
        return;
    }
	
	public boolean operate_dGiraph_max(Map<Long, String> msgListMainMap, boolean isPull) {
    	Log.info("SEREF Vertex "+getVertexId()+" operates at superstep "+getSuperstep());
    	long mymax = Long.parseLong(getVertexValue().toString());
    	long oldmymax = Long.parseLong(getVertexValue().toString());
		boolean isUpdated=false;

		if(isPull){
			readFromAllNeighbors(vertexValues,allEdgeIndexList);
			iterateVertexValues(mymax, false);
			//vertexValues.clear();
		}	
		iterateMessages(mymax, false);	
    	setVertexValue(new Text(mymax+""));
    	
    	if(mymax!=oldmymax){
			isUpdated=true;
			Log.info("SEREF Vertex "+getVertexId()+" has updated to value\t"+mymax);
		}
    	
        return isUpdated;
    }    
 
    public boolean operate_dGiraph_coloring(Map<Long, String> msgListMainMap, boolean isPull) {
    	available = new boolean[colorsLength];
    	Arrays.fill(available, true);
    	
		int oldVal=Integer.parseInt(getVertexValue().toString());
		boolean isUpdated=false;
    	
		iterateMessages(-1, true);
    	if(isPull){
	    	readFromAllNeighbors(vertexValues, allEdgeIndexList);
    	}
    	iterateVertexValues(-1, true);
    				
			
    	int newc=-1;
    	for (int i=0; i<available.length; i++){
    		if(available[i]==true){
    			newc=i;
    			break;
    		}
    	}
		if(newc!=oldVal){
			isUpdated=true;
			if(getSuperstep()>500)
				Log.info("SEREF has updated\t"+getVertexId()+"\tOld-new color:"+getVertexValue()+"-"+newc);
		}
		
        setVertexValue(new Text(newc+""));
        needsOperation = false;
        voteToHalt();
        
        return isUpdated;
    }      
    
    public boolean operate_tGiraph_coloring(Map<Long, String> msgListMainMap) {
		return operate_dGiraph_coloring(msgListMainMap,true);
    }          
    
 	private void iterateMessages(long mymax, boolean isColoring) {
 		if(msgListMainMap.isEmpty()){
 			return;
 		}
 		for (String msgStr : msgListMainMap.values()) {
     		String tmp[]=msgStr.split(":");
     		long id=Long.parseLong(tmp[1]);
     		int value = Integer.parseInt(tmp[2]);
     		//Log.info("SEREF Read the M1 message:\t" + id + ":" + value);
     		if(!isColoring){
     			mymax = Math.max(mymax, value); 
     		}
     		else{
         		vertexValues.put(new LongWritable(id), new Text(tmp[2]));
     		}    		
     	}
     	msgListMainMap.clear();		
 	}
 	
 	private void iterateVertexValues(long mymax, boolean isColoring) {
 		Iterator<Entry<LongWritable, Text>> it = vertexValues.entrySet().iterator();
     	while(it.hasNext()){
     		Entry<LongWritable, Text> e = it.next();
     		long id = e.getKey().get();
     		int value = Integer.parseInt(e.getValue().toString());
     		//Log.info("SEREF Read the local value:\t" + id + ":" + value);
     		if(!isColoring){
     			mymax = Math.max(mymax, value); 
     		}
     		else{
     			if(value<colorsLength){
 	    			available[value]=false;
 	    		} 
     		}
     	}		
 	}
    
	private void initForks(long my_id) {
		if(!isInternal){ // Initially get the fork if you have lower ID
    		for( Long neighborFakeId : fakeIdList){
    			long neighbor_id = (long) (neighborFakeId % Math.pow(10, length));
    			if(VERSION_OF_JOB.equals("dGiraph_coloring")){
    				nonlocalNeighborCount++;
    				if(fakeId<neighborFakeId){
    					myForkMap.put(neighbor_id, "DIRTY");
    				}
    			}
    			else if(!isLocalMap.get(new LongWritable(neighbor_id))){
					nonlocalNeighborCount++;
    				if(fakeId<neighborFakeId){
    					myForkMap.put(neighbor_id, "DIRTY");
    				}
				}
    		}
    	}		
	}

	private boolean detectHasAllCleanOrNoRequest() {
    	for( LongWritable idlong : allEdgeIndexList){
    		long neighbor_id = idlong.get();
    		if( !isLocalMap.get(idlong) || VERSION_OF_JOB.equals("dGiraph_coloring") ){
    			if(inReqList.contains(neighbor_id) && myForkMap.get(neighbor_id).equals("DIRTY")){
    				return false;
    			}
    		}
    	}
    	return true;
	}

	private void requestMissingForks(long my_id) {
		Text m3_msg = new Text("M3:"+my_id);
		for( LongWritable idlong : allEdgeIndexList){
			long neighbor_id = idlong.get();
    		if( !isLocalMap.get(idlong) || VERSION_OF_JOB.equals("dGiraph_coloring") ){
				if(!myForkMap.containsKey(neighbor_id) && !outReqMap.containsKey(idlong)){
					sendMsg(idlong, m3_msg);
					outReqMap.put(idlong, true);
					//Log.info("SEREF sent fork request "+m3_msg+" to "+neighbor_id);
				}
    		}
		}		
	}


	private void handleForkRequestMessages(long my_id) {
		Text m2_msg = new Text("M2:"+my_id);
    	Iterator<Long> it = inReqList.iterator();
    	while(it.hasNext()){
    		long reqID = it.next();
    		if(myForkMap.get(reqID)==null){
    			it.remove();
    		}
    		else if(myForkMap.get(reqID).equals("DIRTY")){
    			sendMsg(new LongWritable(reqID), m2_msg);
    			//Log.info("SEREF sent fork "+m2_msg+" to vertex "+reqID);
    			myForkMap.remove(reqID);
    			it.remove();
    		}
    		else if(myForkMap.get(reqID).equals("CLEAN")){}
    	}
	}

	private void splitIncomingMessages(Iterator<Text> msgIterator) {
		while (msgIterator.hasNext()) {
    		String tmp=msgIterator.next().toString();
    		//Log.info("SEREF Incoming message is:\t"+tmp);
    		
    		if(tmp.split(":")[0].equals("M1")){
    			needsOperation = true;
    			long neighbor_id = Long.parseLong(tmp.split(":")[1]);
    			
    			if(getSuperstep()==1){
    				incomingEdgeIndexList.add(new LongWritable(neighbor_id));			
    			}
    			else{
    				msgListMainMap.put(neighbor_id, tmp);
    			}
    		}
    		else if(tmp.split(":")[0].equals("M2") && !isInternal){
    			String idStr = tmp.split(":")[1];
    			String[] idList = idStr.split("_");
    			for(String id : idList){				
    				outReqMap.remove(new LongWritable(Long.parseLong(id)));
    				myForkMap.put(Long.parseLong(id),"CLEAN");
    			}
    		}
    		else if(tmp.split(":")[0].equals("M3")  && !isInternal){
    			String idStr = tmp.split(":")[1];
    			String[] idList = idStr.split("_");
    			for(String id : idList){
    				inReqList.add(Long.parseLong(id));
    			}
    		}
    		else if(tmp.split(":")[0].equals("M4")  && !isInternal){
    			String idStr = tmp.split(":")[1];
    			String[] idList = idStr.split("_");
    			for(String id : idList){
    				fakeIdList.add(Long.parseLong(id));
    			}
    		}
    		else{
    			Log.info("SEREF Unexpected message type");
    		}
    	}		
	}

	private void initAllEdgeIndexList() {
		allEdgeIndexList.addAll(incomingEdgeIndexList);
    	allEdgeIndexList.addAll(destEdgeIndexList);
    	HashSet<LongWritable> hs = new HashSet<LongWritable>();
    	hs.addAll(allEdgeIndexList);
    	
    	allEdgeIndexList.clear();
    	allEdgeIndexList.addAll(hs);	
    	incomingEdgeIndexList=null;
	}
    
	private boolean operateVersioned() {
		boolean isUpdated=false;
		if(VERSION_OF_JOB.equals("orig_max")){
			isUpdated=operate_dGiraph_max(msgListMainMap,false);
		}
		else if(VERSION_OF_JOB.equals("dGiraph_max")){
			isUpdated=operate_dGiraph_max(msgListMainMap,true);
		}
		else if(VERSION_OF_JOB.equals("dGiraph_coloring")){
			isUpdated=operate_dGiraph_coloring(msgListMainMap,false);
		}
		else if(VERSION_OF_JOB.equals("dGiraphx_coloring")){
			isUpdated=operate_dGiraph_coloring(msgListMainMap,true);
		}		
		return isUpdated;
	}

	private void sendMsgToAllVersioned(Text msg) {
		if(VERSION_OF_JOB.equals("dGiraph_coloring")){
    		sendMsgToAllEdges(msg, allEdgeIndexList);
		}
		else if(VERSION_OF_JOB.equals("dGiraphx_coloring")){
    		sendMsgToDistantEdges(msg, allEdgeIndexList);
		}
	}

	private void initAvailableColors(int maxDegree) {
    	colorsLength = 2*maxDegree+5;
    	available = new boolean[colorsLength];
    	Arrays.fill(available, true);		
	}
    
    
    
    
    
    

    
    
    public static class GiraphxWorkerContext extends
    WorkerContext {

		public static long finalSum;
		public static double finalMax;
		
		@Override
		public void preApplication() 
		throws InstantiationException, IllegalAccessException {
			//Log.info("SEREF aggregator registered");
			registerAggregator("sum", LongSumAggregator.class);
			registerAggregator("prsum", LongSumAggregator.class);
			
			registerAggregator("operatedSum", LongSumAggregator.class);
    		registerAggregator("max", MaxAggregator.class);			
		}
		
		@Override
		public void postApplication() {}
		
		@Override
		public void preSuperstep() {
			
		    LongSumAggregator sumAggreg = 
		    	(LongSumAggregator) getAggregator("sum");
		    LongSumAggregator prsumAggreg = 
			    	(LongSumAggregator) getAggregator("prsum");

		    LongSumAggregator operatedAgg = 
			    	(LongSumAggregator) getAggregator("operatedSum");
		    MaxAggregator maxAggreg = 
		        	(MaxAggregator) getAggregator("max");
		    
		    maxDegree = (int) maxAggreg.getAggregatedValue().get();
		    updatedVertexCount = sumAggreg.getAggregatedValue().get();
		    prDifferenceSum = prsumAggreg.getAggregatedValue().get();
		    operatedVertexCount = operatedAgg.getAggregatedValue().get();
	    
		    useAggregator("sum");
		    useAggregator("prsum");
		    useAggregator("operatedSum");
	        useAggregator("max");

		    sumAggreg.setAggregatedValue(new LongWritable(0L));
		    prsumAggreg.setAggregatedValue(new LongWritable(0L));
		    operatedAgg.setAggregatedValue(new LongWritable(0L));

		}
		
		@Override
		public void postSuperstep() { }
    }



    

    /**
     * VertexInputFormat that supports {@link Giraphx}
     */
    public static class GiraphxInputFormat extends
            TextVertexInputFormat<LongWritable,
                                  Text,
                                  Text,
                                  Text> {
        @Override
        public VertexReader<LongWritable, Text, Text, Text>
                createVertexReader(InputSplit split,
                                   TaskAttemptContext context)
                                   throws IOException {
            return new GiraphxVertexReader(
                textInputFormat.createRecordReader(split, context));
        }
    }

    /**
     * VertexReader that supports {@link Giraphx}.  In this
     * case, the edge values are not used.  The files should be in the
     * following JSON format:
     * JSONArray(<vertex id>, <vertex value>,
     *           JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
     * Here is an example with vertex id 1, vertex value 4.3, and two edges.
     * First edge has a destination vertex 2, edge value 2.1.
     * Second edge has a destination vertex 3, edge value 0.7.
     * [1,4.3,[[2,2.1],[3,0.7]]]
     */
    public static class GiraphxVertexReader extends
            TextVertexReader<LongWritable,
            Text, Text, Text> {

        public GiraphxVertexReader(
                RecordReader<LongWritable, Text> lineRecordReader) {
            super(lineRecordReader);
        }

        @Override
        public BasicVertex<LongWritable, Text, Text,
        Text> getCurrentVertex()
            throws IOException, InterruptedException {
        	
          BasicVertex<LongWritable, Text, Text,
          Text> vertex = BspUtils.<LongWritable, Text, Text,
            		  Text>createVertex(getContext().getConfiguration());

          String partitionerType = getContext().getConfiguration().get(PARTITIONER_TYPE);

            Text line = getRecordReader().getCurrentValue();
            try {
                JSONArray jsonVertex = new JSONArray(line.toString());
                LongWritable vertexId = new LongWritable(jsonVertex.getLong(0));
                LongWritable partitionId = new LongWritable(-1);
                Text vertexValue = new Text(jsonVertex.getString(1));
                Map<LongWritable, Text> edges = Maps.newHashMap();
                JSONArray jsonEdgeArray;
                //Log.info("SEREF in vertexreader: "+partitionerType);
                if(partitionerType.equals("metis")){
                	partitionId = new LongWritable(jsonVertex.getInt(2));
                    jsonEdgeArray = jsonVertex.getJSONArray(3);
                    //vertex.partitionId = partitionId;
                    //BspUtils.addToMetisMap(vertexId, partitionId);
                }else if(partitionerType.equals("nometis")){
                	partitionId = new LongWritable(jsonVertex.getInt(2));
                    jsonEdgeArray = jsonVertex.getJSONArray(3);
                    //vertex.partitionId = partitionId;
                    //BspUtils.addToMetisMap(vertexId, partitionId);
                }else{
                	jsonEdgeArray= jsonVertex.getJSONArray(2);
                }
                
                for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                    JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
                    edges.put(new LongWritable(jsonEdge.getLong(0)),
                            new Text(jsonEdge.getString(1)));
                }
                vertex.initialize(vertexId, vertexValue, edges, null);
            } catch (JSONException e) {
                throw new IllegalArgumentException(
                    "next: Couldn't get vertex from line " + line, e);
            }
            return vertex;
        }

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }
    }

    /**
     * VertexOutputFormat that supports {@link SimpleShortestPathsVertex}
     */
    public static class GiraphxOutputFormat extends
            TextVertexOutputFormat<LongWritable, Text,
            Text> {

        @Override
        public VertexWriter<LongWritable, Text, Text>
                createVertexWriter(TaskAttemptContext context)
                throws IOException, InterruptedException {
            RecordWriter<Text, Text> recordWriter =
                textOutputFormat.getRecordWriter(context);
            return new GiraphxWriter(recordWriter);
        }
    }

    /**
     * VertexWriter that supports {@link SimpleShortestPathsVertex}
     */
    public static class GiraphxWriter extends
            TextVertexWriter<LongWritable, Text, Text> {
        public GiraphxWriter(
                RecordWriter<Text, Text> lineRecordWriter) {
            super(lineRecordWriter);
        }

        @Override
        public void writeVertex(BasicVertex<LongWritable, Text,
        		Text, ?> vertex)
                throws IOException, InterruptedException {
            JSONArray jsonVertex = new JSONArray();
            jsonVertex.put(vertex.getVertexId().get());
			jsonVertex.put(vertex.getVertexValue().toString());
			JSONArray jsonEdgeArray = new JSONArray();
			for (LongWritable targetVertexId : vertex) {
			    JSONArray jsonEdge = new JSONArray();
			    jsonEdge.put(targetVertexId.get());
			    jsonEdge.put(vertex.getEdgeValue(targetVertexId).toString());
			    jsonEdgeArray.put(jsonEdge);
			}
			jsonVertex.put(jsonEdgeArray);
            getRecordWriter().write(new Text(jsonVertex.toString()), null);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int run(String[] argArray) throws Exception {
        Preconditions.checkArgument(argArray.length >= 7,
            "run: Must have 6 arguments <input path> <output path> " +
            "<source vertex id> <# of workers> <# of vertices> <method> <supersteps>");

        GiraphJob job = new GiraphJob(getConf(), getClass().getName());
        job.setVertexClass(getClass());
        job.setWorkerContextClass(
        		GiraphxWorkerContext.class);
        job.setVertexInputFormatClass(
        		GiraphxInputFormat.class);
        job.setVertexOutputFormatClass(
        		GiraphxOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(argArray[0]));
        FileOutputFormat.setOutputPath(job, new Path(argArray[1]));
        job.getConfiguration().setLong(Giraphx.SOURCE_ID,
                                       Long.parseLong(argArray[2]));
        job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
                                   Integer.parseInt(argArray[3]),
                                   100.0f);
        
        int vertexNum = Integer.parseInt(argArray[4]);
        Log.info("vertexNum "+vertexNum);
        job.setVertexNumConfiguration(vertexNum);
        
        job.getConfiguration().set(Giraphx.VERSION_OF_JOB_CONF,
                argArray[5]);
        
        job.getConfiguration().setInt(Giraphx.MAX_SUPERSTEPS_CONF,
                Integer.parseInt(argArray[6]));
        
        String partitioner = "mesh";
        if(argArray.length > 7){
        	partitioner = argArray[7];
        }
        job.setPartitionerTypeConfiguration(partitioner);
        job.getConfiguration().set(Giraphx.PARTITIONER_TYPE,
        		partitioner);
        //job.getConfiguration().setInt("mapreduce.job.counters.limit", 5000);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:54310/user/aeyate/input/web-Google.txt.metised.part.100"), 
        		getConf());
        Log.info("SEREF Configuration is: "+getConf().toString());

        
        return job.run(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Giraphx(), args));
    }
}
