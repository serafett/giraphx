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

package org.apache.giraph.graph.partition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.giraph.graph.BspServiceWorker;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.WorkerInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.mortbay.log.Log;

/**
 * Implements hash-based partitioning from the id hash code.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 * @param <M> Message value
 */
@SuppressWarnings("rawtypes")
public class HashWorkerPartitioner<I extends WritableComparable,
        V extends Writable, E extends Writable, M extends Writable>
        implements WorkerGraphPartitioner<I, V, E, M> {
    /** Mapping of the vertex ids to {@link PartitionOwner} */
    protected List<PartitionOwner> partitionOwnerList =
        new ArrayList<PartitionOwner>();

    @Override
    public PartitionOwner createPartitionOwner() {
        return new BasicPartitionOwner();
    }

    //**************************** SEREF-START ********************************//
    @Override
    public PartitionOwner getMeshPartitionOwner(I vertexId, BspServiceWorker<I, V, E, M> serviceWorker) {
    	
    	
    	String partitionerType = serviceWorker.getConfiguration().
    			get(GiraphJob.PARTITIONER_TYPE, "mesh");

    	int wcount = serviceWorker.getConfiguration().getInt(GiraphJob.MAX_WORKERS, -1);
    	int vertex_count = serviceWorker.getConfiguration().getInt(GiraphJob.NUM_VERTICES, -1);
    	int result = (int) (Math.floor( ((LongWritable)vertexId).get()
    			/ (vertex_count / wcount)
    			));


    	
    	if(partitionerType.equals("metis")){
    		int vid = (int) ((LongWritable)(vertexId)).get();
    		int metisPartition;
    		synchronized(BspUtils.metisMap){
    			metisPartition = BspUtils.getFromMetisMap(vid);
    			if(metisPartition==-1){
    				throw new IllegalStateException(
                            "SEREF getMeshPartitionOwner: Vertex " + vid +
                            " has an invalid owner worker!");
    			}
    		}
        	int metisResult = (int) (Math.floor( metisPartition % (wcount)	));
        	if(vid==3)
        	Log.info("ID: "+vid +"\tmetisPartition: " + metisPartition
        			+"\tmetisResult: " + metisResult);
    		return partitionOwnerList.get(metisResult);
    	}
    	return	partitionOwnerList.get(result);
    }
    //**************************** SEREF-END ********************************//

    @Override
    public PartitionOwner getPartitionOwner(I vertexId) {    	
        return partitionOwnerList.get(Math.abs(vertexId.hashCode())
                % partitionOwnerList.size());
    }

    @Override
    public Collection<PartitionStats> finalizePartitionStats(
            Collection<PartitionStats> workerPartitionStats,
            Map<Integer, Partition<I, V, E, M>> partitionMap) {
        // No modification necessary
        return workerPartitionStats;
    }

    @Override
    public PartitionExchange updatePartitionOwners(
            WorkerInfo myWorkerInfo,
            Collection<? extends PartitionOwner> masterSetPartitionOwners,
            Map<Integer, Partition<I, V, E, M>> partitionMap) {
        partitionOwnerList.clear();
        partitionOwnerList.addAll(masterSetPartitionOwners);

        Set<WorkerInfo> dependentWorkerSet = new HashSet<WorkerInfo>();
        Map<WorkerInfo, List<Integer>> workerPartitionOwnerMap =
            new HashMap<WorkerInfo, List<Integer>>();
        for (PartitionOwner partitionOwner : masterSetPartitionOwners) {
            if (partitionOwner.getPreviousWorkerInfo() == null) {
                continue;
            } else if (partitionOwner.getWorkerInfo().equals(
                       myWorkerInfo) &&
                       partitionOwner.getPreviousWorkerInfo().equals(
                       myWorkerInfo)) {
                throw new IllegalStateException(
                    "updatePartitionOwners: Impossible to have the same " +
                    "previous and current worker info " + partitionOwner +
                    " as me " + myWorkerInfo);
            } else if (partitionOwner.getWorkerInfo().equals(myWorkerInfo)) {
                dependentWorkerSet.add(partitionOwner.getPreviousWorkerInfo());
            } else if (partitionOwner.getPreviousWorkerInfo().equals(
                    myWorkerInfo)) {
                if (workerPartitionOwnerMap.containsKey(
                        partitionOwner.getWorkerInfo())) {
                    workerPartitionOwnerMap.get(
                        partitionOwner.getWorkerInfo()).add(
                            partitionOwner.getPartitionId());
                } else {
                    List<Integer> partitionOwnerList = new ArrayList<Integer>();
                    partitionOwnerList.add(partitionOwner.getPartitionId());
                    workerPartitionOwnerMap.put(partitionOwner.getWorkerInfo(),
                                                partitionOwnerList);
                }
            }
        }

        return new PartitionExchange(dependentWorkerSet,
                                     workerPartitionOwnerMap);
    }

    @Override
    public Collection<? extends PartitionOwner> getPartitionOwners() {
        return partitionOwnerList;
    }
}
