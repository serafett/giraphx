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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.giraph.graph.WorkerInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Helper class for balancing partitions across a set of workers.
 */
public class PartitionBalancer {
    /** Partition balancing algorithm */
    public static final String PARTITION_BALANCE_ALGORITHM =
        "hash.partitionBalanceAlgorithm";
    public static final String STATIC_BALANCE_ALGORITHM =
        "static";
    public static final String EGDE_BALANCE_ALGORITHM =
        "edges";
    public static final String VERTICES_BALANCE_ALGORITHM =
        "vertices";
    /** Class logger */
    private static Logger LOG = Logger.getLogger(PartitionBalancer.class);

    /**
     * What value to balance partitions with?  Edges, vertices?
     */
    private enum BalanceValue {
        UNSET,
        EDGES,
        VERTICES
    }

    /**
     * Get the value used to balance.
     *
     * @param partitionStat
     * @param balanceValue
     * @return
     */
    private static long getBalanceValue(PartitionStats partitionStat,
                                        BalanceValue balanceValue) {
        switch (balanceValue) {
            case EDGES:
                return partitionStat.getEdgeCount();
            case VERTICES:
                return partitionStat.getVertexCount();
            default:
                throw new IllegalArgumentException(
                    "getBalanceValue: Illegal balance value " + balanceValue);
        }
    }

    /**
     * Used to sort the partition owners from lowest value to highest value
     */
    private static class PartitionOwnerComparator implements
            Comparator<PartitionOwner> {
        /** Map of owner to stats */
        private final Map<PartitionOwner, PartitionStats> ownerStatMap;
        /** Value type to compare on */
        private final BalanceValue balanceValue;


        /**
         * Only constructor.
         *
         * @param comparatorValue What to compare with?
         */
        public PartitionOwnerComparator(
                Map<PartitionOwner, PartitionStats> ownerStatMap,
                BalanceValue balanceValue) {
            this.ownerStatMap = ownerStatMap;
            this.balanceValue = balanceValue;
        }

        @Override
        public int compare(PartitionOwner owner1, PartitionOwner owner2) {
            return (int)
                (getBalanceValue(ownerStatMap.get(owner1), balanceValue) -
                 getBalanceValue(ownerStatMap.get(owner2), balanceValue));
        }
    }

    /**
     * Structure to keep track of how much value a {@link WorkerInfo} has
     * been assigned.
     */
    private static class WorkerInfoAssignments implements
            Comparable<WorkerInfoAssignments> {
        /** Worker info associated */
        private final WorkerInfo workerInfo;
        /** Balance value */
        private final BalanceValue balanceValue;
        /** Map of owner to stats */
        private final Map<PartitionOwner, PartitionStats> ownerStatsMap;
        /** Current value of this object */
        private long value = 0;

        public WorkerInfoAssignments(
                WorkerInfo workerInfo,
                BalanceValue balanceValue,
                Map<PartitionOwner, PartitionStats> ownerStatsMap) {
            this.workerInfo = workerInfo;
            this.balanceValue = balanceValue;
            this.ownerStatsMap = ownerStatsMap;
        }

        /**
         * Get the total value of all partitions assigned to this worker.
         *
         * @return Total value of all partition assignments.
         */
        public long getValue() {
            return value;
        }

        /**
         * Assign a {@link PartitionOwner} to this {@link WorkerInfo}.
         *
         * @param partitionOwner PartitionOwner to assign.
         */
        public void assignPartitionOwner(
                PartitionOwner partitionOwner) {
            value += getBalanceValue(ownerStatsMap.get(partitionOwner),
                                     balanceValue);
            if (!partitionOwner.getWorkerInfo().equals(workerInfo)) {
                partitionOwner.setPreviousWorkerInfo(
                    partitionOwner.getWorkerInfo());
                partitionOwner.setWorkerInfo(workerInfo);
            } else {
                partitionOwner.setPreviousWorkerInfo(null);
            }
        }

        @Override
        public int compareTo(WorkerInfoAssignments other) {
            return (int)
                (getValue() - ((WorkerInfoAssignments) other).getValue());
        }
    }

    /**
     * Balance the partitions with an algorithm based on a value.
     *
     * @param conf Configuration to find the algorithm
     * @param allPartitionStatsList All the partition stats
     * @param availableWorkerInfos All the available workers
     * @return Balanced partition owners
     */
    public static Collection<PartitionOwner> balancePartitionsAcrossWorkers(
        Configuration conf,
        Collection<PartitionOwner> partitionOwners,
        Collection<PartitionStats> allPartitionStats,
        Collection<WorkerInfo> availableWorkerInfos) {

        String balanceAlgorithm =
            conf.get(PARTITION_BALANCE_ALGORITHM, STATIC_BALANCE_ALGORITHM);
        if (LOG.isInfoEnabled()) {
            LOG.info("balancePartitionsAcrossWorkers: Using algorithm " +
                     balanceAlgorithm);
        }
        BalanceValue balanceValue = BalanceValue.UNSET;
        if (balanceAlgorithm.equals(STATIC_BALANCE_ALGORITHM)) {
            return partitionOwners;
        } else if (balanceAlgorithm.equals(EGDE_BALANCE_ALGORITHM)) {
            balanceValue = BalanceValue.EDGES;
        } else if (balanceAlgorithm.equals(VERTICES_BALANCE_ALGORITHM)) {
            balanceValue = BalanceValue.VERTICES;
        } else {
            throw new IllegalArgumentException(
                "balancePartitionsAcrossWorkers: Illegal balance " +
                "algorithm - " + balanceAlgorithm);
        }

        // Join the partition stats and partition owners by partition id
        Map<Integer, PartitionStats> idStatMap =
            new HashMap<Integer, PartitionStats>();
        for (PartitionStats partitionStats : allPartitionStats) {
            if (idStatMap.put(partitionStats.getPartitionId(), partitionStats)
                    != null) {
                throw new IllegalStateException(
                    "balancePartitionsAcrossWorkers: Duplicate partition id " +
                    "for " + partitionStats);
            }
        }
        Map<PartitionOwner, PartitionStats> ownerStatsMap =
            new HashMap<PartitionOwner, PartitionStats>();
        for (PartitionOwner partitionOwner : partitionOwners) {
            PartitionStats partitionStats =
                idStatMap.get(partitionOwner.getPartitionId());
            if (partitionStats == null) {
                throw new IllegalStateException(
                    "balancePartitionsAcrossWorkers: Missing partition " +
                    "stats for " + partitionOwner);
            }
            if (ownerStatsMap.put(partitionOwner, partitionStats) != null) {
                throw new IllegalStateException(
                    "balancePartitionsAcrossWorkers: Duplicate partition " +
                    "owner " + partitionOwner);
            }
        }
        if (ownerStatsMap.size() != partitionOwners.size()) {
            throw new IllegalStateException(
                "balancePartitionsAcrossWorkers: ownerStats count = " +
                ownerStatsMap.size() + ", partitionOwners count = " +
                partitionOwners.size() + " and should match.");
        }

        List<WorkerInfoAssignments> workerInfoAssignmentsList =
            new ArrayList<WorkerInfoAssignments>(availableWorkerInfos.size());
        for (WorkerInfo workerInfo : availableWorkerInfos) {
            workerInfoAssignmentsList.add(
                new WorkerInfoAssignments(
                    workerInfo, balanceValue, ownerStatsMap));
        }

        // A simple heuristic for balancing the partitions across the workers
        // using a value (edges, vertices).  An improvement would be to
        // take into account the already existing partition worker assignments.
        // 1.  Sort the partitions by size
        // 2.  Place the workers in a min heap sorted by their total balance
        //     value.
        // 3.  From largest partition to the smallest, take the partition
        //     worker at the top of the heap, add the partition to it, and
        //     then put it back in the heap
        List<PartitionOwner> partitionOwnerList =
            new ArrayList<PartitionOwner>(partitionOwners);
        Collections.sort(partitionOwnerList,
            Collections.reverseOrder(
                new PartitionOwnerComparator(ownerStatsMap, balanceValue)));
        PriorityQueue<WorkerInfoAssignments> minQueue =
            new PriorityQueue<WorkerInfoAssignments>(workerInfoAssignmentsList);
        for (PartitionOwner partitionOwner : partitionOwnerList) {
            WorkerInfoAssignments chosenWorker = minQueue.remove();
            chosenWorker.assignPartitionOwner(partitionOwner);
            minQueue.add(chosenWorker);
        }

        return partitionOwnerList;
    }
}

