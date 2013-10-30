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

package org.apache.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.partition.PartitionStats;
import org.apache.hadoop.io.Writable;

/**
 * Aggregated stats by the master.
 */
public class GlobalStats implements Writable {
    private long vertexCount = 0;
    private long finishedVertexCount = 0;
    private long edgeCount = 0;
    private long messageCount = 0;

    public void addPartitionStats(PartitionStats partitionStats) {
        this.vertexCount += partitionStats.getVertexCount();
        this.finishedVertexCount += partitionStats.getFinishedVertexCount();
        this.edgeCount += partitionStats.getEdgeCount();
    }

    public long getVertexCount() {
        return vertexCount;
    }

    public long getFinishedVertexCount() {
        return finishedVertexCount;
    }

    public long getEdgeCount() {
        return edgeCount;
    }

    public long getMessageCount() {
        return messageCount;
    }

    public void addMessageCount(long messageCount) {
        this.messageCount += messageCount;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        vertexCount = input.readLong();
        finishedVertexCount = input.readLong();
        edgeCount = input.readLong();
        messageCount = input.readLong();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(vertexCount);
        output.writeLong(finishedVertexCount);
        output.writeLong(edgeCount);
        output.writeLong(messageCount);
    }

    @Override
    public String toString() {
        return "(vtx=" + vertexCount + ",finVtx=" +
               finishedVertexCount + ",edges=" + edgeCount + ",msgCount=" +
               messageCount + ")";
    }
}
