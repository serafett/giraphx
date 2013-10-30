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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.graph.WorkerInfo;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * Basic partition owner, can be subclassed for more complicated partition
 * owner implementations.
 */
public class BasicPartitionOwner implements PartitionOwner, Configurable {
    /** Configuration */
    private Configuration conf;
    /** Partition id */
    private int partitionId = -1;
    /** Owning worker information */
    private WorkerInfo workerInfo;
    /** Previous (if any) worker info */
    private WorkerInfo previousWorkerInfo;
    /** Checkpoint files prefix for this partition */
    private String checkpointFilesPrefix;

    public BasicPartitionOwner() {
    }

    public BasicPartitionOwner(int partitionId, WorkerInfo workerInfo) {
        this(partitionId, workerInfo, null, null);
    }

    public BasicPartitionOwner(int partitionId,
                               WorkerInfo workerInfo,
                               WorkerInfo previousWorkerInfo,
                               String checkpointFilesPrefix) {
        this.partitionId = partitionId;
        this.workerInfo = workerInfo;
        this.previousWorkerInfo = previousWorkerInfo;
        this.checkpointFilesPrefix = checkpointFilesPrefix;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public WorkerInfo getWorkerInfo() {
        return workerInfo;
    }

    @Override
    public void setWorkerInfo(WorkerInfo workerInfo) {
        this.workerInfo = workerInfo;
    }

    @Override
    public WorkerInfo getPreviousWorkerInfo() {
        return previousWorkerInfo;
    }

    @Override
    public void setPreviousWorkerInfo(WorkerInfo workerInfo) {
        this.previousWorkerInfo = workerInfo;
    }

    @Override
    public String getCheckpointFilesPrefix() {
        return checkpointFilesPrefix;
    }

    @Override
    public void setCheckpointFilesPrefix(String checkpointFilesPrefix) {
        this.checkpointFilesPrefix = checkpointFilesPrefix;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        partitionId = input.readInt();
        workerInfo = new WorkerInfo();
        workerInfo.readFields(input);
        boolean hasPreviousWorkerInfo = input.readBoolean();
        if (hasPreviousWorkerInfo) {
            previousWorkerInfo = new WorkerInfo();
            previousWorkerInfo.readFields(input);
        }
        boolean hasCheckpointFilePrefix = input.readBoolean();
        if (hasCheckpointFilePrefix) {
            checkpointFilesPrefix = input.readUTF();
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(partitionId);
        workerInfo.write(output);
        if (previousWorkerInfo != null) {
            output.writeBoolean(true);
            previousWorkerInfo.write(output);
        } else {
            output.writeBoolean(false);
        }
        if (checkpointFilesPrefix != null) {
            output.writeBoolean(true);
            output.writeUTF(checkpointFilesPrefix);
        } else {
            output.writeBoolean(false);
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
    public String toString() {
        return "(id=" + partitionId + ",cur=" + workerInfo + ",prev=" +
               previousWorkerInfo + ",ckpt_file=" + checkpointFilesPrefix + ")";
    }
}
