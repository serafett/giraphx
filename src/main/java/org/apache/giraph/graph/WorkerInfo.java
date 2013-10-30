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

import org.apache.hadoop.io.Writable;

/**
 * Information about a worker that is sent to the master and other workers.
 */
public class WorkerInfo implements Writable {
	private int workerID;
    /** Worker hostname */
    private String hostname;
    /** Partition id of this worker */
    private int partitionId = -1;
    /** Port that the RPC server is using */
    private int port = -1;
    /** Hostname + "_" + id for easier debugging */
    private String hostnameId;

    /**
     * Constructor for reflection
     */
    public WorkerInfo() {
    }

    public WorkerInfo(String hostname, int partitionId, int port, int workerID) {
        this.hostname = hostname;
        this.partitionId = partitionId;
        this.port = port;
        this.hostnameId = hostname + "_" + partitionId;
        this.workerID = workerID;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public String getHostnameId() {
        return hostnameId;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof WorkerInfo) {
            WorkerInfo workerInfo = (WorkerInfo) other;
            if (hostname.equals(workerInfo.getHostname()) &&
                    (partitionId == workerInfo.getPartitionId()) &&
                    (port == workerInfo.getPort())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 37 * result + port;
        result = 37 * result + hostname.hashCode();
        result = 37 * result + partitionId;
        return result;
    }

    @Override
    public String toString() {
        return "Worker(hostname=" + hostname + ", MRpartition=" +
            partitionId + ", port=" + port + ")";
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        hostname = input.readUTF();
        partitionId = input.readInt();
        port = input.readInt();
        hostnameId = hostname + "_" + partitionId;
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(hostname);
        output.writeInt(partitionId);
        output.writeInt(port);
    }

	/**
	 * @return the workerID
	 */
	public int getWorkerID() {
		return workerID;
	}

	/**
	 * @param workerID the workerID to set
	 */
	public void setWorkerID(int workerID) {
		this.workerID = workerID;
	}
}
