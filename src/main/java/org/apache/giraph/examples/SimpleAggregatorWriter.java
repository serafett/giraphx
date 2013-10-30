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

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.graph.Aggregator;
import org.apache.giraph.graph.AggregatorWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * This is a simple example for an aggregator writer. After each superstep
 * the writer will persist the aggregator values to disk, by use of the
 * Writable interface. The file will be created on the current working
 * directory.
 */
public class SimpleAggregatorWriter implements AggregatorWriter {
    /** the name of the file we wrote to */
    public static String filename;
    private FSDataOutputStream output;
    
    @SuppressWarnings("rawtypes")
    @Override
    public void initialize(Context context, long applicationAttempt)
            throws IOException {
        filename = "aggregatedValues_"+applicationAttempt;
        Path p = new Path(filename);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        output = fs.create(p, true);
    }

    @Override
    public void writeAggregator(Map<String, Aggregator<Writable>> map,
            long superstep) throws IOException {

        for (Entry<String, Aggregator<Writable>> aggregator: map.entrySet()) {
            aggregator.getValue().getAggregatedValue().write(output);
        }
        output.flush();
    }

    @Override
    public void close() throws IOException {
        output.close();
    }
}
