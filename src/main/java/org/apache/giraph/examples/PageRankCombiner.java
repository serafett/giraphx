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

import org.apache.giraph.graph.VertexCombiner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link VertexCombiner} that finds the sum of messages in {@link Text} format
 */
public class PageRankCombiner
        extends VertexCombiner<LongWritable, Text> {

    @Override
    public Iterable<Text> combine(LongWritable target,
    		Iterable<Text> messages) throws IOException {
    	double sum = 0;	        
    	for (Text message : messages) {
    		sum += Double.parseDouble(message.toString());
        }

        List<Text> value = new ArrayList<Text>();
        value.add(new Text(sum+""));
        
        return value;
    }
}
