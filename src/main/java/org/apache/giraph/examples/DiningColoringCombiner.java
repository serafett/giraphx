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
public class DiningColoringCombiner
        extends VertexCombiner<LongWritable, Text> {

    @Override
    public Iterable<Text> combine(LongWritable target,
    		Iterable<Text> messages) throws IOException {
    	
    	List<Text> value = new ArrayList<Text>();
    	String msgType;
    	String m2="M2:"; boolean isM2 = false;
    	String m3="M3:"; boolean isM3 = false;
    	String m4="M4:"; boolean isM4 = false;
    	
    	for (Text message : messages) {
    		msgType = message.toString().split(":")[0];
    		if(msgType.equals("M1")){
    			value.add(message);
    		}
    		else if(msgType.equals("M2")){
    			isM2 = true;
    			m2=m2+message.toString().split(":")[1]+"_";
    		}
    		else if(msgType.equals("M3")){
    			isM3 = true;
    			m3=m3+message.toString().split(":")[1]+"_";
    		}
    		else if(msgType.equals("M4")){
    			isM4 = true;
    			m4=m4+message.toString().split(":")[1]+"_";
    		}
    		else{
    			value.add(message);
    		}
        }
    	m2 = m2.substring(0, m2.length() - 1);
    	if(isM2){ value.add(new Text(m2)); }
    	
    	m3 = m3.substring(0, m3.length() - 1);
    	if(isM3){ value.add(new Text(m3)); }
    	
    	m4 = m4.substring(0, m4.length() - 1);
    	if(isM4){ value.add(new Text(m4)); }
    			
        return value;
    }
}
