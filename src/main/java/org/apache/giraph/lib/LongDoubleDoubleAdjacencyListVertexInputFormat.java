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
package org.apache.giraph.lib;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * InputFormat for reading graphs stored as (ordered) adjacency lists
 * with the vertex ids longs and the vertex values and edges doubles.
 * For example:
 * 22 0.1 45 0.3 99 0.44
 * to repesent a vertex with id 22, value of 0.1 and edges to nodes 45 and 99,
 * with values of 0.3 and 0.44, respectively.
 */
public class LongDoubleDoubleAdjacencyListVertexInputFormat<M extends Writable> extends
    TextVertexInputFormat<LongWritable, DoubleWritable, DoubleWritable, M>  {

  static class VertexReader<M extends Writable> extends
      AdjacencyListVertexReader<LongWritable, DoubleWritable, DoubleWritable, M> {

    VertexReader(RecordReader<LongWritable, Text> lineRecordReader) {
      super(lineRecordReader);
    }

    VertexReader(RecordReader<LongWritable, Text> lineRecordReader,
                 LineSanitizer sanitizer) {
      super(lineRecordReader, sanitizer);
    }

    @Override
    public void decodeId(String s, LongWritable id) {
      id.set(Long.valueOf(s));
    }

    @Override
    public void decodeValue(String s, DoubleWritable value) {
      value.set(Double.valueOf(s));
    }

    @Override
    public void decodeEdge(String s1, String s2, Edge<LongWritable, DoubleWritable>
        textIntWritableEdge) {
      textIntWritableEdge.setDestVertexId(new LongWritable(Long.valueOf(s1)));
      textIntWritableEdge.setEdgeValue(new DoubleWritable(Double.valueOf(s2)));
    }
  }

  @Override
  public org.apache.giraph.graph.VertexReader<LongWritable,
    DoubleWritable, DoubleWritable, M> createVertexReader(
      InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new VertexReader<M>(textInputFormat.createRecordReader(
      split, context));
  }
}
