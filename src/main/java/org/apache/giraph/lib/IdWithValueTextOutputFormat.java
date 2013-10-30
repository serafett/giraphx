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


import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Write out Vertices' IDs and values, but not their edges nor edges' values.
 * This is a useful output format when the final value of the vertex is
 * all that's needed. The boolean configuration parameter reverse.id.and.value
 * allows reversing the output of id and value.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class IdWithValueTextOutputFormat <I extends WritableComparable,
    V extends Writable, E extends Writable> extends TextVertexOutputFormat<I, V, E>{

  static class IdWithValueVertexWriter<I extends WritableComparable, V extends
      Writable, E extends Writable> extends TextVertexWriter<I, V, E> {

    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    public static final String REVERSE_ID_AND_VALUE = "reverse.id.and.value";
    public static final boolean REVERSE_ID_AND_VALUE_DEFAULT = false;

    private String delimiter;

    public IdWithValueVertexWriter(RecordWriter<Text, Text> recordWriter) {
      super(recordWriter);
    }

    @Override
    public void writeVertex(BasicVertex<I, V, E, ?> vertex) throws IOException,
        InterruptedException {
      if (delimiter == null) {
        delimiter = getContext().getConfiguration()
           .get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
      }

      String first;
      String second;
      boolean reverseOutput = getContext().getConfiguration()
          .getBoolean(REVERSE_ID_AND_VALUE, REVERSE_ID_AND_VALUE_DEFAULT);

      if (reverseOutput) {
        first = vertex.getVertexValue().toString();
        second = vertex.getVertexId().toString();
      } else {
        first = vertex.getVertexId().toString();
        second = vertex.getVertexValue().toString();
      }

      Text line = new Text(first + delimiter + second);

      getRecordWriter().write(line, null);
    }
  }

  @Override
  public VertexWriter<I, V, E> createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new IdWithValueVertexWriter<I, V, E>
        (textOutputFormat.getRecordWriter(context));
  }

}
