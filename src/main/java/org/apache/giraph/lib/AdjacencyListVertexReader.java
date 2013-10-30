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

import com.google.common.collect.Maps;
import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.Edge;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordReader;

import java.io.IOException;
import java.util.Map;

/**
 * VertexReader that readers lines of text with vertices encoded as adjacency
 * lists and converts each token to the correct type.  For example, a graph
 * with vertices as integers and values as doubles could be encoded as:
 *   1 0.1 2 0.2 3 0.3
 * to represent a vertex named 1, with 0.1 as its value and two edges, to
 * vertices 2 and 3, with edge values of 0.2 and 0.3, respectively.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class AdjacencyListVertexReader<I extends WritableComparable,
    V extends Writable, E extends Writable, M extends Writable> extends
    TextVertexInputFormat.TextVertexReader<I, V, E, M> {

  public static final String LINE_TOKENIZE_VALUE = "adj.list.input.delimiter";
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

  private String splitValue = null;

  /**
   * Utility for doing any cleaning of each line before it is tokenized.
   */
  public interface LineSanitizer {
    /**
     * Clean string s before attempting to tokenize it.
     */
    public String sanitize(String s);
  }

  private LineSanitizer sanitizer = null;

  public AdjacencyListVertexReader(RecordReader<LongWritable, Text> lineRecordReader) {
    super(lineRecordReader);
  }

  public AdjacencyListVertexReader(RecordReader<LongWritable, Text> lineRecordReader,
      LineSanitizer sanitizer) {
    super(lineRecordReader);
    this.sanitizer = sanitizer;
  }

  /**
   * Store the Id for this line in an instance of its correct type.
   * @param s Id of vertex from line
   * @param id Instance of Id's type, in which to store its value
   */
  abstract public void decodeId(String s, I id);

  /**
   * Store the value for this line in an instance of its correct type.
   * @param s Value from line
   * @param value Instance of value's type, in which to store its value
   */
  abstract public void decodeValue(String s, V value);

  /**
   * Store an edge from the line into an instance of a correctly typed Edge
   * @param id The edge's id from the line
   * @param value The edge's value from the line
   * @param edge Instance of edge in which to store the id and value
   */
  abstract public void decodeEdge(String id, String value, Edge<I, E> edge);


  @Override
  public boolean nextVertex() throws IOException, InterruptedException {
    return getRecordReader().nextKeyValue();
  }

  @Override
  public BasicVertex<I, V, E, M> getCurrentVertex() throws IOException, InterruptedException {
    Configuration conf = getContext().getConfiguration();
    String line = getRecordReader().getCurrentValue().toString();
    BasicVertex<I, V, E, M> vertex = BspUtils.createVertex(conf);

    if (sanitizer != null) {
      line = sanitizer.sanitize(line);
    }

    if (splitValue == null) {
      splitValue = conf.get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    String [] values = line.split(splitValue);

    if ((values.length < 2) || (values.length % 2 != 0)) {
      throw new IllegalArgumentException("Line did not split correctly: " + line);
    }

    I vertexId = BspUtils.<I>createVertexIndex(conf);
    decodeId(values[0], vertexId);

    V value = BspUtils.<V>createVertexValue(conf);
    decodeValue(values[1], value);

    int i = 2;
    Map<I, E> edges = Maps.newHashMap();
    Edge<I, E> edge = new Edge<I, E>();
    while(i < values.length) {
      decodeEdge(values[i], values[i + 1], edge);
      edges.put(edge.getDestVertexId(), edge.getEdgeValue());
      i += 2;
    }
    vertex.initialize(vertexId, value, edges, null);
    return vertex;
  }
}
