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

import org.apache.giraph.bsp.BspInputSplit;
import org.apache.giraph.graph.VertexReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Used by GeneratedVertexInputFormat to read some generated data
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public abstract class GeneratedVertexReader<
        I extends WritableComparable, V extends Writable, E extends Writable,
        M extends Writable>
        implements VertexReader<I, V, E, M> {
    /** Records read so far */
    protected long recordsRead = 0;
    /** Total records to read (on this split alone) */
    protected long totalRecords = 0;
    /** The input split from initialize(). */
    protected BspInputSplit inputSplit = null;
    /** Reverse the id order? */
    protected boolean reverseIdOrder;

    protected Configuration configuration = null;

    public static final String READER_VERTICES =
        "GeneratedVertexReader.reader_vertices";
    public static final long DEFAULT_READER_VERTICES = 10;
    public static final String REVERSE_ID_ORDER =
        "GeneratedVertexReader.reverseIdOrder";
    public static final boolean DEAFULT_REVERSE_ID_ORDER = false;

    public GeneratedVertexReader() {
    }

    @Override
    final public void initialize(InputSplit inputSplit,
                                 TaskAttemptContext context)
            throws IOException {
        configuration = context.getConfiguration();
        totalRecords = configuration.getLong(
            GeneratedVertexReader.READER_VERTICES,
            GeneratedVertexReader.DEFAULT_READER_VERTICES);
        reverseIdOrder = configuration.getBoolean(
            GeneratedVertexReader.REVERSE_ID_ORDER,
            GeneratedVertexReader.DEAFULT_REVERSE_ID_ORDER);
        this.inputSplit = (BspInputSplit) inputSplit;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    final public float getProgress() throws IOException {
        return recordsRead * 100.0f / totalRecords;
    }
}
