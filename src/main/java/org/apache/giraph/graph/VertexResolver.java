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

import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Default implementation of how to resolve vertex creation/removal, messages
 * to nonexistent vertices, etc.
 *
 * @param <I>
 * @param <V>
 * @param <E>
 * @param <M>
 */
@SuppressWarnings("rawtypes")
public class VertexResolver<I extends WritableComparable, V extends Writable,
        E extends Writable, M extends Writable>
        implements BasicVertexResolver<I, V, E, M>, Configurable {
    /** Configuration */
    private Configuration conf = null;

    private GraphState<I,V,E,M> graphState;

    /** Class logger */
    private static final Logger LOG = Logger.getLogger(VertexResolver.class);

    @Override
    public BasicVertex<I, V, E, M> resolve(
            I vertexId,
            BasicVertex<I, V, E, M> vertex,
            VertexChanges<I, V, E, M> vertexChanges,
            Iterable<M> messages) {
        // Default algorithm:
        // 1. If the vertex exists, first prune the edges
        // 2. If vertex removal desired, remove the vertex.
        // 3. If creation of vertex desired, pick first vertex
        // 4. If vertex doesn't exist, but got messages, create
        // 5. If edge addition, add the edges
        if (vertex != null) {
            if (vertexChanges != null) {
                List<I> removedEdgeList = vertexChanges.getRemovedEdgeList();
                for (I removedDestVertex : removedEdgeList) {
                    E removeEdge =
                        ((MutableVertex<I, V, E, M>) vertex).removeEdge(
                            removedDestVertex);
                    if (removeEdge == null) {
                        LOG.warn("resolve: Failed to remove edge with " +
                                 "destination " + removedDestVertex + "on " +
                                 vertex + " since it doesn't exist.");
                    }
                }
                if (vertexChanges.getRemovedVertexCount() > 0) {
                    vertex = null;
                }
            }
        }

        if (vertex == null) {
            if (vertexChanges != null) {
                if (!vertexChanges.getAddedVertexList().isEmpty()) {
                    vertex = vertexChanges.getAddedVertexList().get(0);
                }
            }
            if (vertex == null && messages != null
                    && !Iterables.isEmpty(messages)) {
                vertex = instantiateVertex();
                vertex.initialize(vertexId,
                                  BspUtils.<V>createVertexValue(getConf()),
                                  null,
                                  messages);
            }
        } else {
            if ((vertexChanges != null) &&
                    (!vertexChanges.getAddedVertexList().isEmpty())) {
                LOG.warn("resolve: Tried to add a vertex with id = " +
                         vertex.getVertexId() + " when one already " +
                        "exists.  Ignoring the add vertex request.");
            }
        }

        if (vertexChanges != null &&
                !vertexChanges.getAddedEdgeList().isEmpty()) {
            MutableVertex<I, V, E, M> mutableVertex =
                (MutableVertex<I, V, E, M>) vertex;
            for (Edge<I, E> edge : vertexChanges.getAddedEdgeList()) {
                edge.setConf(getConf());
                mutableVertex.addEdge(edge.getDestVertexId(),
                                      edge.getEdgeValue());
            }
        }

        return vertex;
    }

    @Override
    public BasicVertex<I, V, E, M> instantiateVertex() {
        BasicVertex<I, V, E, M> vertex =
            BspUtils.<I, V, E, M>createVertex(getConf());
        vertex.setGraphState(graphState);
        return vertex;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public void setGraphState(GraphState<I, V, E, M> graphState) {
      this.graphState = graphState;
    }
}
