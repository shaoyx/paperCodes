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

package org.apache.giraph.edge;

import com.google.common.collect.UnmodifiableIterator;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * {@link VertexEdges} implementation with long ids and double edge values,
 * backed by a {@link Long2DoubleOpenHashMap}.
 * Parallel edges are not allowed.
 * Note: this implementation is optimized for fast random access and mutations,
 * and uses less space than a generic {@link HashMapEdges} (but more than
 * {@link LongDoubleArrayEdges}.
 */
public class LongDoubleHashMapEdges
    extends ConfigurableVertexEdges<LongWritable, DoubleWritable>
    implements StrictRandomAccessVertexEdges<LongWritable, DoubleWritable>,
    ReuseObjectsVertexEdges<LongWritable, DoubleWritable>,
    MutableVertexEdges<LongWritable, DoubleWritable> {
  /** Hash map from target vertex id to edge value. */
  private Long2DoubleOpenHashMap edgeMap;
  /** Representative edge value object, used by getEdgeValue(). */
  private DoubleWritable representativeEdgeValue;

  @Override
  public void initialize(Iterable<Edge<LongWritable, DoubleWritable>> edges) {
    // If the iterable is actually a collection, we can cheaply get the
    // size and initialize the hash-map with the expected capacity.
    if (edges instanceof Collection) {
      initialize(
          ((Collection<Edge<LongWritable, DoubleWritable>>) edges).size());
    } else {
      initialize();
    }
    for (Edge<LongWritable, DoubleWritable> edge : edges) {
      add(edge);
    }
  }

  @Override
  public void initialize(int capacity) {
    edgeMap = new Long2DoubleOpenHashMap(capacity);
  }

  @Override
  public void initialize() {
    edgeMap = new Long2DoubleOpenHashMap();
  }

  @Override
  public void add(Edge<LongWritable, DoubleWritable> edge) {
    edgeMap.put(edge.getTargetVertexId().get(), edge.getValue().get());
  }

  @Override
  public void remove(LongWritable targetVertexId) {
    edgeMap.remove(targetVertexId.get());
  }

  @Override
  public DoubleWritable getEdgeValue(LongWritable targetVertexId) {
    if (!edgeMap.containsKey(targetVertexId.get())) {
      return null;
    }
    if (representativeEdgeValue == null) {
      representativeEdgeValue = getConf().createEdgeValue();
    }
    representativeEdgeValue.set(edgeMap.get(targetVertexId.get()));
    return representativeEdgeValue;
  }

  @Override
  public void setEdgeValue(LongWritable targetVertexId,
                           DoubleWritable edgeValue) {
    if (edgeMap.containsKey(targetVertexId.get())) {
      edgeMap.put(targetVertexId.get(), edgeValue.get());
    }
  }

  @Override
  public int size() {
    return edgeMap.size();
  }

  @Override
  public Iterator<Edge<LongWritable, DoubleWritable>> iterator() {
    // Returns an iterator that reuses objects.
    return new UnmodifiableIterator<Edge<LongWritable, DoubleWritable>>() {
      /** Wrapped map iterator. */
      private ObjectIterator<Long2DoubleMap.Entry> mapIterator =
          edgeMap.long2DoubleEntrySet().fastIterator();
      /** Representative edge object. */
      private ReusableEdge<LongWritable, DoubleWritable> representativeEdge =
          getConf().createReusableEdge();

      @Override
      public boolean hasNext() {
        return mapIterator.hasNext();
      }

      @Override
      public Edge<LongWritable, DoubleWritable> next() {
        Long2DoubleMap.Entry nextEntry = mapIterator.next();
        representativeEdge.getTargetVertexId().set(nextEntry.getLongKey());
        representativeEdge.getValue().set(nextEntry.getDoubleValue());
        return representativeEdge;
      }
    };
  }

  /** Helper class for a mutable edge that modifies the backing map entry. */
  private static class LongDoubleHashMapMutableEdge
      extends DefaultEdge<LongWritable, DoubleWritable> {
    /** Backing entry for the edge in the map. */
    private Long2DoubleMap.Entry entry;

    /** Constructor. */
    public LongDoubleHashMapMutableEdge() {
      super(new LongWritable(), new DoubleWritable());
    }

    /**
     * Make the edge point to the given entry in the backing map.
     *
     * @param entry Backing entry
     */
    public void setEntry(Long2DoubleMap.Entry entry) {
      // Update the id and value objects from the superclass.
      getTargetVertexId().set(entry.getLongKey());
      getValue().set(entry.getValue());
      // Update the entry.
      this.entry = entry;
    }

    @Override
    public void setValue(DoubleWritable value) {
      // Update the value object from the superclass.
      getValue().set(value.get());
      // Update the value stored in the backing map.
      entry.setValue(value.get());
    }
  }

  @Override
  public Iterator<MutableEdge<LongWritable, DoubleWritable>> mutableIterator() {
    return new Iterator<MutableEdge<LongWritable, DoubleWritable>>() {
      /**
       * Wrapped map iterator.
       * Note: we cannot use the fast iterator in this case,
       * because we need to call setValue() on an entry.
       */
      private ObjectIterator<Long2DoubleMap.Entry> mapIterator =
          edgeMap.long2DoubleEntrySet().iterator();
      /** Representative edge object. */
      private LongDoubleHashMapMutableEdge representativeEdge =
          new LongDoubleHashMapMutableEdge();

      @Override
      public boolean hasNext() {
        return mapIterator.hasNext();
      }

      @Override
      public MutableEdge<LongWritable, DoubleWritable> next() {
        representativeEdge.setEntry(mapIterator.next());
        return representativeEdge;
      }

      @Override
      public void remove() {
        mapIterator.remove();
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(edgeMap.size());
    for (Long2DoubleMap.Entry entry : edgeMap.long2DoubleEntrySet()) {
      out.writeLong(entry.getLongKey());
      out.writeDouble(entry.getDoubleValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEdges = in.readInt();
    initialize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      edgeMap.put(in.readLong(), in.readDouble());
    }
  }
}
