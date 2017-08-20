/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.client.solrj.io.stream.metrics;

//import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
//import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
//import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import org.apache.solr.common.util.Hash;


import org.apache.solr.client.solrj.io.Tuple;
//import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;

import java.util.Map;

//import com.clearspring.analytics.stream.membership.Filter;
//import com.clearspring.analytics.util.Preconditions;

/**
 * Count-Min Sketch datastructure. An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
public class CMSMetric extends Metric {// , implements /*IFrequency, */Serializable {

  public static final long PRIME_MODULUS = (1L << 31) - 1;
  private static final long serialVersionUID = -5084982213094657923L;

  int depth;
  int width;
  long[][] table;
  long[] hashA;
  long size;
  double eps;
  double confidence;

  private Map<String,Long> mapStringFacet = new HashMap<String,Long>();
  private Map<Long,Long> mapLongFacet = new HashMap<Long,Long>();
  private String columnName;
  private int TopFacets;

  public CMSMetric(String columnName, int topFacets, int width, int depth) {
    init("CMScount", columnName, topFacets, width, depth);
    this.eps = 2.0 / width;
    this.confidence = 1 - 1 / Math.pow(2, depth);
    initTablesWith(depth, width, 0);
  }

  public CMSMetric(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    String functionName = expression.getFunctionName();
    String columnName = factory.getValueOperand(expression, 0);
    String topFacetsStr = factory.getValueOperand(expression, 1);
    String widthStr = factory.getValueOperand(expression, 2);
    String depthStr = factory.getValueOperand(expression, 3);

    if (1 > expression.getParameters().size()) {
      throw new IOException(String.format(Locale.ROOT, "Invalid expression %s - name oprant is not found", expression));
    }
    
    int topFacets = 20;
    if (2 < expression.getParameters().size())
    try{
      topFacets = Integer.parseInt(topFacetsStr);
      if(topFacets <= 0 || topFacets > 100){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - topFacets '%s' must be greater than 0. and less or equal than 100",expression, topFacetsStr));
      }
    }
    catch(NumberFormatException e){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - topFacets '%s' is not a valid integer.",expression, topFacetsStr));
    }

    int width = 2000;
    if (3 < expression.getParameters().size())
    try{
      width = Integer.parseInt(widthStr);
      if(width <= 0 || width > 100000){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - width '%s' must be greater than 0. and less or equal than 100000",expression, widthStr));
      }
    }
    catch(NumberFormatException e){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - width '%s' is not a valid integer.",expression, widthStr));
    }

    int depth = 10;
    if (4 < expression.getParameters().size())
    try{
      depth = Integer.parseInt(depthStr);
      if(depth <= 0 || depth > 100){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - depth '%s' must be greater than 0. and less or equal than 100",expression, depthStr));
      }
    }
    catch(NumberFormatException e){
      throw new IOException(String.format(Locale.ROOT,"invalid expression %s - depth '%s' is not a valid integer.",expression, depthStr));
    }

    
    init(functionName, columnName, topFacets, width, depth);
  }

  public String[] getColumns() {
    if (isAllColumns()) {
      return new String[0];
    }
    return new String[] {columnName};
  }

  private void init(String functionName, String columnName, int topFacets, int width, int depth) {
    this.columnName = columnName;
    setFunctionName(functionName);
    setIdentifier(functionName, "(", columnName, ")");
    this.TopFacets = topFacets;
    this.width = width;
    this.depth = depth;
    
  }

  private boolean isAllColumns() {
    return "*".equals(this.columnName);
  }

  public void update(Tuple tuple) {
    if (isAllColumns() || tuple.get(columnName) != null) {
      Class<? extends Object> clazz = tuple.get(columnName).getClass();
      
      if ("java.lang.String" == clazz.getName())
        add(tuple.get(columnName).toString(), 1);
      else if ("long" == clazz.getName() || "integer" == clazz.getName() || "byte" == clazz.getName() || "short" == clazz.getName() || "boolean" == clazz.getName())
        add((long)tuple.get(columnName), 1);
      else if ("java.util.ArrayList" == clazz.getName()) {
        ArrayList array = (ArrayList)tuple.get(columnName);
        for (Object item : array) {
          if (item instanceof String)
            add((String)item, 1);
          else
            add((long)item, 1);
        }
      }
    }
  }

  public Long getValue() {
    return (long)mapStringFacet.size();
  }

  public Metric newInstance() {
    return new CMSMetric(columnName, TopFacets, width, depth);
  }

  @Override
  public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
    return new StreamExpression(getFunctionName()).withParameter(columnName);
  }

  @Override
  public String toString() {
    return getJSON().toString();
  }

  // @Override
  public int hashCode() {
    int result;
    long temp;
    result = depth;
    result = 31 * result + width;
    result = 31 * result + Arrays.deepHashCode(table);
    result = 31 * result + Arrays.hashCode(hashA);
    result = 31 * result + (int) (size ^ (size >>> 32));
    temp = Double.doubleToLongBits(eps);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(confidence);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  private void initTablesWith(int depth, int width, int seed) {
    this.table = new long[depth][width];
    this.hashA = new long[depth];
    Random r = new Random(seed);
    // We're using a linear hash functions
    // of the form (a*x+b) mod p.
    // a,b are chosen independently for each hash function.
    // However we can set b = 0 as all it does is shift the results
    // without compromising their uniformity or independence with
    // the other hashes.
    for (int i = 0; i < depth; ++i) {
      hashA[i] = r.nextInt(Integer.MAX_VALUE);
    }
  }

  public double getRelativeError() {
    return eps;
  }

  public double getConfidence() {
    return confidence;
  }

  int hash(long item, int i) {
    long hash = hashA[i] * item;
    // A super fast way of computing x mod 2^p-1
    // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
    // page 149, right after Proposition 7.
    hash += hash >> 32;
    hash &= PRIME_MODULUS;
    // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
    return ((int) hash) % width;
  }

  // Murmur is faster than an SHA-based approach and provides as-good collision
  // resistance. The combinatorial generation approach described in
  // https://gnunet.org/sites/default/files/LessHashing2006Kirsch.pdf
  // does prove to work in actual tests, and is obviously faster
  // than performing further iterations of murmur.
  public static int[] getHashBuckets(String key, int hashCount, int max) {
    byte[] b;
    try {
      b = key.getBytes("UTF-16");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return getHashBuckets(b, hashCount, max);
  }

  static int[] getHashBuckets(byte[] b, int hashCount, int max) {
    int[] result = new int[hashCount];
    //int hash1 = MurmurHash2hash(b, 0, b.length);
    //int hash2 = MurmurHash2hash(b, 1, b.length - 1);
    int hash1 = Hash.murmurhash3_x86_32(b, 0, b.length, 0);
    int hash2 = Hash.murmurhash3_x86_32(b, 0, b.length, hash1);
    for (int i = 0; i < hashCount; i++) {
      result[i] = Math.abs((hash1 + i * hash2) % max);
    }
    return result;
  }

  // @Override
  public void add(long item, long count) {
    if (count < 0) {
      // Actually for negative increments we'll need to use the median
      // instead of minimum, and accuracy will suffer somewhat.
      // Probably makes sense to add an "allow negative increments"
      // parameter to constructor.
      throw new IllegalArgumentException("Negative increments not implemented");
    }
    long countCurrent = addLong2table(item, count);
    if (mapLongFacet.containsKey(item)) {
      mapLongFacet.replace(item, countCurrent);
    } else if (mapLongFacet.size() < TopFacets) {
      mapLongFacet.put(item, countCurrent);
    } else {
      String minCountString = getMinStringCount();
      mapLongFacet.remove(minCountString);
      mapLongFacet.put(item, countCurrent);
    }
  }

  public void add(String item, long count) {
    if (count < 0) {
      // Actually for negative increments we'll need to use the median
      // instead of minimum, and accuracy will suffer somewhat.
      // Probably makes sense to add an "allow negative increments"
      // parameter to constructor.
      throw new IllegalArgumentException("Negative increments not implemented");
    }
    long countCurrent = addString2table(item, count);
    if (mapStringFacet.containsKey(item)) {
      mapStringFacet.replace(item, countCurrent);
    } else if (mapStringFacet.size() < TopFacets) {
      mapStringFacet.put(item, countCurrent);
    } else {
      String minCountString = getMinStringCount();
      mapStringFacet.remove(minCountString);
      mapStringFacet.put(item, countCurrent);
    }
  }

  public long addString2table(String item, long count) {
    long minCountLocal = Long.MAX_VALUE;
    int[] buckets = getHashBuckets(item, depth, width);
    for (int i = 0; i < depth; ++i) {
      table[i][buckets[i]] += count;
      minCountLocal = Math.min(minCountLocal, table[i][buckets[i]]);
    }
    return minCountLocal;
  }

  public String getMinStringCount() {
    String minCountString = null;
    long minCount = Long.MAX_VALUE;
    
    for (Map.Entry<String, Long> entry : mapStringFacet.entrySet()) {
      if (minCount >  entry.getValue()) {
        minCount =  entry.getValue();
        minCountString = entry.getKey();
      }
    }
    return minCountString;
  }

  public long addLong2table(long item, long count) {
    long minCountLocal = Long.MAX_VALUE;

    for (int i = 0; i < depth; ++i) {
      table[i][hash(item, i)] += count;
      minCountLocal = Math.min(minCountLocal, table[i][hash(item, i)]);
    }
    return minCountLocal;
  }

  public long getMinLongCount() {
    long minCountLong = Long.MAX_VALUE;
    long minCount = Long.MAX_VALUE;
    
    for (Map.Entry<Long, Long> entry : mapLongFacet.entrySet()) {
      if (minCount >  entry.getValue()) {
        minCount =  entry.getValue();
        minCountLong = entry.getKey();
      }
    }
    return minCountLong;
  }

  // @Override
  public long size() {
    return size;
  }

  /**
   * The estimate is correct within 'epsilon' * (total item count), with probability 'confidence'.
   */
  // @Override
  public long estimateCount(long item) {
    long res = Long.MAX_VALUE;
    for (int i = 0; i < depth; ++i) {
      res = Math.min(res, table[i][hash(item, i)]);
    }
    return res;
  }

  // @Override
  public long estimateCount(String item) {
    long res = Long.MAX_VALUE;
    int[] buckets = getHashBuckets(item, depth, width);
    for (int i = 0; i < depth; ++i) {
      res = Math.min(res, table[i][buckets[i]]);
    }
    return res;
  }

  public static byte[] serialize(CMSMetric sketch) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    DataOutputStream s = new DataOutputStream(bos);
    try {
      s.writeLong(sketch.size);
      s.writeInt(sketch.depth);
      s.writeInt(sketch.width);
      for (int i = 0; i < sketch.depth; ++i) {
        s.writeLong(sketch.hashA[i]);
        for (int j = 0; j < sketch.width; ++j) {
          s.writeLong(sketch.table[i][j]);
        }
      }
      return bos.toByteArray();
    } catch (IOException e) {
      // Shouldn't happen
      throw new RuntimeException(e);
    }
  }



  @SuppressWarnings("serial")
  protected static class CMSMergeException extends Exception {

    public CMSMergeException(String message) {
      super(message);
    }
  }

  public Object getJSON() {
    StringBuilder buf = new StringBuilder();
    // buf.append("\"intermediate_result\"");
    buf.append("{");
    

    if (!mapStringFacet.isEmpty()) {

      buf.append(",");
      buf.append("\"facet\":[");

      boolean first = true;

      List<Map.Entry<String, Long>> list =
          new LinkedList<Map.Entry<String, Long>>(mapStringFacet.entrySet());

      Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
          public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
              return (o2.getValue()).compareTo(o1.getValue());
          }
      });
      
      for (Map.Entry<String, Long> entry : list) {
        if (!first)
          buf.append(",");

        buf.append("{\"");
        buf.append(entry.getKey());
        buf.append("\":");
        buf.append(entry.getValue());
        buf.append("},");
      }
      buf.append("]");
    }
    buf.append("}");
    return buf.toString();
  }
}
