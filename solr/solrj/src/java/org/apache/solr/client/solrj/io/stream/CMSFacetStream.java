/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.client.solrj.io.Tuple;
//import org.apache.solr.client.solrj.io.comp.FieldComparator;
//import org.apache.solr.client.solrj.io.comp.HashKey;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
//import org.apache.solr.client.solrj.io.eq.FieldEqualitor;
//import org.apache.solr.client.solrj.io.eq.MultipleFieldEqualitor;
//import org.apache.solr.client.solrj.io.eq.StreamEqualitor;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Explanation.ExpressionType;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionValue;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
//import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
//import org.apache.solr.client.solrj.io.stream.metrics.Bucket;
import org.apache.solr.client.solrj.io.stream.metrics.Metric;

public class CMSFacetStream extends TupleStream implements Expressible {

  private static final long serialVersionUID = 1;

  private PushBackStream tupleStream;
  private Metric[] metrics;
  private int bucketSizeLimit;
  private int sizeLimit;
  private float random;
  
  private int currentRowCount = 0;
//  private Map<String,Object> mapPartialResult;
  private boolean finished = false;
  private Metric[] currentMetrics;
  
  public CMSFacetStream(TupleStream tupleStream,
                      int bucketSizeLimit,
                      int sizeLimit,
                      float random,
                      Metric[] metrics) {
    init(tupleStream, bucketSizeLimit, sizeLimit, random, metrics);
  }
  
  public CMSFacetStream(StreamExpression expression, StreamFactory factory) throws IOException {
    // grab all parameters out
    List<StreamExpression> streamExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, TupleStream.class);
    List<StreamExpression> metricExpressions = factory.getExpressionOperandsRepresentingTypes(expression, Expressible.class, Metric.class);
    StreamExpressionNamedParameter bucketSizeLimitExpression = factory.getNamedOperand(expression, "bucketSizeLimit");
    StreamExpressionNamedParameter sizeLimitExpression = factory.getNamedOperand(expression, "sizeLimit");
    StreamExpressionNamedParameter randomExpression = factory.getNamedOperand(expression, "random");

    
    if(1 != streamExpressions.size()){
      throw new IOException(String.format(Locale.ROOT,"Invalid expression %s - expecting a single stream but found %d",expression, streamExpressions.size()));
    }

   
    // Construct the metrics
    Metric[] metrics = new Metric[metricExpressions.size()];
    for(int idx = 0; idx < metricExpressions.size(); ++idx){
      metrics[idx] = factory.constructMetric(metricExpressions.get(idx));
    }
    
    int bucketSizeLimitLocal = 0;
    // Construct the bucket SizeLimit [optional]
    if(null != bucketSizeLimitExpression && null != bucketSizeLimitExpression.getParameter() && (bucketSizeLimitExpression.getParameter() instanceof StreamExpressionValue)){
      String limitStr = ((StreamExpressionValue)bucketSizeLimitExpression.getParameter()).getValue();
      try{
        bucketSizeLimitLocal = Integer.parseInt(limitStr);
        if(bucketSizeLimitLocal <= 0){
          throw new IOException(String.format(Locale.ROOT,"invalid expression %s - bucketSizeLimit '%s' must be greater than 0.",expression, limitStr));
        }
      }
      catch(NumberFormatException e){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - bucketSizeLimit '%s' is not a valid integer.",expression, limitStr));
      }
    }
    
    // Construct the sizeLimit [optional]
    int sizeLimitLocal = 0;
    if(null != sizeLimitExpression && null != sizeLimitExpression.getParameter() && (sizeLimitExpression.getParameter() instanceof StreamExpressionValue)){
      String limitStr = ((StreamExpressionValue)sizeLimitExpression.getParameter()).getValue();
      try{
        sizeLimitLocal = Integer.parseInt(limitStr);
        if(sizeLimitLocal <= 0){
          throw new IOException(String.format(Locale.ROOT,"invalid expression %s - sizeLimit '%s' must be greater than 0.",expression, limitStr));
        }
        if(sizeLimitLocal <= bucketSizeLimitLocal){
          throw new IOException(String.format(Locale.ROOT,"invalid expression %s - bucketSizeLimit '%d' must be smaller than sizeLimit '%s'.",expression, bucketSizeLimitLocal, limitStr));
        }
      }
      catch(NumberFormatException e){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - sizeLimit '%s' is not a valid integer.",expression, limitStr));
      }
    }
    
    // Construct the random [optional]
    float random = 1;
    if(null != randomExpression && null != randomExpression.getParameter() && (randomExpression.getParameter() instanceof StreamExpressionValue)){
      String randomStr = ((StreamExpressionValue)randomExpression.getParameter()).getValue();
      try{
        random = Float.parseFloat(randomStr);
        if(random <= 0 || random > 1){
          throw new IOException(String.format(Locale.ROOT,"invalid expression %s - random '%s' must be greater than 0. and less or equal than 1.",expression, randomStr));
        }
      }
      catch(NumberFormatException e){
        throw new IOException(String.format(Locale.ROOT,"invalid expression %s - random '%s' is not a valid float.",expression, randomStr));
      }
    }    
    init(factory.constructStream(streamExpressions.get(0)), bucketSizeLimitLocal, sizeLimitLocal, random, metrics);
  }
  
  
  private void init(TupleStream tupleStream, int bucketSizeLimitLocal, int sizeLimitLocal, float random, Metric[] metrics){
    this.tupleStream = new PushBackStream(tupleStream);
    this.bucketSizeLimit = bucketSizeLimitLocal;
    this.sizeLimit = sizeLimitLocal;
    this.random = random;
    this.metrics = metrics;
  }

  @Override
  public StreamExpression toExpression(StreamFactory factory) throws IOException{
    return toExpression(factory, true);
  }
  
  private StreamExpression toExpression(StreamFactory factory, boolean includeStreams) throws IOException {
    // function name
    StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
    
    // stream
    if(includeStreams){
      expression.addParameter(tupleStream.toExpression(factory));
    }
    else{
      expression.addParameter("<stream>");
    }
    
    // bucketSizeLimit
    if (bucketSizeLimit > 0) {
      expression.addParameter(new StreamExpressionNamedParameter("bucketSizeLimit", Integer.toString(bucketSizeLimit)));
    }

    //sizeLimit
    if (sizeLimit > 0) {
      expression.addParameter(new StreamExpressionNamedParameter("sizeLimit", Integer.toString(sizeLimit)));
    }

    //sizeLimit
    if (random > 0) {
      expression.addParameter(new StreamExpressionNamedParameter("random", Float.toString(random)));
    }

    // metrics
    for(Metric metric : metrics){
      expression.addParameter(metric.toExpression(factory));
    }
    
    return expression;
  }
  
  @Override
  public Explanation toExplanation(StreamFactory factory) throws IOException {

    Explanation explanation = new StreamExplanation(getStreamNodeId().toString())
      .withChildren(new Explanation[]{
        tupleStream.toExplanation(factory)
      })
      .withFunctionName(factory.getFunctionName(this.getClass()))
      .withImplementingClass(this.getClass().getName())
      .withExpressionType(ExpressionType.STREAM_DECORATOR)
      .withExpression(toExpression(factory, false).toString());
    
    for(Metric metric : metrics){
      explanation.withHelper(metric.toExplanation(factory));
    }

    return explanation;
  }

  public void setStreamContext(StreamContext context) {
    this.tupleStream.setStreamContext(context);
  }

  public List<TupleStream> children() {
    List<TupleStream> l =  new ArrayList<TupleStream>();
    l.add(tupleStream);
    return l;
  }

  public void open() throws IOException {
    tupleStream.open();
   
    currentMetrics = new Metric[metrics.length];
    for(int i=0; i<metrics.length; i++) {
      Metric bucketMetric = metrics[i].newInstance();
      currentMetrics[i]  = bucketMetric;
    }
  }

  public void close() throws IOException {
    tupleStream.close();
    
    currentMetrics = null;
  }

  public Tuple read() throws IOException {

    while(true) {
      Tuple t = null;
      currentRowCount++;
      
      Tuple tuple = tupleStream.read();
      if(tuple.EOF || (currentRowCount >= sizeLimit && sizeLimit > 0)) {
        if(!finished) {
          Map<String,Object> mapEOF = new HashMap<String,Object>();
          mapEOF.put("EOF", true);
          tupleStream.pushBack(new Tuple(mapEOF));
          finished = true;
          
          Map<String,Object> map = new HashMap<String,Object>();
          for(Metric metric : currentMetrics) {
            map.put(metric.getIdentifier(), metric.toString());
          }
          t = new Tuple(map);
          return t;
        } else {
          return tuple;
        }
      }
      else if (bucketSizeLimit > 0 && currentRowCount % bucketSizeLimit == 0) {
        Map<String,Object> map = new HashMap<String,Object>();

        for(Metric metric : currentMetrics) {
          map.put(metric.getIdentifier(), metric.toString());
        }
        t = new Tuple(map);
      }
      
      for(Metric bucketMetric : currentMetrics) {
        bucketMetric.update(tuple);
      }
      
      if(t != null) {
        return t;
      }
    }
  }

  public int getCost() {
    return 0;
  }

  @Override
  public StreamComparator getStreamSort() {
    return tupleStream.getStreamSort();
  }

}