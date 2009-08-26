package org.parsimonygroup;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Tuple;
import clojure.lang.IFn;

public class AggregationOperationBootstrap extends BaseOperation<Object[]> implements Aggregator<Object[]>  {
  private IFn reader;
  private IFn initFn;
  private IFn aggregateFn;
  private ClojureCascadingHelper clojureHelper;
  private IFn converter;
  private IFn cljCallback;

  public AggregationOperationBootstrap(IFn reader, IFn converter, IFn aggregateFn, IFn initFn, IFn cljCallback, String fnNsName) {
    this.reader = reader;
    this.initFn = initFn;
    this.aggregateFn = aggregateFn;
    this.converter = converter;
    this.cljCallback = cljCallback;
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
  }

  public void start(FlowProcess flowProcess, AggregatorCall<Object[]> aggregatorCall) {
    try {
      clojureHelper.bootClojure();
      if(aggregatorCall.getContext() == null) {
        aggregatorCall.setContext(new Object[] {clojureHelper.callClojure(initFn)});
      } else {
        aggregatorCall.getContext()[0] = clojureHelper.callClojure(initFn);
      }
    } catch (Exception e) {
      e.printStackTrace();  
    }

  }

  public void aggregate(FlowProcess flowProcess, AggregatorCall<Object[]> aggregatorCall) {
    try {
      clojureHelper.bootClojure();
      aggregatorCall.getContext()[0] = clojureHelper.callClojure(aggregatorCall.getContext()[0], aggregatorCall.getArguments(), aggregateFn, cljCallback, reader, converter);
    } catch (Exception e) {
      e.printStackTrace(); 
    }

  }

  public void complete(FlowProcess flowProcess, AggregatorCall<Object[]> aggregatorCall) {
    try {
      aggregatorCall.getOutputCollector().add(new Tuple((Comparable) converter.invoke(aggregatorCall.getContext()[0].toString())));
    } catch (Exception e) {
      e.printStackTrace();  
    }
  }
}
