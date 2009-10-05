package org.parsimonygroup;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import clojure.lang.IFn;

import java.util.Collection;

public class AggregationOperationBootstrap extends BaseOperation<Object[]> implements Aggregator<Object[]>  {
  private IFn reader;
  private IFn initFn;
  private IFn aggregateFn;
  private ClojureCascadingHelper clojureHelper;
  private IFn writer;
  private IFn cljCallback;

  public AggregationOperationBootstrap(Fields inFields, Fields outFields, IFn reader, IFn writer, IFn aggregateFn, IFn initFn, IFn cljCallback, String fnNsName) {
    super(inFields.size(), outFields);
    this.reader = reader;
    this.initFn = initFn;
    this.aggregateFn = aggregateFn;
    this.writer = writer;
    this.cljCallback = cljCallback;
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
  }

  @Override
  public void prepare(FlowProcess flowProcess, OperationCall<Object[]> operationCall) {
    super.prepare(flowProcess, operationCall);    //To change body of overridden methods use File | Settings | File Templates.
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
      aggregatorCall.getContext()[0] = clojureHelper.callClojure(aggregatorCall.getContext()[0], aggregatorCall.getArguments(), aggregateFn, cljCallback, reader, writer);
    } catch (Exception e) {
      e.printStackTrace(); 
    }

  }

  public void complete(FlowProcess flowProcess, AggregatorCall<Object[]> aggregatorCall) {
    try {
      Collection resultRow = (Collection) aggregatorCall.getContext()[0];
      Comparable [] rowForTuple = new Comparable[resultRow.size()];
      int i = 0;
      for(Object data : resultRow) {
        rowForTuple[i] = (Comparable) writer.invoke(data);
        i++;
      }
      aggregatorCall.getOutputCollector().add(new Tuple(rowForTuple));
    } catch (Exception e) {
      e.printStackTrace();  
    }
  }
}
