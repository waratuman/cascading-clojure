package org.parsimonygroup;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.IFn;

import java.util.Collection;

public class FunctionBootstrap extends BaseOperation implements Function {
  private IFn reader;
  private IFn function;
  private ClojureCascadingHelper clojureHelper;
  private IFn writer;
  private IFn cljCallback;

  public FunctionBootstrap(Fields inFields, Fields outFields, IFn reader, IFn writer, IFn function, IFn cljCallback, String fnNsName) {
    super( outFields);
    this.reader = reader;
    this.function = function;
    this.writer = writer;
    this.cljCallback = cljCallback;
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
  }

  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    try {
      clojureHelper.bootClojure();
    } catch (Exception e) {
      e.printStackTrace();
    }
    TupleEntryCollector outputCollector = functionCall.getOutputCollector();
    TupleEntry arguments = functionCall.getArguments();
    processData(arguments, outputCollector);
  }

  private void processData(TupleEntry arguments, TupleEntryCollector collector) {
    try {
      Collection<Tuple> resultTuples = clojureHelper.callClojure(arguments, function, cljCallback, reader, writer);
      for(Tuple tuple : resultTuples) {
         collector.add(tuple);
      }
    } catch (Exception e) {
      throw new RuntimeException("error prcessing Data", e);
    }
  }
}
