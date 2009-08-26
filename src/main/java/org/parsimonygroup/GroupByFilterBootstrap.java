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

public class GroupByFilterBootstrap extends BaseOperation implements Function {
  private IFn reader;
  private IFn function;
  private IFn groupBy;
  private ClojureCascadingHelper clojureHelper;
  private static Fields outputFields = new Fields("key", "clojurecode");
  private IFn writer;
  private IFn cljCallBack;

  public GroupByFilterBootstrap(IFn reader, IFn writer, IFn function, IFn groupBy, IFn cljCallBack, String fnNsName) {
    super(1, outputFields);
    this.reader = reader;
    this.function = function;
    this.groupBy = groupBy;
    this.writer = writer;
    this.cljCallBack = cljCallBack;
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
  }

  public GroupByFilterBootstrap(Fields fields, String fnNsName) {
    super(1, fields);
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
  }

  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    try {
      clojureHelper.bootClojure();
    } catch (Exception e) {
      e.printStackTrace();
    }
    TupleEntry arguments = functionCall.getArguments();

    TupleEntryCollector outputCollector = functionCall.getOutputCollector();
    outputCollector.add(processData(arguments));
  }

    private TupleEntry processData(TupleEntry arguments) {
    Tuple result = new Tuple();
    try {
      if((Boolean) clojureHelper.callClojure(arguments, function, cljCallBack,reader, writer)) {
        result.add((Comparable) clojureHelper.callClojure(arguments, groupBy, cljCallBack, reader, writer));
        result.add(arguments.getTuple().get(arguments.getFields().size() -1));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new TupleEntry(result);
  }
}