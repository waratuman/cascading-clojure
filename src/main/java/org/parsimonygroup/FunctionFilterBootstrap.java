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

public class FunctionFilterBootstrap extends BaseOperation implements Function {
  private IFn reader;
  private IFn function;
  private ClojureCascadingHelper clojureHelper;
  private IFn writer;
  private IFn cljCallback;

  public FunctionFilterBootstrap(IFn reader,IFn writer, IFn function, IFn cljCallback, String fnNsName) {
    super(1, new Fields("clojurecode"));
    this.reader = reader;
    this.function = function;
    this.writer = writer;
    this.cljCallback = cljCallback;
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
  }

  public FunctionFilterBootstrap(Fields fields, String fnNsName) {
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
    Tuple result = processData(arguments);

    TupleEntryCollector outputCollector = functionCall.getOutputCollector();
    outputCollector.add(result);
  }

  private Tuple processData(TupleEntry arguments) {
    Tuple result = new Tuple();
    try {
      if((Boolean)clojureHelper.callClojure(arguments, function, cljCallback, reader, writer)) {
        result.add(arguments.getTuple().get(arguments.getFields().size() -1));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result;
  }
}