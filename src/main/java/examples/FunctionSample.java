package examples;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import clojure.lang.IFn;


public class FunctionSample extends BaseOperation implements Function{

  public FunctionSample() {
    super(1, new Fields("clojurecode"));
  }

  public FunctionSample(Fields fields) {
    super(1, fields);
  }


  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    TupleEntry arguments = functionCall.getArguments();
    Tuple tuple = new Tuple(arguments.getTuple());
    String clojureData = tuple.getString(1);

    // do clojure stuff here
    Tuple result = processData(clojureData);

    TupleEntryCollector outputCollector = functionCall.getOutputCollector();
    outputCollector.add(result);
  }

  private Tuple processData(String clojureData) {
    Tuple result = new Tuple();
    try {
      result.add(clojureData);
    } catch (Exception e) {
      e.printStackTrace();  
    }
    return result;
  }

}
