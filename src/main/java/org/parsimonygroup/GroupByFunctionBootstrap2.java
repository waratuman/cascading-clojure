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
import org.apache.log4j.Logger;

import java.util.Map;

public class GroupByFunctionBootstrap2 extends BaseOperation implements Function {
  private IFn rdr;
  private IFn writer;
  private IFn function;
  private IFn cljCallback;
  private ClojureCascadingHelper clojureHelper;
  private static final Logger LOG = Logger.getLogger( GroupByFunctionBootstrap2.class );

  public GroupByFunctionBootstrap2(Fields inFields, Fields outFields, IFn rdr, IFn writer, IFn function, IFn cljCallback, String fnNsName) {
    super(inFields.size(), outFields);
    this.rdr = rdr;
    this.writer = writer;
    this.function = function;
    this.cljCallback = cljCallback;
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
  }

  public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    try {
      clojureHelper.bootClojure();
    } catch (Exception e) {
      e.printStackTrace();
    }
    processData(functionCall.getArguments(), functionCall.getOutputCollector());
  }

  private void processData(TupleEntry arguments, TupleEntryCollector outputCollector) {
    try {
      Map result = (Map) clojureHelper.callClojure(outputCollector, arguments, function, cljCallback, rdr, writer);
      for (Object k : result.keySet()) {
        Object data = result.get(k);
//        LOG.fatal("key is" + k);
//        LOG.fatal("data is" + data);
        outputCollector.add(new Tuple((String) k, (String) data));
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}