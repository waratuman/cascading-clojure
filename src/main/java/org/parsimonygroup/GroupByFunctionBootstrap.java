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

public class GroupByFunctionBootstrap extends BaseOperation implements Function {
  private IFn rdr;
  private IFn writer;
  private IFn function;
  private IFn groupBy;
  private IFn cljCallback;
  private ClojureCascadingHelper clojureHelper;
  private static Fields outputFields = new Fields("key", "clojurecode");
  private static final Logger LOG = Logger.getLogger( GroupByFunctionBootstrap.class );

  public GroupByFunctionBootstrap(IFn rdr, IFn writer, IFn function, IFn groupBy, IFn cljCallback, String fnNsName) {
    super(1, outputFields);
    this.rdr = rdr;
    this.writer = writer;
    this.function = function;
    this.groupBy = groupBy;
    this.cljCallback = cljCallback;
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
  }

  public GroupByFunctionBootstrap(Fields fields, String fnNsName) {
    super(1, fields);
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
      String key = (String) clojureHelper.callClojure(arguments, groupBy, cljCallback, rdr, writer);
      //     LOG.fatal("key=" + key);
       String data = ((String) clojureHelper.callClojure(arguments, function, cljCallback, rdr, writer));
       //String data = ((String) clojureHelper.callClojure(outputCollector, arguments, function, cljCallback, rdr, writer));

      //      //LOG.fatal("data=" + data);
      if(data != null && data.trim().length() > 0 && !"nil".equalsIgnoreCase(data)) {
	  //  LOG.fatal("adding tuple with key of" + key + "and data of=" +data);
        outputCollector.add(new Tuple(key, data));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}