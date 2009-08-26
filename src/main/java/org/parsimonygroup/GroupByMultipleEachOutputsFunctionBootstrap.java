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
import org.json.JSONArray;
import org.json.JSONObject;

public class GroupByMultipleEachOutputsFunctionBootstrap extends BaseOperation implements Function {
  private IFn rdr;
  private IFn function;
  private IFn cljCallback;
  private ClojureCascadingHelper clojureHelper;
    private static Fields outputFields = new Fields("key", "clojurecode");
  private IFn writer;

  public GroupByMultipleEachOutputsFunctionBootstrap(IFn reader, IFn writer, IFn function, IFn cljCallback, String fnNsName) {
    super(1, outputFields);
    this.rdr = reader;
    this.function = function;
    this.cljCallback = cljCallback;
    this.clojureHelper = new ClojureCascadingHelper(fnNsName);
    this.writer = writer;
  }

  public GroupByMultipleEachOutputsFunctionBootstrap(Fields fields, String fnNsName) {
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

  protected void processData(TupleEntry arguments, TupleEntryCollector outputCollector) {
    try {
      String jsonOutput = (String) clojureHelper.callClojure(arguments, function, cljCallback, rdr, writer);

      JSONArray jsonArray = new JSONArray(jsonOutput);
      for(int i = 0; i < jsonArray.length(); i++) {
        JSONObject jsonMap = jsonArray.getJSONObject(i);
        String groupKey = (String) jsonMap.keys().next();
        outputCollector.add(new Tuple(groupKey, jsonMap.getString(groupKey)));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public void setClojureHelper(ClojureCascadingHelper clojureHelper) {
    this.clojureHelper = clojureHelper;
  }
}