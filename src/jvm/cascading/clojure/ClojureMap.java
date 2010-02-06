package cascading.clojure;

import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import java.util.Collection;

public class ClojureMap extends BaseOperation implements Function {
  private String clj_ns;
  private String clj_var;
  private IFn clj_fn;
  
  public ClojureMap(Fields out_fields, String clj_ns, String clj_var) {
    super(out_fields);
    this.clj_ns = clj_ns;
    this.clj_var = clj_var;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.clj_fn = (IFn) Util.bootToVar(this.clj_ns, this.clj_var);
  }

  public void operate(FlowProcess flow_process, FunctionCall fn_call) {
    Tuple fn_args = fn_call.getArguments().getTuple();
    ISeq fn_args_seq = Util.coerceSeq(fn_args);
    try {
      Collection clj_tuple = (Collection) this.clj_fn.applyTo(fn_args_seq);
      TupleEntryCollector collector = fn_call.getOutputCollector();
      collector.add(Util.coerceTuple(clj_tuple));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
