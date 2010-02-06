package cascading.clojure;

import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.OperationCall;
import cascading.flow.FlowProcess;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;

public class ClojureFilter extends BaseOperation implements Filter {
  private String clj_ns;
  private String clj_var;
  private IFn clj_pred;
  
  public ClojureFilter(String clj_ns, String clj_var) {
    this.clj_ns = clj_ns;
    this.clj_var = clj_var;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.clj_pred = (IFn) Util.bootToVar(this.clj_ns, this.clj_var);
  }

  public boolean isRemove(FlowProcess flow_process, FilterCall filter_call) {
    Tuple filter_args = filter_call.getArguments().getTuple();
    ISeq filter_args_seq = Util.coerceSeq(filter_args);
    try {
      return !Util.truthy(this.clj_pred.applyTo(filter_args_seq));
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }
}
