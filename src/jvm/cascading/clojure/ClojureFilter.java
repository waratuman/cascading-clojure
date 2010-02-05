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
  private String clojure_ns;
  private String clojure_var;
  private IFn clojure_pred;
  
  
  public ClojureFilter(String clojure_ns, String clojure_var) {
    this.clojure_ns = clojure_ns;
    this.clojure_var = clojure_var;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.clojure_pred = (IFn) Util.bootToVar(this.clojure_ns, this.clojure_var);
  }

  public boolean isRemove(FlowProcess flow_process, FilterCall filter_call) {
    Tuple filter_args = filter_call.getArguments().getTuple();
    ISeq filter_args_seq = IteratorSeq.create(filter_args.iterator());
    Object result;
    try {
      result = (this.clojure_pred.applyTo(filter_args_seq));
      return ((result == null) || (Boolean.FALSE.equals(result)));
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }
}
