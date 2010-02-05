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
import java.util.List;

public class ClojureMapcat extends BaseOperation implements Function {
  private String clojure_ns;
  private String clojure_var;
  private IFn clojure_fn;
  
  public ClojureMapcat(Fields out_fields, String clojure_ns, String clojure_var) {
    super(out_fields);
    this.clojure_ns = clojure_ns;
    this.clojure_var = clojure_var;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall op_call) {
    this.clojure_fn = (IFn) RT.var(this.clojure_ns, this.clojure_var).deref();
  }

  public void operate(FlowProcess flow_process, FunctionCall fn_call) {
    Tuple fn_args = fn_call.getArguments().getTuple();
    ISeq fn_args_seq = IteratorSeq.create(fn_args.iterator());
    Object result;
    try {
      result = (this.clojure_fn.applyTo(fn_args_seq));
      ISeq seq = RT.seq(result);
      TupleEntryCollector collector = fn_call.getOutputCollector();
      while (seq != null) {
        Object elem = seq.first();
        collector.add(new Tuple((Comparable[]) RT.seqToTypedArray(RT.seq(elem))));
        seq = seq.next();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
