package cascading.clojure;

import cascading.operation.BaseOperation;
import cascading.operation.Aggregator;
import cascading.operation.OperationCall;
import cascading.operation.AggregatorCall;
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

public class ClojureAggregator extends BaseOperation<ClojureAggregator.Box>
                               implements Aggregator<ClojureAggregator.Box> {
  
  public static class Box {
    public Object val;
    
    public Box(Object val) {
      this.val = val;
    }
  }
  
  private String start_clj_ns;
  private String start_clj_var;
  private IFn    start_clj_fn;
  private String aggregate_clj_ns;
  private String aggregate_clj_var;
  private IFn    aggregate_clj_fn;
  private String complete_clj_ns;
  private String complete_clj_var;
  private IFn    complete_clj_fn;

  public ClojureAggregator(Fields out_fields,
                           String start_clj_ns,     String start_clj_var,
                           String aggregate_clj_ns, String aggregate_clj_var,
                           String complete_clj_ns,  String complete_clj_var) {
    super(out_fields);
    this.start_clj_ns =      start_clj_ns;
    this.start_clj_var =     start_clj_var;
    this.aggregate_clj_ns =  aggregate_clj_ns;
    this.aggregate_clj_var = aggregate_clj_var;
    this.complete_clj_ns =   complete_clj_ns;
    this.complete_clj_var =  complete_clj_var;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall<Box> ag_call) {
    this.start_clj_fn =     (IFn) Util.bootToVar(this.start_clj_ns,     this.start_clj_var);
    this.aggregate_clj_fn = (IFn) Util.bootToVar(this.aggregate_clj_ns, this.aggregate_clj_var);
    this.complete_clj_fn =  (IFn) Util.bootToVar(this.complete_clj_ns,  this.complete_clj_var);
  }

  public void start(FlowProcess flow_process, AggregatorCall<Box> ag_call) {
    try {
      ag_call.setContext(new Box(this.start_clj_fn.invoke()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void aggregate(FlowProcess flow_process, AggregatorCall<Box> ag_call) {
    try {
      Tuple fn_args = ag_call.getArguments().getTuple();
      ISeq fn_args_seq = Util.coerceSeq(fn_args);
      Box box = ag_call.getContext();
      box.val = this.aggregate_clj_fn.applyTo(RT.cons(box.val, fn_args_seq));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void complete(FlowProcess flow_process, AggregatorCall<Box> ag_call) {
    try {
      Box box = ag_call.getContext();
      Collection clj_tuple = (Collection) this.complete_clj_fn.invoke(box.val);
      ag_call.getOutputCollector().add(Util.coerceTuple(clj_tuple));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
