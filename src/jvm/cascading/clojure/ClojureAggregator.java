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
import java.util.Collection;

public class ClojureAggregator extends BaseOperation<Object>
                               implements Aggregator<Object> {
  private Object[] start_fn_spec;
  private IFn      start_fn;
  private Object[] aggregate_fn_spec;
  private IFn      aggregate_fn;
  private Object[] complete_fn_spec;
  private IFn      complete_fn;

  public ClojureAggregator(Fields out_fields, Object[] start_fn_spec,
                           Object[] aggregate_fn_spec, Object[] complete_fn_spec) {
    super(out_fields);
    this.start_fn_spec =      start_fn_spec;
    this.aggregate_fn_spec =  aggregate_fn_spec;
    this.complete_fn_spec =   complete_fn_spec;
  }
  
  public void prepare(FlowProcess flow_process, OperationCall<Object> op_call) {
    this.start_fn =     Util.bootFn(start_fn_spec);
    this.aggregate_fn = Util.bootFn(aggregate_fn_spec);
    this.complete_fn =  Util.bootFn(complete_fn_spec);
  }

  public void start(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
    try {
      ag_call.setContext(this.start_fn.invoke());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void aggregate(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
    try {
      ISeq fn_args_seq = Util.coerceFromTuple(ag_call.getArguments().getTuple());
      ag_call.setContext(
        this.aggregate_fn.applyTo(RT.cons(ag_call.getContext(), fn_args_seq)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void complete(FlowProcess flow_process, AggregatorCall<Object> ag_call) {
    try {
      Collection coll = (Collection) this.complete_fn.invoke(ag_call.getContext());
      ag_call.getOutputCollector().add(Util.coerceToTuple(coll));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
