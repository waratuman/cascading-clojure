package cascading.clojure;

import clojure.lang.IFn;
import java.util.Collection;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.BaseOperation;

public class MapOperation extends BaseOperation implements Function {
    private IFn fn;
    
    public MapOperation(IFn fn) {
        super(Fields.ALL);
        this.fn = fn;
    }

    public static Each pipe(Pipe previous, IFn fn) {
        MapOperation operation = new MapOperation(fn);
        return new Each(previous, operation);
    }

    public void operate(FlowProcess flowProcess, FunctionCall fnCall) {
        try {
            Collection result = (Collection)fn.invoke(Util.tupleEntryToMap(fnCall.getArguments()));
            TupleEntry[] emittedTuples = Util.collectionToTupleEntries(result);
            for (TupleEntry tuple : emittedTuples) {
                fnCall.getOutputCollector().add(tuple);
            }
        }
        catch (Exception e) { throw new RuntimeException(e); }
    }
    
}
