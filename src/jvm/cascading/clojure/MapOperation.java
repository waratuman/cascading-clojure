package cascading.clojure;

import clojure.lang.IFn;
import java.util.Iterator;
import java.util.Collection;

import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Tuple;
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
    
    public MapOperation(IFn fn, Fields fieldsDeclaration) {
        super(fieldsDeclaration);
        this.fn = fn;
    }

    public static Each pipe(Pipe previous, IFn fn) {
        return new Each(previous, new MapOperation(fn));
    }

    public static Each pipe(Pipe previous, Fields argumentSelector, IFn fn) {
        MapOperation operation = new MapOperation(fn, argumentSelector);
        return new Each(previous, argumentSelector, operation);
    }

    public static Each pipe(Pipe previous, Fields argumentSelector, IFn fn, Fields outputSelector) {
        MapOperation operation = new MapOperation(fn, outputSelector);
        return new Each(previous, argumentSelector, operation, Fields.RESULTS);
    }
    
    public void operate(FlowProcess flowProcess, FunctionCall fnCall) {
        try {
            Collection result = (Collection)fn.invoke(Util.tupleEntryToMap(fnCall.getArguments()));
            TupleEntry[] emittedTuples = Util.collectionToTupleEntries(result);
            
            for (TupleEntry tupleEntry : emittedTuples) {
                if (fieldDeclaration.size() != 0) {
                    Tuple emitTuple = new Tuple();
                    for (Object key : fieldDeclaration) {
                        emitTuple.add(tupleEntry.get((Comparable)key));
                    }
                    fnCall.getOutputCollector().add(emitTuple);
                }
                else { 
                    fnCall.getOutputCollector().add(tupleEntry);
                }
            }
        }
        catch (Exception e) { throw new RuntimeException(e); }
    }
    
}
