package cascading.clojure;

import clojure.lang.IFn;
import cascading.tuple.Fields;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.BaseOperation;
import cascading.operation.FilterCall;

public class FilterOperation extends BaseOperation implements Filter {
    private IFn fn;
    
    public FilterOperation(IFn fn) {
        this.fn = fn;
    }

    public static Each pipe(Pipe previous, IFn fn) {
        return new Each(previous, new FilterOperation(fn));
    }
    
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        try {
            Object result = fn.invoke(Util.tupleEntryToMap(filterCall.getArguments()));
            return ((result == null) || (Boolean.FALSE.equals(result)));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
}
