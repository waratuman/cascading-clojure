package org.parsimonygroup;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.*;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ClojureCascadingHelper implements Serializable {
  private String fnNsName;
   private static final Logger LOG = Logger.getLogger(ClojureCascadingHelper.class);

  public ClojureCascadingHelper(String fnNsName) {
    this.fnNsName = fnNsName;
  }

  public void bootClojure() throws Exception {
    final Symbol symbolClojureMain       = Symbol.create("clojure.main");
    final Namespace namespaceClojureMain = Namespace.findOrCreate(symbolClojureMain);
		final Var varRequire                 = Var.intern(RT.CLOJURE_NS, Symbol.create("require"));

    final Namespace functionsNS = Namespace.findOrCreate(Symbol.create(fnNsName));
    final Var fnRequire = Var.intern(RT.CLOJURE_NS, Symbol.create("require"));

    varRequire.invoke(symbolClojureMain);

		fnRequire.invoke(Symbol.create(fnNsName));
		fnRequire.invoke(Symbol.create("org.parsimonygroup.cascading"));
  }

  public Object loadFunctions(String functions) throws Exception {
    bootClojure();
    return RT.var(fnNsName, functions).invoke();
  }



  private Object clojureData(TupleEntry arguments) {
    Object[] result = new Object[arguments.getFields().size()];
    int i = 0;
    Iterator it = arguments.getTuple().iterator();
    while(it.hasNext() && i < result.length) {
      result[i] = it.next();
      i++;
    }
    return result;
  }

  public Object callClojure(IFn f) throws Exception {
    return f.invoke();
  }

  private Collection<Tuple> toFieldTuples(Object fromClj) {
    List<Tuple> result = new ArrayList<Tuple>();
     Collection cljResult = (Collection) fromClj;
     for(Object r : cljResult) {
       Collection row = (Collection) r;
       Comparable[] rowItems = new Comparable[row.size()];
       int i = 0;
//       System.out.println("*****************************");
       for(Object rowItem : row) {
         rowItems[i] = (Comparable) rowItem;
//         System.out.println(rowItem);
         i++;
       }
//       System.out.println("*****************************");
       result.add(new Tuple(rowItems));
     }
    return result;
  }

   public Collection<Tuple> callClojure(TupleEntry arguments, IFn function, IFn dataConverter, IFn reader, IFn writer) throws Exception {
     return toFieldTuples(dataConverter.invoke(reader, writer, function, clojureData(arguments)));
   }

   public Object groupByGetKey(TupleEntry arguments, IFn function) throws Exception {
     Object data = clojureData(arguments);
     return function.invoke(ArraySeq.create((Object[]) data));
   }

  // for multiple groupbys per file/line
  public Object callClojure(TupleEntryCollector outputCollector, TupleEntry arguments, IFn f, IFn dataConverter, IFn reader, IFn writer) throws Exception {
     return dataConverter.invoke(reader, writer, f, outputCollector, clojureData(arguments));
  }

  public Object callClojure(Object acc, TupleEntry arguments, IFn aggregateFn, IFn dataConverter, IFn reader, IFn writer) throws Exception {
     return dataConverter.invoke(reader, writer, aggregateFn, acc, clojureData(arguments));
  }

  // for joins
  public Tuple callClojure(Comparable[] args, IFn joinFn, IFn dataConverter, IFn reader, IFn writer) throws Exception {
    Collection cljResultRow = (Collection) dataConverter.invoke(reader, writer, joinFn, args);
    Comparable[] result = new Comparable[cljResultRow.size()];
    int i = 0;
    for(Object item : cljResultRow) {
      LOG.debug(i + "th item " + item);
      result[i] = (Comparable) item;
        i++;
    }
    return new Tuple(result);
  }

  public Boolean filterCall(TupleEntry arguments, IFn function, IFn cljCallback, IFn reader, IFn writer) throws Exception {
    return (Boolean) cljCallback.invoke(reader, writer, function, clojureData(arguments));
  }
}
