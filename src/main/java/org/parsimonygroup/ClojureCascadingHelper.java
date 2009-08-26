package org.parsimonygroup;

import cascading.tuple.TupleEntry;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.*;

import java.io.Serializable;

public class ClojureCascadingHelper implements Serializable {
  private String fnNsName;

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

		// Call require on our utility clojure code
		fnRequire.invoke(Symbol.create(fnNsName));
		fnRequire.invoke(Symbol.create("org.parsimonygroup.cascading"));

		// This will work, get the length
//		final Var len = Var.intern(functionsNS, Symbol.create("length"));
//		System.out.println(len.invoke("1234"));
//    RT.loadResourceScript("org/parsimonygroup/cascading.clj");
  }

  public Object loadFunctions(String functions) throws Exception {
    bootClojure();
    return RT.var(fnNsName, functions).invoke();
  }

  public Object callClojure(TupleEntry arguments, IFn function, IFn dataConverter, IFn reader, IFn writer) throws Exception {
    return dataConverter.invoke(reader, writer, function, clojureData(arguments));
  }

  private Object clojureData(TupleEntry arguments) {
    Fields fields = arguments.getFields();
    fields.size();
    Tuple tuple = new Tuple(arguments.getTuple().get(fields.size() -1));
    return tuple.getString(0);
  }

  public Object callClojure(IFn f) throws Exception {
    return f.invoke();
  }

  // for multiple groupbys per file/line
  public Object callClojure(TupleEntryCollector outputCollector, TupleEntry arguments, IFn f, IFn dataConverter, IFn reader, IFn writer) throws Exception {
     return dataConverter.invoke(reader, writer, f, outputCollector, clojureData(arguments));
  }

  public Object callClojure(Object acc, TupleEntry arguments, IFn aggregateFn, IFn dataConverter, IFn reader, IFn writer) throws Exception {
     return dataConverter.invoke(reader, writer, aggregateFn, acc, clojureData(arguments));
  }

  // for joins
  public Object callClojure(Comparable[] args, IFn joinFn, IFn dataConverter, IFn reader, IFn writer) throws Exception {
    return dataConverter.invoke(reader, writer, joinFn, args);
  }

}
