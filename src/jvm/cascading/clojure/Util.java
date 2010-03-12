package cascading.clojure;

import clojure.lang.RT;
import clojure.lang.IFn;
import clojure.lang.ISeq;
import clojure.lang.IPersistentCollection;
import clojure.lang.IteratorSeq;
import clojure.lang.ArraySeq;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.operation.OperationCall;
import java.util.Collection;

public class Util {
  public static IFn bootSimpleFn(String ns_name, String fn_name) {
    String root_path = ns_name.replace('-', '_').replace('.', '/');
    try {
      RT.load(root_path);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return (IFn) RT.var(ns_name, fn_name).deref();
  }
  
  public static IFn bootFn(Object[] fn_spec) {
    String ns_name = (String) fn_spec[0];
    String fn_name = (String) fn_spec[1];
    IFn simple_fn = bootSimpleFn(ns_name, fn_name);
    if (fn_spec.length == 2) {
      return simple_fn;
    } else {
      ISeq hof_args = ArraySeq.create(fn_spec).next().next();
      try {
        return (IFn) simple_fn.applyTo(hof_args);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
 public static Object[] coerceArrayFromTuple(Tuple tuple) {
    int s = tuple.size();
    Object[] obj_elems = new Object[s];
    for (int i= 0; i < s; i++) {
      Comparable comp_elem = tuple.get(i);
      if (comp_elem instanceof ClojureWrapper) {
        obj_elems[i] = ((ClojureWrapper)comp_elem).toClojure();
      } else {
        obj_elems[i] = comp_elem;
      }
    }
    return obj_elems;
  }

  public static ISeq coerceFromTuple(Tuple tuple) {
    return ArraySeq.create(coerceArrayFromTuple(tuple));
  }

  public static ISeq coerceFromTuple(TupleEntry tuple) {
    return coerceFromTuple(tuple.getTuple());
  }
  
  public static Tuple coerceToTuple(Object obj) {
    if(obj instanceof Collection) {
      Object[] obj_elems = ((Collection)obj).toArray();
      int s = obj_elems.length;
      Comparable[] comp_elems = new Comparable[s];
      for (int i = 0; i < s; i++) {
        Object obj_elem = obj_elems[i];
        if (obj_elem instanceof IPersistentCollection) {
          comp_elems[i] = new ClojureWrapper((IPersistentCollection)obj_elem);
        } else {
          comp_elems[i] = (Comparable) obj_elem;
        }
      }
      return new Tuple(comp_elems);
    } else {
      return new Tuple((Comparable) obj);
    }
  }
  
  public static boolean truthy(Object obj) {
    return ((obj != null) && (!Boolean.FALSE.equals(obj)));
  }
}
