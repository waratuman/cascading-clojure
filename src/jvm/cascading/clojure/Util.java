package cascading.clojure;

import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.IteratorSeq;
import cascading.tuple.Tuple;
import java.util.Collection;

public class Util {
  public static Object bootToVar(String ns, String var) {
    String root_path = ns.replace('-', '_').replace('.', '/');
    try {
      RT.load(root_path);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return RT.var(ns, var).deref();
  }
  
  public static Tuple coerceTuple(Collection clj_tuple) {
    Object[] raw_arr = clj_tuple.toArray();
    Comparable[] arr = new Comparable[raw_arr.length];
    System.arraycopy(raw_arr, 0, arr, 0, raw_arr.length);
    return new Tuple(arr);
  }
  
  public static ISeq coerceSeq(Tuple cas_tuple) {
    return IteratorSeq.create(cas_tuple.iterator());
  }
  
  public static boolean truthy(Object obj) {
    return ((obj != null) && (!Boolean.FALSE.equals(obj)));
  }
}