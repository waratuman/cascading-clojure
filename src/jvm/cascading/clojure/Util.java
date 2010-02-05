package cascading.clojure;

import clojure.lang.RT;

public class Util {
  public static Object bootToVar(String ns, String var) {
    String root_path = ns.replace('-', '_').replace('.', '/');
    try {
      System.out.println("LOADING: " + root_path);
      RT.load(root_path);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return RT.var(ns, var).deref();
  }
}