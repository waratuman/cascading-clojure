package cascading.clojure;

import clojure.lang.IPersistentCollection;
import clojure.lang.Util;
import org.apache.hadoop.io.Writable;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import clj_serializer.Serializer;

public class ClojureWrapper implements Comparable, Writable {
  private static final Object EOF = new Object();
  private Object obj;

  public ClojureWrapper() {
    this.obj = null;
  }

  public ClojureWrapper(Object obj) {
    this.obj = obj;
  }

  public Object toClojure() {
    return this.obj;
  }

  public int hashCode() {
    return this.obj.hashCode();
  }

  public boolean equals(Object o) {
    return ((ClojureWrapper)o).toClojure().equals(this.toClojure());
  }

  public int compareTo(Object o) {
    return this.hashCode() - ((ClojureWrapper)o).hashCode();
  }

  public void write(DataOutput out) throws IOException {
    Serializer.serialize(out, this.obj);
  }
  
  public void readFields(DataInput in) throws IOException {
    this.obj = Serializer.deserialize(in, EOF);
  }
}
