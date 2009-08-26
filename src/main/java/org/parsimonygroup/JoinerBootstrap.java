package org.parsimonygroup;

import cascading.pipe.cogroup.GroupClosure;
import cascading.pipe.cogroup.Joiner;
import cascading.tuple.Tuple;
import clojure.lang.IFn;

import java.util.Iterator;

public class JoinerBootstrap implements Joiner {

  private IFn writer;
  private IFn joinFn;
  private IFn reader;
  private IFn cljCallBack;
  private int numPipeFields;
  private String fnNsName;
  private ClojureCascadingHelper cljHelper;


  public JoinerBootstrap(IFn reader,IFn writer, IFn joinFn, IFn cljCallBack, String fnNsName, int numPipeFields) {
    this.fnNsName = fnNsName;
    this.writer = writer;
    this.joinFn = joinFn;
    this.reader = reader;
    this.cljCallBack = cljCallBack;
    this.numPipeFields = numPipeFields;
    this.cljHelper = new ClojureCascadingHelper(fnNsName);
  }

  // GroupClosure holds the data that was "joined"
  
  public Iterator<Tuple> getIterator(GroupClosure groupClosure) {

    return new CljJoinIterator(groupClosure, cljHelper, reader, writer, joinFn, cljCallBack, numPipeFields);
  }

  public int numJoins() {
    return -1;   // unlimited number of joins
  }

}
