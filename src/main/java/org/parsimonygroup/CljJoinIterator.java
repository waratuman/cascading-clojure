package org.parsimonygroup;

import cascading.pipe.cogroup.CoGroupClosure;
import cascading.pipe.cogroup.GroupClosure;
import cascading.tuple.Tuple;
import clojure.lang.IFn;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.*;

public class CljJoinIterator implements Iterator<Tuple>, Serializable {
  private GroupClosure closure;
  private ClojureCascadingHelper cljHelper;
  private IFn reader;
  private IFn writer;
  private IFn joinFn;
  private IFn cljCallBack;
  private int pipeFieldsSum;
  private Iterator[] iterators;
  private Comparable[] lastValues;
  private List[] singletons;
  private static final Logger LOG = Logger.getLogger(CljJoinIterator.class);

  public CljJoinIterator(GroupClosure groupClosure, ClojureCascadingHelper cljHelper, IFn reader, IFn writer, IFn joinFn, IFn cljCallBack, int pipeFieldsSum) {
    this.closure = groupClosure;
    this.cljHelper = cljHelper;
    this.reader = reader;
    this.writer = writer;
    this.joinFn = joinFn;
    this.cljCallBack = cljCallBack;
    this.pipeFieldsSum = pipeFieldsSum;
    init();
  }

  private void init() {
    singletons = new List[closure.size()];

    for (int i = 0; i < singletons.length; i++) {
      if (isOuter(i))
        singletons[i] = Collections.singletonList(Tuple.size(4 /*value fields*/));
    }

    iterators = new Iterator[closure.size()];
    for (int i = 0; i < closure.size(); i++) iterators[i] = getIterator(i);
  }

  private Comparable[] initLastValues() {
    lastValues = new Comparable[iterators.length];

    for (int i = 0; i < iterators.length; i++)
      lastValues[i] = (Comparable) iterators[i].next();

    return lastValues;
  }

  public final boolean hasNext() {
    // if this is the first pass, and there is an iterator without a next value,
    // then we have no next element
    if (lastValues == null) {
      for (Iterator iterator : iterators) {
        if (!iterator.hasNext())
          return false;
      }
      return true;
    }
    for (Iterator iterator : iterators) {
      if (iterator.hasNext())
        return true;
    }
    return false;
  }

  public Tuple next() {
    if (lastValues == null)
      return makeResult(initLastValues());

    for (int i = iterators.length - 1; i >= 0; i--) {
      if (iterators[i].hasNext()) {
        lastValues[i] = (Comparable) iterators[i].next();
        break;
      }

      // reset to first
      iterators[i] = getIterator(i);
      lastValues[i] = (Comparable) iterators[i].next();
    }

    return makeResult(lastValues);
  }

  private Tuple makeResult(Comparable[] lastValues) {
    Tuple result = new Tuple();
    lastValues = dumpKeysKeepData(lastValues);
    try {
      result.add((Comparable) cljHelper.callClojure(lastValues, joinFn, cljCallBack, reader, writer));
    } catch (Exception e) {
      LOG.fatal("calling clojure blew up in join");
      LOG.fatal(e.toString(), e);
      for (Comparable lastValue : lastValues) {
        LOG.fatal(lastValue + "-" + lastValue.toString());
      }
      return null;
    }

    // add enough fields to satisfy cogroup
//    addDummyFields(pipeFieldsSum -1, result);

    if (LOG.isTraceEnabled())
      LOG.trace("tuple: " + result.print());

    return result;
  }

  private String[] dumpKeysKeepData(Comparable[] lastValues) {
    Collection<String> values = new Transform<Comparable, String>() {
      @Override
      public String closure(Comparable in) {
        Tuple tuple = (Tuple) in;
        String result = tuple.getString(tuple.size() - 1);
        return (result != null && result.trim().length() > 0) ? result : null;
      }
    }.apply(Arrays.asList(lastValues));
    return values.toArray(new String[values.size()]);
  }

  private void addDummyFields(int numDummys, Tuple result) {
    for (int i = 0; i < numDummys; i++)
      result.add("");
  }

  protected boolean isOuter(int i) {
    return i == closure.size() - 1 && (getCoGroupClosure().getGroup(i).size() == 0);
//    return i == closure.size() - 1 && super.isOuter( i );
  }

  protected final CoGroupClosure getCoGroupClosure() {
    return (CoGroupClosure) closure;
  }

  protected Iterator getIterator(int i) {
    if (singletons[i] == null) // let init() decide
      return closure.getIterator(i);

    return singletons[i].iterator();
  }


  public void remove() {
  }
}