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
        singletons[i] = Collections.singletonList(Tuple.size(pipeFieldsSum));
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
    Comparable[] rowItems = toRowItems(lastValues);
    try {
      Tuple cljResult = cljHelper.callClojure(rowItems, joinFn, cljCallBack, reader, writer);
      LOG.debug("result added is" + cljResult);
      return cljResult;
    } catch (Exception e) {
      for (Comparable lastValue : rowItems) {
        LOG.fatal(lastValue + "-" + lastValue.toString());
      }
      throw new RuntimeException("clj blew up in join", e);
    }

  }

  private Comparable[] toRowItems(Comparable[] lastValues) {
    List<Comparable> result = new ArrayList<Comparable>();
    for (Comparable val : lastValues) {
      Tuple in = (Tuple) val;
      for(Object rowItem : in) {
        LOG.debug("rowItem is " + rowItem);
        result.add((Comparable) rowItem);  
      }
    }
    
    return result.toArray(new Comparable[result.size()]);
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