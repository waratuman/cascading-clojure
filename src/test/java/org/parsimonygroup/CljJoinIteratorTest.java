package org.parsimonygroup;

import cascading.pipe.cogroup.CoGroupClosure;
import cascading.tuple.SpillableTupleList;
import cascading.tuple.Tuple;
import clojure.lang.IFn;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;

public class CljJoinIteratorTest {
  private CljJoinIterator cljIterator;
  private CoGroupClosure groupClosure;
  private ClojureCascadingHelper cljHelper;
  private IFn reader;
  private Tuple dummyData;

  @Before
  public void setUp() {
    groupClosure = mock(CoGroupClosure.class);
    cljHelper = mock(ClojureCascadingHelper.class);
    reader = mock(IFn.class);
  }

  @Test
  public void withTwoPipes() throws Exception {
    Tuple expected = new Tuple("data1");
    mockIteratorCalls();
    when(cljHelper.callClojure((Comparable[]) anyObject(), (IFn) anyObject(), (IFn) anyObject(), (IFn) anyObject(), (IFn) anyObject())).thenReturn(expected);
    when(groupClosure.getGroup(anyInt())).thenReturn(new SpillableTupleList());
    this.cljIterator = new CljJoinIterator(groupClosure, cljHelper, reader, mock(IFn.class), mock(IFn.class), mock(IFn.class), 2);

    asertTuplesEqual(expected, cljIterator.next());
    asertTuplesEqual(expected, cljIterator.next());
  }

  private void asertTuplesEqual(Tuple expected, Tuple actual) {
    assertEquals(expected.size(), actual.size());
    assertEquals(expected.toString().trim(), actual.toString().trim());
  }

  private void mockIteratorCalls() {
    Iterator iter1 = mock(Iterator.class);
    when(iter1.hasNext()).thenReturn(true);
    dummyData = new Tuple("key", "dummydata");
    when(iter1.next()).thenReturn(dummyData);

    when(groupClosure.size()).thenReturn(2);
    when(groupClosure.getIterator(0)).thenReturn(iter1);
    when(groupClosure.getIterator(1)).thenReturn(iter1);
  }
}
