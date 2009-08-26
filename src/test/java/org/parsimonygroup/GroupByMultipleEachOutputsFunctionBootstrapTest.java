package org.parsimonygroup;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import clojure.lang.IFn;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import org.json.JSONObject;
import org.json.JSONException;

import java.util.Iterator;

public class GroupByMultipleEachOutputsFunctionBootstrapTest {

  private class AreJSONValueTuplesSame extends ArgumentMatcher<Tuple> {
    private String key;
    private String json;

    public AreJSONValueTuplesSame(String key, String json) {
      this.key = key;
      this.json = json;
    }
      public boolean matches(Object candidate) {
        Tuple tuple = (Tuple) candidate;

        try {
          Comparable jsonValue = tuple.get(1);
          System.out.println("jsonValue = " + jsonValue);
          System.out.println("****************");
          System.out.println("json = " + json);
          return key.equals(tuple.get(0)) && jsonObjectsEqual(new JSONObject(json), new JSONObject(jsonValue));
        } catch (JSONException e) {
          return false;
        }
      }

    private boolean jsonObjectsEqual(JSONObject champion, JSONObject challenger) throws JSONException {
      Iterator championKeys = champion.keys();
      while(championKeys.hasNext()) {
        String key = (String) championKeys.next();
        if(!champion.get(key).equals(challenger.get(key))) return false;
      }
      return true;
    }
  }

  private GroupByMultipleEachOutputsFunctionBootstrap groupBy;
  private ClojureCascadingHelper clojureHelper;
  private String json;
  private String key1 = "key1";
  private String data1 = "{\"estimated_runway_arrival_time\":\"2009-05-09T06:53:00Z\",\"departure_terminal\":\"\",\"updated_at\":\"2009-05-09T07:00:07Z\",\"pu" +
        "blished_departure_time\":\"2009-05-09T05:50:00Z\",\"arrival_terminal\":\"\",\"number\":5407,\"scheduled_gate_arrival_time\":\"2009-0" +
        "5-09T07:00:00Z\",\"origin_icao_code\":\"KFSD\",\"scheduled_gate_departure_time\":\"2009-05-09T05:50:00Z\",\"creator_code\":\"O\",\"est" +
        "imated_runway_departure_time\":\"2009-05-09T05:56:00Z\",\"scheduled_runway_departure_time\":\"2009-05-09T06:01:00Z\",\"codeshare" +
        "s\":[],\"departure_time\":\"2009-05-09T05:50:00Z\",\"scheduled_block_time\":70,\"scheduled_aircraft_type\":\"BE9\",\"actual_runway_d" +
        "eparture_time\":\"2009-05-09T05:56:43Z\",\"scheduled_runway_arrival_time\":\"2009-05-09T07:05:00Z\",\"destination_icao_code\":\"KP" +
        "IR\",\"arrival_time\":\"2009-05-09T07:00:00Z\",\"published_arrival_time\":\"2009-05-09T07:00:00Z\",\"status_code\":\"A\",\"scheduled_a" +
        "ir_time\":64,\"airline_icao_id\":\"AIP\",\"history_id\":159261998}";
  

  private String key2 = "key2";
  private String data2 = "{\"estimated_runway_arrival_time\":\"2009-05-08T15:30:00Z\",\"dep" +
        "arture_terminal\":\"2\",\"updated_at\":\"2009-05-09T07:00:09Z\",\"published_departure_time\":\"2009-05-08T14:35:00Z\",\"arrival_term" +
        "inal\":\"\",\"number\":322,\"scheduled_gate_arrival_time\":\"2009-05-08T15:30:00Z\",\"origin_icao_code\":\"MMMX\",\"scheduled_gate_dep" +
        "arture_time\":\"2009-05-08T14:35:00Z\",\"creator_code\":\"O\",\"estimated_runway_departure_time\":\"2009-05-08T14:45:00Z\",\"schedul" +
        "ed_runway_departure_time\":\"2009-05-08T14:45:00Z\",\"codeshares\":[{\"number\":\"4022\",\"airline_icao\":null,\"designator\":\"L\"}],\"" +
        "departure_time\":\"2009-05-08T14:35:00Z\",\"scheduled_block_time\":55,\"scheduled_aircraft_type\":\"ATR\",\"scheduled_runway_arriv" +
        "al_time\":\"2009-05-08T15:30:00Z\",\"destination_icao_code\":\"MMJA\",\"arrival_time\":\"2009-05-08T15:30:00Z\",\"published_arrival_" +
        "time\":\"2009-05-08T15:30:00Z\",\"status_code\":\"U\",\"scheduled_air_time\":45,\"airline_icao_id\":\"TAO\",\"history_id\":159225731}";

  @Before
  public void setUp() {
    groupBy = new GroupByMultipleEachOutputsFunctionBootstrap(mock(IFn.class), mock(IFn.class), mock(IFn.class), mock(IFn.class), "dummy");
    clojureHelper = mock(ClojureCascadingHelper.class);
    groupBy.setClojureHelper(clojureHelper);
    json = "[{\"" + key1 + "\":" + data1 + "}," + "{\"" + key2 +"\":" + data2 + "}]";
  }

  @Test
  public void testJSONWriting() throws Exception {
    TupleEntryCollector tupleEntryCollector = mock(TupleEntryCollector.class);
    when(clojureHelper.callClojure((TupleEntry) anyObject(), (IFn) anyObject(), (IFn) anyObject(), (IFn) anyObject(), (IFn) anyObject())).thenReturn(json);

    groupBy.processData(mock(TupleEntry.class), tupleEntryCollector);
    verify(tupleEntryCollector, times(2)).add((Tuple) anyObject());

//    verify(tupleEntryCollector).add(argThat(new AreJSONValueTuplesSame(key2, data2)));
//    verify(tupleEntryCollector).add(argThat(new AreJSONValueTuplesSame(key2, data2)));
  }
}
