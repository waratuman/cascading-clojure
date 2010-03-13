package cascading.clojure;

import java.util.Map;
import java.util.Iterator;
import java.util.Collection;
import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;

public class Util {
    
    /**
     * Converts a Map object to a TupleEntry object. The keys of the 
     * map will be converted to the fields of the TupleEntry object;
     * the values of the map  will be set to the values of the
     * respective fields in the TupleEntry object.
     */
    public static TupleEntry mapToTupleEntry(Map map) {
        Fields fields = new Fields();
        Tuple tuple = new Tuple();
        for (Object key : map.keySet()) {
            fields = fields.append(new Fields((Comparable)key));
            tuple.add((Comparable)map.get(key));
        }
        return new TupleEntry(fields, tuple);
    }
    
    /**
     * Converts a TupleEntry object to a IPersistentMap. The fields
     * of the TupleEntry object are set to the keys of the map and
     * the values of the keys are set to the value of the respective
     * fields in the TupleEntry object.
     */
    public static IPersistentMap tupleEntryToMap(TupleEntry entry) {
        IPersistentMap map = PersistentHashMap.EMPTY;
        Iterator iter = entry.getFields().iterator();
        while (iter.hasNext()) {
            Comparable key = (Comparable) iter.next();
            map = map.assoc(key, entry.get(key));
        }
        return map;
    }

    /**
     * Converts a collection of maps to an array of TupleEntries.
     */
    public static TupleEntry[] collectionToTupleEntries(Collection coll) {
        TupleEntry[] entries = new TupleEntry[coll.size()];
        int i = 0;
        Iterator iter = coll.iterator();
        while (iter.hasNext()) {
            entries[i] = mapToTupleEntry((Map)iter.next());
            i++;
        }
        return entries;
    }
    
    /**
     * Converts a tuple to a sequence (ISeq) of values.
     */
    public static ISeq tupleToSeq(Tuple tuple) {
        Comparable[] objects = new Comparable[tuple.size()];
        for (int i = 0; i < tuple.size(); i++) {
            objects[i] = tuple.get(i);
        }
        return RT.seq(objects);
    }
    
    /**
     * Converts a collection of values to a Tuple.
     */
    public static Tuple collectionToTuple(Collection coll) {
        Tuple tuple = new Tuple();
        for (Object obj : coll) { tuple.add((Comparable)obj); }
        return tuple;
    }
    
}

