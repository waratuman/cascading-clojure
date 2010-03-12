package cascading.clojure;

import java.util.Iterator;
import java.util.Collection;

import cascading.tuple.Tuple;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import clojure.lang.RT;
import clojure.lang.IPersistentMap;
import clojure.lang.PersistentHashMap;

public class Util {
    
    public static TupleEntry mapToTupleEntry(IPersistentMap map) {
        Object[] keys = RT.seqToArray(RT.keys(map));
        Fields fields = new Fields();
        Tuple tuple = new Tuple();
        for (Object key : keys) {
            fields = fields.append(new Fields((Comparable)key));
            tuple.add((Comparable)RT.get(map,key));
        }
        return new TupleEntry(fields, tuple);
    }

    public static IPersistentMap tupleEntryToMap(TupleEntry entry) {
        IPersistentMap map = PersistentHashMap.EMPTY;
        Iterator iter = entry.getFields().iterator();
        while (iter.hasNext()) {
            Comparable key = (Comparable) iter.next();
            map = map.assoc(key, entry.get(key));
        }
        return map;
    }

    public static TupleEntry[] collectionToTupleEntries(Collection coll) {
        TupleEntry[] entries = new TupleEntry[coll.size()];
        int i = 0;
        Iterator iter = coll.iterator();
        while (iter.hasNext()) {
            entries[i] = mapToTupleEntry((IPersistentMap)iter.next());
            i++;
        }
        return entries;
    }
    
}
