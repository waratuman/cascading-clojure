package cascading.clojure.scheme;

import cascading.clojure.Util;
import clj_json.JsonExt;
import java.io.StringWriter;
import java.io.StringReader;
import clojure.lang.ITransientMap;
import clojure.lang.IPersistentMap;
import cascading.clojure.Util;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.scheme.Scheme;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import java.io.IOException;

public class JSONMapLineFile extends Scheme {
    
    public static final JsonFactory jsonFactory = new JsonFactory();
    
    public JSONMapLineFile(Fields sourceFields) {
        super(sourceFields);
    }

    public JSONMapLineFile(Fields sourceFields, Fields sinkFields) {
        super(sourceFields, sinkFields);
    }
    
    public void sourceInit(Tap tap, JobConf conf) {
        conf.setInputFormat(TextInputFormat.class);
    }

    public void sinkInit(Tap tap, JobConf conf) {
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setOutputFormat(TextOutputFormat.class);
    }

    public Tuple source(Object key, Object value) {
        try {
            JsonParser jp = jsonFactory.createJsonParser((new StringReader(value.toString())));
            // IPersistentMap map = ((IPersistentMap)JsonExt.parse(jp, true, new Object())).persistent();
            IPersistentMap map = (IPersistentMap)JsonExt.parse(jp, true, new Object());
            
            Fields fields = this.getSourceFields();
            TupleEntry tupleEntry = Util.mapToTupleEntry((java.util.Map)map);
            Tuple tuple = new Tuple();
            for (int i = 0; i < fields.size(); i++) {
                tuple.add(tupleEntry.get(fields.get(i)));
            }
            return tuple;
        }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector)  throws IOException {
        try {
            StringWriter stringWriter = new StringWriter();
            JsonGenerator generator = jsonFactory.createJsonGenerator(stringWriter);
            JsonExt.generate(generator, Util.tupleEntryToMap(tupleEntry));
            generator.flush();
            outputCollector.collect(null, stringWriter.toString());
        }
        catch (IOException e) { throw e; }
        catch (Exception e) { throw new RuntimeException(e); }
    }
    
}
