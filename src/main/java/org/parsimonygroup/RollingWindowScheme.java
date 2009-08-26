package org.parsimonygroup;

import cascading.scheme.Scheme;
import cascading.scheme.TextLine;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

public class RollingWindowScheme extends TextLine {
  @Override
  public void sourceInit(Tap tap, JobConf jobConf) {
     jobConf.setInputFormat(RollingWindowInputFormat.class);
  }


}
