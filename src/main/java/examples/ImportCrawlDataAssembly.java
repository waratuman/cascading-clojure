package examples;

import cascading.pipe.SubAssembly;
import cascading.pipe.Pipe;
import cascading.pipe.Each;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexReplace;
import cascading.tuple.Fields;

public class ImportCrawlDataAssembly extends SubAssembly {
    public ImportCrawlDataAssembly(String name) {
      // split the text line into "url" and "raw" with the default delimiter of tab
      RegexSplitter regexSplitter = new RegexSplitter(new Fields("url", "raw"));
      Pipe importPipe = new Each(name, new Fields("line"), regexSplitter);
      // remove all pdf documents from the stream
      importPipe = new Each(importPipe, new Fields("url"), new RegexFilter(".*\\.pdf$", true));
      // replace ":nl" with a new line, return the fields "url" and "page" to the stream.
      // discared the other fields in the stream
      RegexReplace regexReplace = new RegexReplace(new Fields("page"), ":nl:", "\n");
      importPipe = new Each(importPipe, new Fields("raw"), regexReplace, new Fields("url", "page"));

      setTails(importPipe);
    }
}