package examples;

import cascading.pipe.*;
import cascading.tuple.Fields;
import cascading.operation.xml.TagSoupParser;
import cascading.operation.xml.XPathGenerator;
import cascading.operation.xml.XPathOperation;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.aggregator.Count;

public class WordCountSplitAssembly  extends SubAssembly {
    public WordCountSplitAssembly(String sourceName, String sinkUrlName, String sinkWordName) {
      // create a new pipe assembly to create the word count across all the pages, and the word count in a single page
      Pipe pipe = new Pipe(sourceName);

      // convert the html to xhtml using the TagSouParser. return only the fields "url" and "xml", discard the rest
      pipe = new Each(pipe, new Fields("page"), new TagSoupParser(new Fields("xml")), new Fields("url", "xml"));
      // apply the given XPath expression to the xml in the "xml" field. this expression extracts the 'body' element.
      XPathGenerator bodyExtractor = new XPathGenerator(new Fields("body"), XPathOperation.NAMESPACE_XHTML, "//xhtml:body");
      pipe = new Each(pipe, new Fields("xml"), bodyExtractor, new Fields("url", "body"));
      // apply another XPath expression. this expression removes all elements from the xml, leaving only text nodes.
      // text nodes in a 'script' element are removed.
      String elementXPath = "//text()[ name(parent::node()) != 'script']";
      XPathGenerator elementRemover = new XPathGenerator(new Fields("words"), XPathOperation.NAMESPACE_XHTML, elementXPath);
      pipe = new Each(pipe, new Fields("body"), elementRemover, new Fields("url", "words"));
      // apply the regex to break the document into individual words and stuff each word at a new tuple into the current
      // stream with field names "url" and "word"
      RegexGenerator wordGenerator = new RegexGenerator(new Fields("word"), "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)");
      pipe = new Each(pipe, new Fields("words"), wordGenerator, new Fields("url", "word"));

      // group on "url"
      Pipe urlCountPipe = new GroupBy(sinkUrlName, pipe, new Fields("url", "word"));
      urlCountPipe = new Every(urlCountPipe, new Fields("url", "word"), new Count(), new Fields("url", "word", "count"));

      // group on "word"
      Pipe wordCountPipe = new GroupBy(sinkWordName, pipe, new Fields("word"));
      wordCountPipe = new Every(wordCountPipe, new Fields("word"), new Count(), new Fields("word", "count"));

      setTails(urlCountPipe, wordCountPipe);
  }
}