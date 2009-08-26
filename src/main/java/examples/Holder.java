/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package examples;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.pipe.*;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class Holder
  {

    public void main( String[] args )
    {
    // set the current job jar
    Properties properties = new Properties();
    FlowConnector.setApplicationJarClass( properties, Holder.class );
    FlowConnector flowConnector = new FlowConnector( properties );

    String inputPath = args[ 0 ];
    String pagesPath = args[ 1 ] + "/pages/";
    String urlsPath = args[ 1 ] + "/urls/";
    String wordsPath = args[ 1 ] + "/words/";
    String localUrlsPath = args[ 2 ] + "/urls/";
    String localWordsPath = args[ 2 ] + "/words/";

    // import a text file with crawled pages from the local filesystem into a Hadoop distributed filesystem
    // the imported file will be a native Hadoop sequence file with the fields "page" and "url"
    // note this examples stores crawl pages as a tabbed file, with the first field being the "url"
    // and the second being the "raw" document that had all new line chars ("\n") converted to the text ":nl:".

    // a predefined pipe assembly that returns fields named "url" and "page"
    Pipe importPipe = new ImportCrawlDataAssembly( "import pipe" );

    // create the tap instances
    Tap localPagesSource = new Lfs( new TextLine(), inputPath );
    Tap importedPages = new Lfs( new SequenceFile( new Fields( "url", "page" ) ), pagesPath );

    // connect the pipe assembly to the tap instances
    Flow importPagesFlow = flowConnector.connect( "import pages", localPagesSource, importedPages, importPipe );

    // a predefined pipe assembly that splits the stream into two named "url pipe" and "word pipe"
    // these pipes could be retrieved via the getTails() method and added to new pipe instances
    SubAssembly wordCountPipe = new WordCountSplitAssembly( "wordcount pipe", "url pipe", "word pipe" );

    // create Hadoop sequence files to store the results of the counts
    Tap sinkUrl = new Lfs( new SequenceFile( new Fields( "url", "word", "count" ) ), urlsPath );
    Tap sinkWord = new Lfs( new SequenceFile( new Fields( "word", "count" ) ), wordsPath );
                                                                                         
    // convenience method to bind multiple pipes and taps
    Map<String, Tap> sinks = Cascades.tapsMap( new String[]{"url pipe", "word pipe"}, Tap.taps( sinkUrl, sinkWord ) );

    // wordCountPipe will be recognized as an assembly and handled appropriately
    Flow count = flowConnector.connect( importedPages, sinks, wordCountPipe );

    // create an assembly to export the Hadoop sequence file to local text files
    Pipe exportPipe = new Each( "export pipe", new Identity() );

    Tap localSinkUrl = new Lfs( new TextLine(), localUrlsPath );
    Tap localSinkWord = new Lfs( new TextLine(), localWordsPath );

    // connect up both sinks using the same exportPipe assembly
    Flow exportFromUrl = flowConnector.connect( "export url", sinkUrl, localSinkUrl, exportPipe );
    Flow exportFromWord = flowConnector.connect( "export word", sinkWord, localSinkWord, exportPipe );

    // connect up all the flows, order is not significant
    Cascade cascade = new CascadeConnector().connect( importPagesFlow, count, exportFromUrl, exportFromWord );

    // run the cascade to completion
    cascade.complete();
    } 
  }
