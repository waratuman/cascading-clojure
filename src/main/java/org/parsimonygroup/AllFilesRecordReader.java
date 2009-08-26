package org.parsimonygroup;

import java.io.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.StringUtils;

public class AllFilesRecordReader implements RecordReader<LongWritable, Text> {

  private MultiFileSplit multiFileSplit;
  private JobConf job;
  private long pos;
  private long end;
  private FileSystem fs;
  private BufferedReader currentReader;
  private AtomicBoolean hasBeenRead;
  private CompressionCodecFactory compressionCodecs = null;

  public AllFilesRecordReader(JobConf job, MultiFileSplit in) throws IOException {
    this.multiFileSplit = in;
    this.job = job;
    this.pos = 0;
    this.end = in.getLength();
    fs = FileSystem.get(job);
    hasBeenRead = new AtomicBoolean(false);
  }

  public AllFilesRecordReader() {
  }

  public void close() throws IOException {
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public Text createValue() {
    return new Text();
  }

  public long getPos() throws IOException {
    return pos;
  }

  public float getProgress() {
    if (pos == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, pos / (float) (end));
    }
  }

  public boolean next(LongWritable key, Text value) throws IOException {
    value.clear();
    if (hasBeenRead.get()) return false;

    Path[] paths = multiFileSplit.getPaths();

    StringBuilder acc = new StringBuilder();
    acc.append("["); 
    for (int i = 0; i < paths.length; i++) {
      compressionCodecs = new CompressionCodecFactory(job);
      final CompressionCodec codec = compressionCodecs.getCodec(paths[i]);
      FSDataInputStream fileIn = fs.open(paths[i]);
      if (codec != null) {
        currentReader = new BufferedReader(new InputStreamReader(codec.createInputStream(fileIn)));
      } else {
        currentReader = new BufferedReader(new InputStreamReader(fs.open(paths[i])));
      }

      acc.append(readFile(currentReader)).append(" ");
    }
    acc.append("]");
    value.set(acc.toString());

    hasBeenRead.set(true);
    this.pos = value.getLength();
    return true;
  }

  public String readFile(BufferedReader currentReader) throws IOException {
    StringWriter stringWriter = new StringWriter();
    BufferedWriter writer = new BufferedWriter(stringWriter);


    String line;
    do {
      line = currentReader.readLine();
      if (line == null) {
        writer.write(" ");
        currentReader.close();
      } else {
        writer.write(line);
      }
    } while (line != null);
    writer.flush();
    return stringWriter.getBuffer().toString();
  }
//	/**
//	   * RecordReader is responsible from extracting records from the InputSplit.
//	   * This record reader accepts a {@link MultiFileSplit}, which encapsulates several
//	   * files, and no file is divided.
//	   */
//	  public static class MultiFileLineRecordReader
//	    implements RecordReader<WordOffset, Text> {
//
//	    private MultiFileSplit split;
//	    private long offset; //total offset read so far;
//	    private long totLength;
//	    private FileSystem fs;
//	    private int count = 0;
//	    private Path[] paths;
//
//	    private FSDataInputStream currentStream;
//	    private BufferedReader currentReader;
//
//	    public MultiFileLineRecordReader(Configuration conf, MultiFileSplit split)
//	      throws IOException {
//
//	      this.split = split;
//	      fs = FileSystem.get(conf);
//	      this.paths = split.getPaths();
//	      this.totLength = split.getLength();
//	      this.offset = 0;
//
//	      //open the first file
//	      Path file = paths[count];
//	      currentStream = fs.open(file);
//	      currentReader = new BufferedReader(new InputStreamReader(currentStream));
//	    }
//
//	    public void close() throws IOException { }
//
//	    public long getPos() throws IOException {
//	      long currentOffset = currentStream == null ? 0 : currentStream.getPos();
//	      return offset + currentOffset;
//	    }
//
//	    public float getProgress() throws IOException {
//	      return ((float)getPos()) / totLength;
//	    }
//
//	    public boolean next(WordOffset key, Text value) throws IOException {
//	      if(count >= split.getNumPaths())
//	        return false;
//
//	      /* Read from file, fill in key and value, if we reach the end of file,
//	       * then open the next file and continue from there until all files are
//	       * consumed.
//	       */
//	      String line;
//	      do {
//	        line = currentReader.readLine();
//	        if(line == null) {
//	          //close the file
//	          currentReader.close();
//	          offset += split.getLength(count);
//
//	          if(++count >= split.getNumPaths()) //if we are done
//	            return false;
//
//	          //open a new file
//	          Path file = paths[count];
//	          currentStream = fs.open(file);
//	          currentReader=new BufferedReader(new InputStreamReader(currentStream));
//	          key.fileName = file.getName();
//	        }
//	      } while(line == null);
//	      //update the key and value
//	      key.offset = currentStream.getPos();
//	      value.set(line);
//
//	      return true;
//	    }
//
//	    public WordOffset createKey() {
//	      WordOffset wo = new WordOffset();
//	      wo.fileName = paths[0].toString(); //set as the first file
//	      return wo;
//	    }
//
//	    public Text createValue() {
//	      return new Text();
//	    }
//	  }


}
