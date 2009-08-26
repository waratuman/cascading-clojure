package org.parsimonygroup;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;

public class AllFilesRecordReaderTest {
  private AllFilesRecordReader allFilesReader;
  private BufferedReader reader;

  @Before
  public void setUp() {
    allFilesReader = new AllFilesRecordReader();
    reader = new BufferedReader(new StringReader("12"));

  }

  @Test
  public void testReadFile() throws IOException {
    assertEquals("12 ", allFilesReader.readFile(reader));
  }

}
