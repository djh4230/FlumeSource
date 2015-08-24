/*
   Copyright 2013 Vincent.Gu

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package org.apache.flume.serialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/** A class with information about a file being processed. */
public class ResettableFileLineReader {
  private static final Logger logger = LoggerFactory.getLogger(ResettableFileLineReader
      .class);
  private File file;
  private FileChannel ch;
  private ByteBuffer byteBuffer;
  private ByteArrayOutputStream outputStream;
  private boolean skipLF;
  private boolean fileEnded;
  private File statsFile;
  private File finishedStatsFile;
  private FileOutputStream statsFileOut;
  private long markedPosition = 0;
  private long readingPosition = 0;
  private boolean resumable = false;
  private boolean eof = false;
  private boolean finished = false;
  private boolean invalidStatsFile = false;

  /**
   * @param file                    to read
   * @param fileEnded               hinted by caller, if true, then this file
   *                                should be treated as ended, no more reading
   *                                after this batch. if false, always reading
   *                                from this file
   * @param statsFileSuffix
   * @param finishedStatsFileSuffix
   * @throws IOException
   */
  public ResettableFileLineReader(File file,
                                  boolean fileEnded,
                                  String statsFilePrefix,
                                  String statsFileSuffix,
                                  String finishedStatsFileSuffix) throws IOException {
    this.file = file;
    if (file.isDirectory())
      throw new IOException("file '" + file + "' is a directory");
    ch = new FileInputStream(file).getChannel();
    this.skipLF = false;
    this.fileEnded = fileEnded;

    //get file inode
    Path path=Paths.get(file.getPath());
    BasicFileAttributes bfa=java.nio.file.Files.readAttributes(path,BasicFileAttributes.class);
    String fileInfo=bfa.fileKey().toString();
    fileInfo=fileInfo.substring(1,fileInfo.length()-1);
    String inode=fileInfo.split(",")[1].split("=")[1];
    
    /* stats file */
    //statsFile = new File(file.getParent(), statsFilePrefix + file.getName() + statsFileSuffix);
    statsFile = new File(file.getParent(), statsFilePrefix + inode + statsFileSuffix);
    //finishedStatsFile = new File(file.getParent(), statsFilePrefix + file.getName() + finishedStatsFileSuffix);
    finishedStatsFile = new File(file.getParent(), statsFilePrefix + inode + finishedStatsFileSuffix);

    /* get previous line position */
    retrieveStats();
  }

  /** Retrieve previous line position. */
  private void retrieveStats() throws IOException {
    logger.debug("retrieving status for file '{}'", file);
    finished = finishedStatsFile.exists();
    if (finished) {
      logger.debug("found finished stats file: '{}', no more reading needed",
          finishedStatsFile);
      return;
    }
    if (statsFile.exists()) {
      logger.debug("found stats file: '{}'", statsFile);
      BufferedReader reader = new BufferedReader(new FileReader(statsFile));
      List<String> lines = new ArrayList<String>();
      try {
        String line = null;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
        }
      } finally {
        reader.close();
      }
      if (lines.size() == 0 || lines.get(0).length() == 0) {
        logger.error("stats file '{}' empty, will re-read corresponding file",
            statsFile);
        purgeStatFile();
        return;
      }
      try {
        readingPosition = markedPosition = Long.valueOf(lines.get(0));
        ch.position(markedPosition);
      } catch (NumberFormatException e) {
        logger.warn("stats file '{}' format error, reset stat file",
            file.getAbsolutePath());
        purgeStatFile();
      }
    }
    logger.debug("opened stats file '{}', got line number '{}'",
        statsFile, markedPosition);
  }

  private void ensureOpen() throws IOException {
    if (!ch.isOpen()) {
      throw new IOException("the channel of file '" + file + "' is closed");
    }
  }

  public byte[] readLine() throws IOException {
    /* this file was already marked as finished, EOF now */
    if (finished || invalidStatsFile || eof) return null;
    ensureOpen();

    if (null == outputStream) {
      outputStream = new ByteArrayOutputStream(1024); // 1KB
    } else {
      outputStream.reset();
    }
    if (null == byteBuffer) {
      byteBuffer = ByteBuffer.allocate(128 * 1024); // 128KB
    }
    if (!resumable) {
      byteBuffer.limit(byteBuffer.position()); // zero buffer
      resumable = true;
    }

    while (true) {
      while (byteBuffer.hasRemaining()) {
        byte b = byteBuffer.get();
        readingPosition++;
        if (skipLF) {
          skipLF = false;
          if (b != '\n') {
            byteBuffer.position(byteBuffer.position() - 1);
            readingPosition--;
          }
          return outputStream.toByteArray();
        }
        if (b == '\n') {
          return outputStream.toByteArray();
        }
        if (b == '\r') {
          skipLF = true;
          continue;
        }
        outputStream.write(b);
      }
      byteBuffer.clear(); // re-init buffer
      if (ch.read(byteBuffer) == -1) {
        eof = true;
        if (outputStream.size() > 0)
          return outputStream.toByteArray();
        else
          return null;
      }
      byteBuffer.flip();
    }
  }

  public void close() throws IOException {
    if (null != ch)
      ch.close();
    if (null != statsFileOut)
      statsFileOut.close();
    logger.debug("close file: '{}'", file);
  }

  private void purgeStatFile() throws IOException {
    if (null != statsFileOut) {
      statsFileOut.close();
      statsFileOut = null;
    }
    if (null != statsFile) {
      statsFile.delete();
    }
  }

  /** Record the position of current reading file into stats file. */
  public void commit() throws IOException {
    if (finished) {
      logger.warn("commit while file is marked as finished: {}", file);
      return;
    }

    if (invalidStatsFile) {
      logger.warn("commit while file's stat file is unavailable: {}", file);
      return;
    }

    /* open stat file for write */
    try {
      if (null == statsFileOut) {
        statsFileOut = new FileOutputStream(statsFile, false);
        invalidStatsFile = false;
      }
    } catch (IOException ioe) {
      invalidStatsFile = true;
      throw new IOException("cannot create stats file for log file '" + file +
          "', this class needs stats file to function normally", ioe);
    }
    statsFileOut.getChannel().position(0);
    statsFileOut.write(String.valueOf(readingPosition).getBytes());
    statsFileOut.flush();
    markedPosition = readingPosition;
    logger.trace("committed '{}', position: {}", statsFile, markedPosition);
    if (fileEnded && eof) {
      logger.debug("sealing stats file, renaming from '{}' to '{}'",
          statsFile, finishedStatsFile);
      statsFileOut.close();
      statsFile.renameTo(finishedStatsFile);
      logger.debug("file '{}' marked as done", file);
      finished = true;
    }
  }

  /** Rewind reading position to previous recorded position. */
  public void reset() throws IOException {
    if (finished || invalidStatsFile) return;

    resumable = false;
    readingPosition = markedPosition;
    ch.position(markedPosition);
    logger.info("file '{}': reverted to previous marked position: [{}]",
        file, String.valueOf(markedPosition));
  }

  public File getFile() {
    return file;
  }
}

