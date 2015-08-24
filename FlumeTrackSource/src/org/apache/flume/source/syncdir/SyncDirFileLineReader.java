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

package org.apache.flume.source.syncdir;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.flume.FlumeException;
import org.apache.flume.serialization.ResettableFileLineReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link org.apache.flume.client.avro.LineReader} which reads log data from
 * files stored in a
 * syncDirectory or subdirectories and put an empty mark file for each file
 * when
 * sync were done(through {@link #readLine()} calls). The user must {@link
 * #commit()} each read, to indicate that the lines have been fully processed.
 */
public class SyncDirFileLineReader {
  private static final Logger logger = LoggerFactory.getLogger(
      SyncDirFileLineReader.class);
  private File directory;
  private String endFileSuffix;
  private String statsFilePrefix;
  private String statsFileSuffix;
  private String finishedStatsFileSuffix;
  private Iterator<File> filesIterator;
  private Optional<ResettableFileLineReader> currentFile = Optional.absent();
  /** Always contains the last file from which lines have been read. * */
  private Optional<ResettableFileLineReader> lastFileRead = Optional.absent();
  private boolean committed = true;
  /** A flag to signal an un-recoverable error has occurred. */
  private boolean disabled = false;

  /**
   * Create a SyncDirFileLineReader to watch the given syncDirectory.
   *
   * @param directory               The syncDirectory to watch
   * @param endFileSuffix           The suffix to append to completed files
   * @param statsFileSuffix
   * @param finishedStatsFileSuffix
   */
  public SyncDirFileLineReader(File directory,
                               final String endFileSuffix,
                               final String statsFilePrefix,
                               final String statsFileSuffix,
                               final String finishedStatsFileSuffix) {
    // Verify syncDirectory exists and is readable/writable
    Preconditions.checkNotNull(directory);
    Preconditions.checkState(directory.exists(),
        "Directory does not exist: " + directory.getAbsolutePath());
    Preconditions.checkState(directory.isDirectory(),
        "Path is not a directory: " + directory.getAbsolutePath());
    this.directory = directory;

    // Do a canary test to make sure we have access to test syncDirectory
    try {
      File tmpFile = File.createTempFile("flume", "test");
      Files.write("testing flume file permissions\n".getBytes(), tmpFile);
      Files.readLines(tmpFile, Charset.defaultCharset());
      tmpFile.delete();
    } catch (IOException e) {
      throw new FlumeException("Unable to read and modify files" +
          " in the sync syncDirectory: " + directory, e);
    }
    this.endFileSuffix = endFileSuffix;
    this.statsFilePrefix = statsFilePrefix;
    this.statsFileSuffix = statsFileSuffix;
    this.finishedStatsFileSuffix = finishedStatsFileSuffix;
  }

  /**
   * Return the relative file filePath which generated the data from the last
   * successful {@link #readLine} or {@link #readLines(int)} call. Returns null
   * if called before any file contents are read.
   */
  public File getLastFileRead() {
    if (!lastFileRead.isPresent()) {
      return null;
    }
    return lastFileRead.get().getFile();
  }

  /** Commit the last lines which were read. */
  public void commit() throws IOException {
    if (disabled) {
      throw new IllegalStateException("Reader is disabled.");
    }
    currentFile.get().commit();
    committed = true;
  }

  public byte[] readLine() {
    if (disabled) {
      throw new IllegalStateException("Reader is disabled.");
    }
    List<byte[]> lines = readLines(1);
    if (lines.size() == 0) {
      return null;
    }
    return lines.get(0);
  }

  public List<byte[]> readLines(int n) {
    //if (disabled) {
    //  throw new IllegalStateException("Reader has been disabled.");
    //}
    boolean prevDisabled = disabled;
    disabled = ! directory.exists();
    if (prevDisabled && !disabled) {
      logger.info("directory {} found, resuming..", directory);
    } else if (!prevDisabled && disabled) {
      throw new IllegalStateException("missing directory " + directory + ", reader disabled.");
    } else if (disabled) {
      throw new IllegalStateException("Reader is disabled.");
    }

    try {
      if (!committed) {
        if (!currentFile.isPresent()) {
          throw new IllegalStateException("File should not roll when " +
              "commit is outstanding.");
        }
        logger.info("Last reads were not committed");
        currentFile.get().reset();
        committed = true;
      }

      // Check if new files have arrived since last call
      if (!currentFile.isPresent()) {
        currentFile = getNextFile();
      }
      // Return empty list if no new files
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }

    /* It's possible that the last read took us just up to a file boundary.
     * If so, try to roll to the next file, if there is one. */
      byte[] outLine;
      while ((outLine = currentFile.get().readLine()) == null) {
        retireCurrentFile();
        currentFile = getNextFile();
        if (!currentFile.isPresent()) {
          return Collections.emptyList();
        }
      }
      List<byte[]> out = Lists.newArrayList();
      while (outLine != null) {
        out.add(outLine);
        if (out.size() == n) {
          break;
        }
        outLine = currentFile.get().readLine();
      }

      committed = false;
      lastFileRead = currentFile;
      return out;
    } catch (IOException ioe) {
      // something wrong with the file(s), skip over to next
      currentFile = Optional.absent();
      return Collections.emptyList();
    }
  }

  /**
   * If these operations fail in a way that may cause duplicate log entries, an
   * error is logged but no exceptions are thrown. If these operations fail in
   * a
   * way that indicates potential misuse of the spooling syncDirectory, a
   * FlumeException will be thrown.
   *
   * @throws FlumeException if files do not conform to spooling assumptions
   */
  private void retireCurrentFile() throws IOException {
    Preconditions.checkState(currentFile.isPresent());

    logger.debug("retire file: '{}'", currentFile.get().getFile());
    currentFile.get().close();
  }

  public void close() {
    try {
    if (currentFile.isPresent())
      currentFile.get().close();
    } catch (IOException ioe) {
      currentFile = Optional.absent();
    }
  }

  /**
   * Find the next file in the directory by walking through directory tree.
   *
   * @return the next file
   */
  private Optional<ResettableFileLineReader> getNextFile() {
    if (null != filesIterator && !filesIterator.hasNext()) {
      filesIterator = null;
      return Optional.absent();
    }
    if (null == filesIterator) {
      filesIterator = FileUtils.iterateFiles(directory, new IOFileFilter() {
        @Override
        public boolean accept(File file) {
          if (!file.exists()) return false; // skip non-exist files in iterator
          if (file.isFile()) {
            String fileStr = file.getName();
            if (!(fileStr.endsWith(endFileSuffix) ||
                fileStr.endsWith(statsFileSuffix) ||
                fileStr.endsWith(finishedStatsFileSuffix))) {
              File finishedMarkFile = new File(file.getPath() + finishedStatsFileSuffix);
              if (!finishedMarkFile.exists())
                return true;
            }
          }
          return false;
        }

        @Override
        public boolean accept(File dir, String name) {
          return false;
        }
      }, TrueFileFilter.INSTANCE);
    }

    File nextFile;
    boolean fileEnded;
    if (!filesIterator.hasNext())
      return Optional.absent();
    /* checking file's reading progress, skip if needed */
    nextFile = filesIterator.next();
    logger.debug("treating next file: {}", nextFile);
    fileEnded = new File(nextFile.getPath() + endFileSuffix).exists();
    if (fileEnded) {
      logger.debug("file {} marked as ended", nextFile);
    }
    try {
      ResettableFileLineReader file = new ResettableFileLineReader(nextFile, fileEnded,
          statsFilePrefix, statsFileSuffix, finishedStatsFileSuffix);
      return Optional.of(file);
    } catch (IOException e) {
      logger.error("Exception opening file: " + nextFile, e);
      return Optional.absent();
    }
  }
}
