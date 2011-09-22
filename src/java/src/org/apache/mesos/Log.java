package org.apache.mesos;

import java.io.Closeable;
import java.io.IOException;

import java.util.List;
import java.util.Set;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

public class Log {

  static class Position implements Comparable<Position> {
    @Override
    public int compareTo(Position that) {
      return Long.signum(value - that.value);
    }

    @Override
    public boolean equals(Object that) {
      return that instanceof Position && value == ((Position) that).value;
    }

    @Override
    public String toString() {
      return "Position " + value;
    }

    /**
     * Returns an "identity" off this position, useful for serializing
     * to logs or across communication mediums.
     */
    public byte[] identity() {
      byte[] bytes = new byte[8];
      bytes[0] = (byte) (0xff & (value >> 56));
      bytes[1] = (byte) (0xff & (value >> 48));
      bytes[2] = (byte) (0xff & (value >> 40));
      bytes[3] = (byte) (0xff & (value >> 32));
      bytes[4] = (byte) (0xff & (value >> 24));
      bytes[5] = (byte) (0xff & (value >> 16));
      bytes[6] = (byte) (0xff & (value >> 8));
      bytes[7] = (byte) (0xff & value);
      return bytes;
    }

    /* A Position is (and should only) get invoked by the underlying JNI. */
    private Position(long value) {
      this.value = value;
    }

    private final long value;
  }

  static class Entry {
    public final Position position;
    public final byte[] data;

    /* An Entry is (and should only) get invoked by the underlying JNI. */
    private Entry(Position position, byte[] data) {
      this.position = position;
      this.data = data;
    }
  }

  /**
   * An exception that gets thrown when an error occurs while
   * performing a read or write operation.
   */
  static class OperationFailedException extends Exception {
    public OperationFailedException(String message) {
      super(message);
    }

    public OperationFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * An exception that gets thrown when a writer no longer has the
   * ability to perform operations (e.g., because it was superseded by
   * another writer).
   */
  static class WriterFailedException extends Exception {
    public WriterFailedException(String message) {
      super(message);
    }

    public WriterFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  static class Reader {
    Reader(Log log) {
      this.log = log;
      initialize(log);
    }

    /**
     * Attepts to read from the log between the specified positions
     * (inclusive). If either of the positions are invalid, an
     * OperationFailedException will get thrown. Unfortunately, this
     * will also get thrown in other circumstances (e.g., disk
     * failure) and therefore it is currently impossible to tell these
     * two cases apart.
     */
    public native List<Entry> read(Position from, Position to)
      throws TimeoutException, OperationFailedException;

    /**
     * Returns the beginning position of the log (might be out of date
     * with respect to another replica).
     */
    public native Position beginning();

    /**
     * Returns the ending position of the log (might be out of date
     * with respect to another replica).
     */
    public native Position ending();

    protected native void initialize(Log log);
    protected native void finalize();

    private Log log; // Keeps the log from getting garbage collected.
    private long __log;
    private long __reader;
  }

  static class Writer {
    Writer(Log log) {
      this.log = log;
      initialize(log);
    }

    /**
     * Attepts to read from the log between the specified positions
     * (inclusive). If either of the positions are invalid, an
     * OperationFailedException will get thrown. Unfortunately, this
     * will also get thrown in other circumstances (e.g., disk
     * failure) and therefore it is currently impossible to tell these
     * two cases apart.
     */
    public native Position append(byte[] data)
      throws TimeoutException, WriterFailedException;

    /**
     * Attepts to truncate the log (from the beginning to the
     * specified position exclusive) If the position is invalid, an
     * OperationFailedException will get thrown. Unfortunately, this
     * will also get thrown in other circumstances (e.g., disk
     * failure) and therefore it is currently impossible to tell these
     * two cases apart.
     */
    public native Position truncate(Position to)
      throws TimeoutException, WriterFailedException;

    protected native void initialize(Log log);
    protected native void finalize();

    private Log log; // Keeps the log from getting garbage collected.
    private long __log;
    private long __writer;
  }

  Log(int quorum,
      String path,
      Set<String> pids) {
    initialize(quorum, path, pids);
  }

  Log(int quorum,
      String path,
      String servers,
      long timeout,
      TimeUnit unit,
      String znode) {
    initialize(quorum, path, servers, timeout, unit, znode);
  }

  /**
   * Returns a position based off of the bytes recovered from
   * Position.identity().
   */
  public Position position(byte[] identity) {
    long value =
      ((long) (identity[0] & 0xff) << 56) |
      ((long) (identity[1] & 0xff) << 48) |
      ((long) (identity[2] & 0xff) << 40) |
      ((long) (identity[3] & 0xff) << 32) |
      ((long) (identity[4] & 0xff) << 24) |
      ((long) (identity[5] & 0xff) << 16) |
      ((long) (identity[6] & 0xff) << 8) |
      ((long) (identity[7] & 0xff));
    return new Position(value);
  }

  protected native void initialize(int quorum,
                                   String path,
                                   Set<String> pids);

  protected native void initialize(int quorum,
                                   String path,
                                   String servers,
                                   long timeout,
                                   TimeUnit unit,
                                   String znode);

  protected native void finalize();

  private long __log;
}
