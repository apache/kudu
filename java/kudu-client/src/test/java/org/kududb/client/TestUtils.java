// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.client;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.List;
import java.util.Set;

/**
 * A grouping of methods that help unit testing.
 */
public class TestUtils {

  // Used by pidOfProcess()
  private static String UNIX_PROCESS_CLS_NAME =  "java.lang.UNIXProcess";
  private static Set VALID_SIGNALS =  ImmutableSet.of("STOP", "CONT", "TERM", "KILL");

  /**
   * Finds the next free port, starting with the one passed. Keep in mind the
   * time-of-check-time-of-use nature of this method, the returned port might become occupied
   * after it was checked for availability.
   * @param startPort First port to be probed.
   * @return A currently usable port.
   * @throws IOException IOE is thrown if we can't close a socket we tried to open or if we run
   * out of ports to try.
   */
  public static int findFreePort(int startPort) throws IOException {
    ServerSocket ss;
    for(int i = startPort; i < 65536; i++) {
      try {
        ss = new ServerSocket(i);
      } catch (IOException e) {
        continue;
      }
      ss.close();
      return i;
    }
    throw new IOException("Ran out of ports.");
  }

  /**
   * Finds a specified number of parts, starting with one passed. Keep in mind the
   * time-of-check-time-of-use nature of this method.
   * @see {@link #findFreePort(int)}
   * @param startPort First port to be probed.
   * @param numPorts Number of ports to reserve.
   * @return A list of currently usable ports.
   * @throws IOException IOE Is thrown if we can't close a socket we tried to open or if run
   * out of ports to try.
   */
  public static List<Integer> findFreePorts(int startPort, int numPorts) throws IOException {
    List<Integer> ports = Lists.newArrayListWithCapacity(numPorts);
    for (int i = 0; i < numPorts; i++) {
      startPort = findFreePort(startPort);
      ports.add(startPort++);
    }
    return ports;
  }

  /**
   * Gets the pid of a specified process. Relies on reflection and only works on
   * UNIX process, not guaranteed to work on JDKs other than Oracle and OpenJDK.
   * @param proc The specified process.
   * @return The process UNIX pid.
   * @throws IllegalArgumentException If the process is not a UNIXProcess.
   * @throws Exception If there are other getting the pid via reflection.
   */
  static int pidOfProcess(Process proc) throws Exception {
    Class<?> procCls = proc.getClass();
    if (!procCls.getName().equals(UNIX_PROCESS_CLS_NAME)) {
      throw new IllegalArgumentException("stopProcess() expects objects of class " +
          UNIX_PROCESS_CLS_NAME + ", but " + procCls.getName() + " was passed in instead!");
    }
    Field pidField = procCls.getDeclaredField("pid");
    pidField.setAccessible(true);
    return (Integer) pidField.get(proc);
  }

  /**
   * Send a code specified by its string representation to the specified process.
   * TODO: Use a JNR/JNR-Posix instead of forking the JVM to exec "kill".
   * @param proc The specified process.
   * @param sig The string representation of the process (e.g., STOP for SIGSTOP).
   * @throws IllegalArgumentException If the signal type is not supported.
   * @throws IllegalStateException If we are unable to send the specified signal.
   */
  static void signalProcess(Process proc, String sig) throws Exception {
    if (!VALID_SIGNALS.contains(sig)) {
      throw new IllegalArgumentException(sig + " is not a supported signal, only " +
              Joiner.on(",").join(VALID_SIGNALS) + " are supported");
    }
    int pid = pidOfProcess(proc);
    int rv = Runtime.getRuntime()
            .exec(String.format("kill -%s %d", sig, pid))
            .waitFor();
    if (rv != 0) {
      throw new IllegalStateException(String.format("unable to send SIG%s to process %s(pid=%d): " +
              "expected return code from kill, but got %d instead", sig, proc, pid, rv));
    }
  }

  /**
   * Pause the specified process by sending a SIGSTOP using the kill command.
   * @param proc The specified process.
   * @throws Exception If error prevents us from pausing the process.
   */
  static void pauseProcess(Process proc) throws Exception {
    signalProcess(proc, "STOP");
  }

  /**
   * Resumes the specified process by sending a SIGCONT using the kill command.
   * @param proc The specified process.
   * @throws Exception If error prevents us from resuming the process.
   */
  static void resumeProcess(Process proc) throws Exception {
    signalProcess(proc, "CONT");
  }
}