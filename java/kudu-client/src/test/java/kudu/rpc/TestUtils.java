// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.rpc;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * A grouping of methods that help unit testing.
 */
public class TestUtils {

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
}