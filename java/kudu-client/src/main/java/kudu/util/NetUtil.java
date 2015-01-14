// Copyright (c) 2015, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package kudu.util;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

import java.util.List;

/**
 * Networking related methods.
 */
public class NetUtil {

  /**
   * Parse a "host:port" pair into a {@link HostAndPort} object. If there is no
   * port specified in the string, then 'defaultPort' is used.
   *
   * @param addrString  A host or a "host:port" pair.
   * @param defaultPort Default port to use if no port is specified in addrString.
   * @return The HostAndPort object constructed from addrString.
   */
  public static HostAndPort parseString(String addrString, int defaultPort) {
    return addrString.indexOf(':') == -1 ? HostAndPort.fromParts(addrString, defaultPort) :
               HostAndPort.fromString(addrString);
  }

  /**
   * Parse a comma separated list of "host:port" pairs into a list of
   * {@link HostAndPort} objects. If no port is specified for an entry in
   * the comma separated list, then a default port is used.
   *
   * @param commaSepAddrs The comma separated list of "host:port" pairs.
   * @param defaultPort   The default port to use if no port is specified.
   * @return A list of HostAndPort objects constructed from commaSepAddrs.
   */
  public static List<HostAndPort> parseStrings(final String commaSepAddrs, int defaultPort) {
    Iterable<String> addrStrings = Splitter.on(',').trimResults().split(commaSepAddrs);
    List<HostAndPort> hostsAndPorts = Lists.newArrayListWithCapacity(Iterables.size(addrStrings));
    for (String addrString : addrStrings) {
      HostAndPort hostAndPort = parseString(addrString, defaultPort);
      hostsAndPorts.add(hostAndPort);
    }
    return hostsAndPorts;
  }
}
