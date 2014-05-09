/**
 * Portions copyright (c) 2014 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package kudu.mapreduce;

import kudu.rpc.KuduTable;
import kudu.rpc.Operation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Utility class to setup MR jobs that use Kudu as an input and/or output
 */
public class KuduTableMapReduceUtil {
  // Mostly lifted from HBase's TableMapReduceUtil

  private static final Log LOG = LogFactory.getLog(KuduTableMapReduceUtil.class);

  /**
   * Doesn't need instantiation
   */
  private KuduTableMapReduceUtil() { }

  /**
   * Setup the required configurations and classes to write to Kudu
   * @param job Job to configure
   * @param masterAddress hostname:port where the master is
   * @param table Which table to write to
   * @param operationTimeoutMs Timeout for operations to complete
   * @param addDependencies If the job should add the Kudu dependencies to the distributed cache
   * @throws IOException
   */
  public static void initTableOutputFormat(Job job, String masterAddress,
                                           String table, long operationTimeoutMs,
                                           boolean addDependencies) throws IOException {
    job.setOutputFormatClass(KuduTableOutputFormat.class);
    job.setOutputKeyClass(WritableRowKey.class);
    job.setOutputValueClass(Operation.class);
    Configuration conf = job.getConfiguration();
    conf.set(KuduTableOutputFormat.MASTER_ADDRESS_KEY, masterAddress);
    conf.set(KuduTableOutputFormat.OUTPUT_TABLE_KEY, table);
    conf.setLong(KuduTableOutputFormat.OPERATION_TIMEOUT_MS_KEY, operationTimeoutMs);
    if (addDependencies) {
      addDependencyJars(job);
    }
  }

  /**
   * Use this method when setting up a task to get access to the KuduTable in order to create
   * Inserts, Updates, and Deletes.
   * @param context Map context
   * @return The kudu table object as setup by the output format
   */
  public static KuduTable getTableFromContext(TaskInputOutputContext context) {
    String multitonKey = context.getConfiguration().get(KuduTableOutputFormat.MULTITON_KEY);
    return KuduTableOutputFormat.getKuduTable(multitonKey);
  }

  /**
   * Add the Kudu dependency jars as well as jars for any of the configured
   * job classes to the job configuration, so that JobClient will ship them
   * to the cluster and add them to the DistributedCache.
   */
  public static void addDependencyJars(Job job) throws IOException {
    addKuduDependencyJars(job.getConfiguration());
    try {
      addDependencyJars(job.getConfiguration(),
          // when making changes here, consider also mapred.TableMapReduceUtil
          // pull job classes
          job.getMapOutputKeyClass(),
          job.getMapOutputValueClass(),
          job.getInputFormatClass(),
          job.getOutputKeyClass(),
          job.getOutputValueClass(),
          job.getOutputFormatClass(),
          job.getPartitionerClass(),
          job.getCombinerClass());
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /**
   * Add the jars containing the given classes to the job's configuration
   * such that JobClient will ship them to the cluster and add them to
   * the DistributedCache.
   */
  public static void addDependencyJars(Configuration conf,
                                       Class<?>... classes) throws IOException {

    FileSystem localFs = FileSystem.getLocal(conf);
    Set<String> jars = new HashSet<String>();
    // Add jars that are already in the tmpjars variable
    jars.addAll(conf.getStringCollection("tmpjars"));

    // add jars as we find them to a map of contents jar name so that we can avoid
    // creating new jars for classes that have already been packaged.
    Map<String, String> packagedClasses = new HashMap<String, String>();

    // Add jars containing the specified classes
    for (Class<?> clazz : classes) {
      if (clazz == null) continue;

      Path path = findOrCreateJar(clazz, localFs, packagedClasses);
      if (path == null) {
        LOG.warn("Could not find jar for class " + clazz +
            " in order to ship it to the cluster.");
        continue;
      }
      if (!localFs.exists(path)) {
        LOG.warn("Could not validate jar file " + path + " for class "
            + clazz);
        continue;
      }
      jars.add(path.toString());
    }
    if (jars.isEmpty()) return;

    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }

  /**
   * Add Kudu and its dependencies (only) to the job configuration.
   * <p>
   * This is intended as a low-level API, facilitating code reuse between this
   * class and its mapred counterpart. It also of use to external tools that
   * need to build a MapReduce job that interacts with Kudu but want
   * fine-grained control over the jars shipped to the cluster.
   * </p>
   * @param conf The Configuration object to extend with dependencies.
   * @see kudu.mapreduce.KuduTableMapReduceUtil
   * @see <a href="https://issues.apache.org/jira/browse/PIG-3285">PIG-3285</a>
   */
  public static void addKuduDependencyJars(Configuration conf) throws IOException {
    addDependencyJars(conf,
        // explicitly pull a class from each module
        kudu.rpc.Operation.class,                      // kudu-client
        kudu.mapreduce.WritableRowKey.class,           // kudu-mapreduce
        // pull necessary dependencies
        org.jboss.netty.channel.ChannelFactory.class,
        com.google.protobuf.Message.class,
        com.google.common.collect.Lists.class);
  }

  /**
   * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop), finds
   * the Jar for a class or creates it if it doesn't exist. If the class is in
   * a directory in the classpath, it creates a Jar on the fly with the
   * contents of the directory and returns the path to that Jar. If a Jar is
   * created, it is created in the system temporary directory. Otherwise,
   * returns an existing jar that contains a class of the same name. Maintains
   * a mapping from jar contents to the tmp jar created.
   * @param my_class the class to find.
   * @param fs the FileSystem with which to qualify the returned path.
   * @param packagedClasses a map of class name to path.
   * @return a jar file that contains the class.
   * @throws IOException
   */
  private static Path findOrCreateJar(Class<?> my_class, FileSystem fs,
                                      Map<String, String> packagedClasses)
      throws IOException {
    // attempt to locate an existing jar for the class.
    String jar = findContainingJar(my_class, packagedClasses);
    if (null == jar || jar.isEmpty()) {
      jar = JarFinder.getJar(my_class);
      updateMap(jar, packagedClasses);
    }

    if (null == jar || jar.isEmpty()) {
      return null;
    }

    LOG.debug(String.format("For class %s, using jar %s", my_class.getName(), jar));
    return new Path(jar).makeQualified(fs);
  }

  /**
   * Find a jar that contains a class of the same name, if any. It will return
   * a jar file, even if that is not the first thing on the class path that
   * has a class with the same name. Looks first on the classpath and then in
   * the <code>packagedClasses</code> map.
   * @param my_class the class to find.
   * @return a jar file that contains the class, or null.
   * @throws IOException
   */
  private static String findContainingJar(Class<?> my_class, Map<String, String> packagedClasses)
      throws IOException {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";

    // first search the classpath
    for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
      URL url = itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        return toReturn.replaceAll("!.*$", "");
      }
    }

    // now look in any jars we've packaged using JarFinder. Returns null when
    // no jar is found.
    return packagedClasses.get(class_file);
  }

  /**
   * Add entries to <code>packagedClasses</code> corresponding to class files
   * contained in <code>jar</code>.
   * @param jar The jar who's content to list.
   * @param packagedClasses map[class -> jar]
   */
  private static void updateMap(String jar, Map<String, String> packagedClasses) throws IOException {
    if (null == jar || jar.isEmpty()) {
      return;
    }
    ZipFile zip = null;
    try {
      zip = new ZipFile(jar);
      for (Enumeration<? extends ZipEntry> iter = zip.entries(); iter.hasMoreElements();) {
        ZipEntry entry = iter.nextElement();
        if (entry.getName().endsWith("class")) {
          packagedClasses.put(entry.getName(), jar);
        }
      }
    } finally {
      if (null != zip) zip.close();
    }
  }
}
