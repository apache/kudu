;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements. See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership. The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License. You may obtain a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing,
;; software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
;; KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations
;; under the License.

(ns jepsen.kudu.util
  "Utilities for Apache Kudu jepsen tests"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen
             [control :as c :refer [|]]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn path
  "Returns the filesystem path for the path components joined with Unix path
  separator."
  [& components]
  (str/join "/" components))

;; TODO(aserbin): make it possible to set the version via a run-time option.
;;
;; The empty string corresponds to the latest snapshot from the main trunk.
;; To run against some other branch, set to "<major>.<minor>.<patch>";
;; e.g. set "1.2.0" to run against packages built for Kudu 1.2.0 release.
(def kudu-pkg-version "")

(def kudu-repo-url
  (str "http://repos.jenkins.cloudera.com/kudu" kudu-pkg-version
       "-nightly/debian/jessie/amd64/kudu"))
(def kudu-repo-name "kudu-nightly")
(def kudu-repo-apt-line (str "deb " kudu-repo-url " jessie-kudu contrib"))
(def kudu-required-packages
  "The set of the required system packages (more are installed by dependency)."
  [:libsasl2-modules
   :libsasl2-modules-gssapi-mit
   :lsb-release
   :ntp
   :openssl])
(def kudu-master-pkg :kudu-master)
(def kudu-tserver-pkg :kudu-tserver)

(def kudu-build-dir "../../build/latest")

(def kudu-conf-dir "/etc/kudu/conf")
(def kudu-master-gflagfile (path kudu-conf-dir "master.gflagfile"))
(def kudu-tserver-gflagfile (path kudu-conf-dir "tserver.gflagfile"))
(def kudu-target-bin-dir "/opt/local/bin")
(def kudu-target-sbin-dir "/opt/local/sbin")
(def kudu-uname "kudu")
(def kudu-uid 999)
(def kudu-gname "kudu")
(def kudu-gid 999)
(def kudu-home-dir "/var/lib/kudu")
(def kudu-master-home-dir (path kudu-home-dir "master"))
(def kudu-tserver-home-dir (path kudu-home-dir "tserver"))
(def kudu-log-dir "/var/log/kudu")
(def kudu-master-log-file (path kudu-log-dir "kudu-master.log"))
(def kudu-tserver-log-file (path kudu-log-dir "kudu-tserver.log"))
(def kudu-run-dir "/var/run/kudu")
(def kudu-master-pid-file (path kudu-run-dir "kudu-master-kudu.pid"))
(def kudu-tserver-pid-file (path kudu-run-dir "kudu-tserver-kudu.pid"))

(def master-rpc-port 7051)
(def tserver-rpc-port 7050)

(defn kudu-cli
  "Returns path to the Kudu CLI tool or just binary name, if it's appropriate
  to rely on the PATH environment variable."
  [test node]
  (if (:use-packages? test)
    "kudu"  ;; relying on the standard PATH env variable
    (path kudu-target-bin-dir "kudu")))


(defn concatenate-addresses
  "Returns a list of the addresses in form 'h0:port,h1:port,h2:port'
  given a port and list of hostnames."
  [port hosts]
  (str/join "," (map #(str (name %) ":" (str port)) hosts)))


(defn group-exist?
  "If the specified group exists?"
  [group-name]
  (try (c/exec :egrep (str "^" group-name) "/etc/group")
       true
       (catch RuntimeException _ false)))


(defn user-exist?
  "If the specified user exists?"
  [user-name]
  (try (c/exec :id user-name)
       true
       (catch RuntimeException _ false)))


(defn ntp-in-sync?
  "Is the NTP server in sync state? This function should be called in the
  context of already established SSH session at the node."
  []
  (try (c/exec :ntp-wait :-n1 :-s1)
       true
       (catch RuntimeException _ false)))


(defn kudu-master-in-service?
  "Is the Kudu master process at the specified node in service already?
  This function should be called in the context of already established SSH
  session at the node."
  [test node]
  (try (c/exec (kudu-cli test node) :table :list node)
       true
       (catch RuntimeException _ false)))


(defn kudu-master-see-tservers?
  "Whether the Kudu master sees the specified number of tablet servers."
  [test node tservers-count]
  (let [pattern (str "Fetched info from all "
                     (str tservers-count)" Tablet Servers")]
    (try (c/exec (kudu-cli test node) :cluster :ksck node | :grep pattern)
         true
         (catch RuntimeException _ false))))


(defn kudu-tserver-in-service?
  "Is the Kudu tserver process at the specified node is up and running?
  This function should be called in the context of already established SSH
  session at the node."
  [test node]
  (try (c/exec (kudu-cli test node) :tserver :status node)
       true
       (catch RuntimeException _ false)))


(defn start-kudu-master
  "Start Kudu master daemon at the specified node. This function should
  be called in the super-user context (jepsen.control/su)."
  [test node]
  (info node "Starting Kudu Master")
  (let [use-svc-scripts? (:use-packages? test)]
    (if use-svc-scripts?
      (c/exec :service :kudu-master :start)
      (c/exec :sudo :-u kudu-uname :start-stop-daemon
              :--start
              :--background
              :--make-pidfile
              :--pidfile      kudu-master-pid-file
              :--chdir        kudu-home-dir
              :--no-close
              :--oknodo
              :--exec         (path kudu-target-sbin-dir "kudu-master")
              :--
              :--flagfile kudu-master-gflagfile
              :>> kudu-master-log-file (c/lit "2>&1"))))

  ;; Wait for master services avaiable (awaiting for the catalog manager).
  (loop [iteration 0]
    (when-not (kudu-master-in-service? test node)
      (if (> iteration 100)
        (c/exec :echo "timeout waiting for master server to start" (c/lit ";")
                :false)
        (do
          (Thread/sleep 500)
          (recur (inc iteration))))))

  ;; Wait until the master sees all tservers in the cluster. Otherwise
  ;; the client would not be able to create a table with the desired
  ;; replication factor when not all tservers have registered.
  (let [tservers-count (count (:tservers test))]
    (loop [iteration 0]
      (when-not (kudu-master-see-tservers? test node tservers-count)
        (if (> iteration 200)
          (c/exec :echo "timeout waiting for all tservers to start" (c/lit ";")
                  :false)
          (do
            (Thread/sleep 500)
            (recur (inc iteration))))))))


(defn stop-kudu-master
  "Stop Kudu master daemon at the specified node. This function should be
  called in the super-user context (jepsen.control/su)."
  [test node]
  (info node "Stopping Kudu Master")
  (let [use-svc-scripts? (:use-packages? test)]
    (if use-svc-scripts?
      (meh (c/exec :service :kudu-master :stop))
      (cu/stop-daemon! "kudu-master" kudu-master-pid-file))))


(defn start-kudu-tserver
  "Start Kudu tablet server daemon at the specified node. This function
  should be called in the super-user context (jepsen.control/su)."
  [test node]
  (info node "Starting Kudu Tablet Server")
  (let [use-svc-scripts? (:use-packages? test)]
    (if use-svc-scripts?
      (c/exec :service :kudu-tserver :start)
      (c/exec :sudo :-u kudu-uname :start-stop-daemon
              :--start
              :--background
              :--make-pidfile
              :--pidfile      kudu-tserver-pid-file
              :--chdir        kudu-home-dir
              :--no-close
              :--oknodo
              :--exec         (path kudu-target-sbin-dir "kudu-tserver")
              :--
              :--flagfile kudu-tserver-gflagfile
              :>> kudu-tserver-log-file (c/lit "2>&1"))))

  ;; Wait for the tablet server to become on-line.
  (loop [iteration 0]
    (when-not (kudu-tserver-in-service? test node)
      (if (> iteration 100)
        (c/exec :echo "timeout waiting for tablet server to start" (c/lit ";")
                :false)
        (do
          (Thread/sleep 500)
          (recur (inc iteration)))))))


(defn stop-kudu-tserver
  "Stops Kudu Tablet Server on the specified node."
  [test node]
  (info node "Stopping Kudu Tablet Server")
  (let [use-svc-scripts? (:use-packages? test)]
    (if use-svc-scripts?
      (meh (c/exec :service :kudu-tserver :stop))
      (cu/stop-daemon! "kudu-tserver" kudu-tserver-pid-file))))


(defn kudu-cfg-master
  "Returns Kudu master flags file contents."
  [test]
  (let [data-path kudu-master-home-dir
        flags [(str "--fs_wal_dir=" data-path)
               (str "--fs_data_dirs=" data-path)
               (str "--log_dir=" kudu-log-dir)
               (str "--rpc_bind_addresses=0.0.0.0:" (str master-rpc-port))]]
    ;; Only set the master addresses when there is more than one master
    (str/join "\n"
              (if (> (count (:masters test)) 1)
                (conj flags (str "--master_addresses="
                                 (concatenate-addresses master-rpc-port
                                                        (:masters test))))
                flags))))


(defn kudu-cfg-tserver
  "Returns Kudu tserver flags file contents."
  [test]
  (let [data-path kudu-tserver-home-dir
        flags [(str "--fs_wal_dir=" data-path)
               (str "--fs_data_dirs=" data-path)
               (str "--log_dir=" kudu-log-dir)
               (str "--rpc_bind_addresses=0.0.0.0:" (str tserver-rpc-port))
               (str "--heartbeat_interval_ms="
                    (str (:ts-hb-interval-ms test)))
               (str "--raft_heartbeat_interval_ms="
                    (str (:ts-raft-hb-interval-ms test)))
               (str "--heartbeat_max_failures_before_backoff="
                    (str (:ts-hb-max-failures-before-backoff test)))]]
    (str/join "\n" (conj flags (str "--tserver_master_addrs="
                                    (concatenate-addresses master-rpc-port
                                                           (:masters test)))))))


(defn ntp-server-config
  "Returns ntp.conf contents for Kudu master node."
  []
  (let [common-opts (slurp (io/resource "ntp.conf.common"))
        server-opts (slurp (io/resource "ntp.conf.server"))]
    (str common-opts "\n" server-opts)))


(defn ntp-slave-config
  "Returns ntp.conf contents for Kudu tserver node."
  [servers]
  (let [common-opts (slurp (io/resource "ntp.conf.common"))
        server-lines (map #(str "server " (name %)
                                " burst iburst prefer minpoll 4 maxpoll 4")
                          servers)]
    (str common-opts "\n" (str/join "\n" server-lines))))


(defn prepare-node-with-pkgs
  "Prepare a Kudu node: install Kudu using packages."
  [test node]
  (let [repo-file (str "/etc/apt/sources.list.d/"
                       (name kudu-repo-name) ".list")]
    (when-not (cu/exists? repo-file)
      (info node "Adding " kudu-repo-name " package repositoy")
      (debian/add-repo! kudu-repo-name kudu-repo-apt-line)
      (info node "Fetching " kudu-repo-name " package key")
      (c/exec :curl :-fLSs (str kudu-repo-url "/" "archive.key") |
              :apt-key :add :-)
      (info node "Updating package index")
      (debian/update!)))

  (when (.contains (:masters test) node)
    (when-not (debian/installed? kudu-master-pkg)
      (info node "Installing kudu-master package")
      (debian/install kudu-master-pkg)))
  (when (.contains (:tservers test) node)
    (when-not (debian/installed? kudu-tserver-pkg)
      (info node "Installing kudu-tserver package")
      (debian/install kudu-tserver-pkg))))


(defn prepare-node-with-binaries
  "Prepare Kudu node: create the directory structure and place necessary
  Kudu binaries at place."
  [test node]

  (when-not (group-exist? kudu-gname)
    (c/exec :groupadd :-o :-g kudu-gid kudu-gname))
  (when-not (user-exist? kudu-uname)
    (c/exec :useradd :-o :-u kudu-uid :-g kudu-gname :-d kudu-home-dir
            :-s "/usr/sbin/nologin" kudu-uname))

  ;; Prepare directory structure for the files.
  (c/exec :mkdir :-p kudu-conf-dir)
  (when (.contains (:masters test) node)
    (c/exec :mkdir :-p path kudu-master-home-dir))
  (when (.contains (:tservers test) node)
    (c/exec :mkdir :-p path kudu-tserver-home-dir))
  (c/exec :chown :-R (str kudu-uname ":" kudu-gname) kudu-home-dir)

  (c/exec :mkdir :-p kudu-run-dir)
  (c/exec :chown :-R (str kudu-uname ":" kudu-gname) kudu-run-dir)

  (c/exec :mkdir :-p kudu-log-dir)
  (c/exec :chown :-R (str kudu-uname ":" kudu-gname) kudu-log-dir)

  (c/exec :rm :-rf kudu-target-bin-dir)
  (c/exec :mkdir :-p kudu-target-bin-dir)

  (c/exec :rm :-rf kudu-target-sbin-dir)
  (c/exec :mkdir :-p kudu-target-sbin-dir)

  ;; Copy appropriate binaries to the node.
  (when (.contains (:masters test) node)
    (let [master-binary-src (path kudu-build-dir "bin" "kudu-master")
          master-binary-dst (path kudu-target-sbin-dir "kudu-master")]
      (c/upload master-binary-src kudu-target-sbin-dir)
      (c/exec :chmod 755 master-binary-dst)))
  (when (.contains (:tservers test) node)
    (let [tserver-binary-src (path kudu-build-dir "bin" "kudu-tserver")
          tserver-binary-dst (path kudu-target-sbin-dir "kudu-tserver")]
      (c/upload tserver-binary-src kudu-target-sbin-dir)
      (c/exec :chmod 755 tserver-binary-dst)))
  (let [kudu-cli-binary-src (path kudu-build-dir "bin" "kudu")
        kudu-cli-binary-dst (path kudu-target-bin-dir "kudu")]
    (c/upload kudu-cli-binary-src kudu-target-bin-dir)
    (c/exec :chmod 755 kudu-cli-binary-dst)))


(defn prepare-node
  "Prepare Kudu node using either packaged Kudu software or
  assorted Kudu binaries for the server-side components."
  [test node]
  (when-not (debian/installed? kudu-required-packages)
      (info node "Installing required packages")
      (debian/install kudu-required-packages))


  (if (:use-packages? test)
    (prepare-node-with-pkgs test node)
    (prepare-node-with-binaries test node))

  (when (.contains (:masters test) node)
    (c/exec :rm :-rf kudu-master-home-dir)
    (c/exec :rm :-f kudu-master-log-file)
    (c/exec :echo (str (slurp (io/resource "kudu.flags"))
                       "\n" (kudu-cfg-master test)) :> kudu-master-gflagfile))
  (when (.contains (:tservers test) node)
    (c/exec :rm :-rf kudu-tserver-home-dir)
    (c/exec :rm :-f kudu-tserver-log-file)
    (c/exec :echo (str (slurp (io/resource "kudu.flags"))
                       "\n" (kudu-cfg-tserver test)) :> kudu-tserver-gflagfile)))


(defn sync-time
  "When ntpd is not in synchronized state, revamps its configs and restarts
  the ntpd daemon."
  [test node]
  (when-not (ntp-in-sync?)
    (c/exec :service :ntp :stop "||" :true)
    (c/exec :echo "NTPD_OPTS='-g -N'" :> "/etc/default/ntp")
    (when (.contains (:masters test) node)
      (c/exec :echo (ntp-server-config) :> "/etc/ntp.conf"))
    (when (.contains (:tservers test) node)
      (c/exec :echo (ntp-slave-config (:masters test)):> "/etc/ntp.conf"))
    (c/exec :service :ntp :start)
    ;; Wait for 5 minutes max for ntpd to get into synchronized state.
    (c/exec :ntp-wait :-s1 :-n300)))


(defn start-kudu
  "Start Kudu services on the node."
  [test node]
  (when (.contains (:masters test) node) (start-kudu-master test node))
  (when (.contains (:tservers test) node) (start-kudu-tserver test node)))
