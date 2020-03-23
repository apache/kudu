//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import sbt._

ThisBuild / scalaVersion := "2.12.10"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "org.apache.kudu"
ThisBuild / organizationName := "Apache Kudu"

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .enablePlugins(OsDetectorPlugin)
  .settings(
    Defaults.itSettings,
    name := "sbt-int-test-example",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "it,test",
    libraryDependencies += "org.apache.kudu" % "kudu-client" % "1.11.1",
    libraryDependencies += "org.apache.kudu" % "kudu-test-utils" % "1.11.1" % "it",
    libraryDependencies += "org.apache.kudu" % "kudu-binary" % "1.11.1" % "it" classifier osDetectorClassifier.value,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
  )
