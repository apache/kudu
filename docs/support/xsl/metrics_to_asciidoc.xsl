<?xml version="1.0"?>
<!--

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:param name="binary"/>
<xsl:output method="text"/>

<!-- Normalize space -->
<xsl:template match="text()">
  <xsl:if test="normalize-space(.)">
    <xsl:value-of select="normalize-space(.)"/>
  </xsl:if>
</xsl:template>

<!-- Grab nodes of the <metric> elements -->
<xsl:template match="AllMetrics">
<!-- Inject the license text into the header of each file -->
////
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
////

:author: Kudu Team
:imagesdir: ./images
:icons: font
:toc: left
:toclevels: 1
:doctype: book
:backend: html5
:sectlinks:
:experimental:

[[<xsl:value-of select="$binary"/>_metrics]]
= `<xsl:value-of select="$binary"/>` Metrics

[[<xsl:value-of select="$binary"/>_warn]]
== Warning Metrics

Metrics tagged as 'warn' are metrics which can often indicate operational oddities
that may need more investigation.

<xsl:for-each select="metric">
  <xsl:sort select="name"/>
  <xsl:if test="contains(level, 'warn')">
[[<xsl:value-of select="$binary"/>_<xsl:value-of select="name"/>]]
=== `<xsl:value-of select="name"/>`

<xsl:value-of select="label"/>
{nbsp}
<xsl:value-of select="description"/>

[cols="1h,3d", width="50%"]
|===
| Entity Type | <xsl:value-of select="entity_type"/>
| Unit | <xsl:value-of select="unit"/>
| Type | <xsl:value-of select="type"/>
| Level | <xsl:value-of select="level"/>
|===
{nbsp}

  </xsl:if>
</xsl:for-each>

[[<xsl:value-of select="$binary"/>_info]]
== Info Metrics

Metrics tagged as 'info' are generally useful metrics that operators always want
to have available but may not be monitored under normal circumstances.

<xsl:for-each select="metric">
  <xsl:sort select="name"/>
  <xsl:if test="contains(level, 'info')">
[[<xsl:value-of select="$binary"/>_<xsl:value-of select="name"/>]]
=== `<xsl:value-of select="name"/>`

<xsl:value-of select="label"/>
{nbsp}
<xsl:value-of select="description"/>

[cols="1h,3d", width="50%"]
|===
| Entity Type | <xsl:value-of select="entity_type"/>
| Unit | <xsl:value-of select="unit"/>
| Type | <xsl:value-of select="type"/>
| Level | <xsl:value-of select="level"/>
|===
{nbsp}

  </xsl:if>
</xsl:for-each>

[[<xsl:value-of select="$binary"/>_debug]]
== Debug Metrics

Metrics tagged as 'debug' are diagnostically helpful but generally not monitored
during normal operation.

<xsl:for-each select="metric">
  <xsl:sort select="name"/>
  <xsl:if test="contains(level, 'debug')">
[[<xsl:value-of select="$binary"/>_<xsl:value-of select="name"/>]]
=== `<xsl:value-of select="name"/>`

<xsl:value-of select="label"/>
{nbsp}
<xsl:value-of select="description"/>

[cols="1h,3d", width="50%"]
|===
| Entity Type | <xsl:value-of select="entity_type"/>
| Unit | <xsl:value-of select="unit"/>
| Type | <xsl:value-of select="type"/>
| Level | <xsl:value-of select="level"/>
|===
{nbsp}

  </xsl:if>
</xsl:for-each>
'''
</xsl:template>
</xsl:stylesheet>
