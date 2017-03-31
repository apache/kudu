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
<xsl:output method="text"/>

<!-- Normalize space -->
<xsl:template match="text()">
    <xsl:if test="normalize-space(.)">
      <xsl:value-of select="normalize-space(.)"/>
    </xsl:if>
</xsl:template>

<xsl:template match="AllModes">
<!-- Inject the license text into the header -->
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

= Command Hierarchy
<xsl:apply-templates select="mode" mode="toc"/>

= Command Details
<xsl:apply-templates select="mode" mode="content"/>

</xsl:template>

<!-- Table of contents template -->
<xsl:template match="mode|action" mode="toc">
<xsl:variable name="depth" select="count(ancestor::*)"/>
<xsl:variable name="ref"><xsl:if test="../name"><xsl:value-of select="../name"/>-</xsl:if><xsl:value-of select="name"/></xsl:variable>
<!-- Print bullets at mode/action depth -->
<xsl:for-each select="(//node())[$depth >= position()]">*</xsl:for-each> &lt;&lt;<xsl:value-of select="$ref"/>,<xsl:value-of select="name"/>&gt;&gt;
<xsl:apply-templates select="mode|action" mode="toc"/>
</xsl:template>

<!-- Content for mode template -->
<xsl:template match="mode" mode="content">
<xsl:variable name="depth" select="count(ancestor::*) + 1"/>
<!-- Create unique anchor with the parent name and name -->
<xsl:variable name="anchor"><xsl:if test="../name"><xsl:value-of select="../name"/>-</xsl:if><xsl:value-of select="name"/></xsl:variable>
[[<xsl:value-of select="$anchor"/>]]
<!-- Print header level at mode depth -->
<xsl:for-each select="(//node())[$depth >= position()]">=</xsl:for-each> `<xsl:value-of select="name"/>`: <xsl:value-of select="description"/>
{empty} +
<xsl:apply-templates select="mode|action" mode="content"/>
</xsl:template>

<!-- Content for action template -->
<xsl:template match="action" mode="content">
<xsl:variable name="depth" select="count(ancestor::*) + 1"/>
<!-- Create unique anchor with the parent name and name -->
<xsl:variable name="anchor"><xsl:if test="../name"><xsl:value-of select="../name"/>-</xsl:if><xsl:value-of select="name"/></xsl:variable>
[[<xsl:value-of select="$anchor"/>]]
<!-- Print header level at action depth -->
<xsl:for-each select="(//node())[$depth >= position()]">=</xsl:for-each> `<xsl:value-of select="name"/>`: <xsl:value-of select="description"/>{nbsp}
<xsl:if test="extra_description != ''"><xsl:value-of select="extra_description"/> +</xsl:if>
*Usage:* +
`<xsl:value-of select="usage"/>`
{empty} +
*Arguments:*
[frame="topbot",options="header"]
|===
| Name |  Description | Type | Default
<xsl:for-each select="argument">
<!-- escape pipe character in description for use in tables -->
<xsl:variable name="escaped_description">
  <xsl:call-template name="string-replace-all">
    <xsl:with-param name="text" select="description" />
    <xsl:with-param name="replace" select="'|'" />
    <xsl:with-param name="by" select="'\|'" />
  </xsl:call-template>
</xsl:variable>
| <xsl:value-of select="name"/><xsl:if test="contains(kind, 'variadic')">...</xsl:if><xsl:if test="contains(kind, 'optional')"> (optional)</xsl:if>
| <xsl:value-of select="$escaped_description"/>
| <xsl:value-of select="type"/>
| <xsl:choose><xsl:when test="default_value != ''">`<xsl:value-of select="default_value"/>`</xsl:when><xsl:otherwise>none</xsl:otherwise></xsl:choose>
</xsl:for-each>
|===
{empty} +
</xsl:template>

<!-- Template to support string replacement in XSLT 1.0) -->
<xsl:template name="string-replace-all">
<xsl:param name="text" />
<xsl:param name="replace" />
<xsl:param name="by" />
<xsl:choose>
  <xsl:when test="contains($text, $replace)">
    <xsl:value-of select="substring-before($text,$replace)" />
    <xsl:value-of select="$by" />
    <xsl:call-template name="string-replace-all">
      <xsl:with-param name="text" select="substring-after($text,$replace)" />
      <xsl:with-param name="replace" select="$replace" />
      <xsl:with-param name="by" select="$by" />
    </xsl:call-template>
  </xsl:when>
  <xsl:otherwise>
    <xsl:value-of select="$text" />
  </xsl:otherwise>
</xsl:choose>
</xsl:template>
</xsl:stylesheet>
