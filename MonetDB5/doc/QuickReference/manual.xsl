<?xml version="1.0"?>

<xsl:stylesheet
  version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:saxon="http://icl.com/saxon"
>

<xsl:output method="text"/>

<xsl:template match="text()"/>

<xsl:template name="latex-header">
	<xsl:text>
\documentclass[10pt,fleqn]{article}

%%\usepackage{a4wide}
%%\usepackage{amsthm}
\usepackage{makeidx}
\usepackage{times}
\usepackage{epsf}
\usepackage{graphicx}
\usepackage{epsfig}
\usepackage{graphics}
\usepackage{color}
\usepackage{url}

\begin{document}

\title{Monet 5.0 Quick Reference Manual}
\author{ CWI Monet Development Group \\
{\small \textsc{CWI}, Netherlands}}
\date{}
\maketitle
\tableofcontents
\newpage
	</xsl:text>
</xsl:template>

<xsl:template name="latex-footer">
	<xsl:text>
\printindex
\end{document}
	</xsl:text>
</xsl:template>

<xsl:template match="/">
	<xsl:call-template name="latex-header"/>

	<xsl:apply-templates/>

	<xsl:call-template name="latex-footer"/>
</xsl:template>

<xsl:template name="list">
	
<xsl:if test="count(saxon:evaluate($type))>0">
<xsl:text>
\subsection{</xsl:text><xsl:value-of select="$title"/><xsl:text>}
</xsl:text>
<xsl:apply-templates select="saxon:evaluate($type)">
	<xsl:sort select="@name"/>
</xsl:apply-templates>
<xsl:text>
</xsl:text>
</xsl:if>
</xsl:template>

<xsl:template match="manual">
	<xsl:apply-templates/>
</xsl:template>

<xsl:template match="module">
<xsl:text>
\section{Module </xsl:text><xsl:value-of select="@name"/><xsl:text>}
</xsl:text>

<xsl:call-template name="list">
	<xsl:with-param name="type">command</xsl:with-param>
	<xsl:with-param name="title">Commands</xsl:with-param>
</xsl:call-template>

<xsl:call-template name="list">
	<xsl:with-param name="type">function</xsl:with-param>
	<xsl:with-param name="title">Functions</xsl:with-param>
</xsl:call-template>

<xsl:call-template name="list">
	<xsl:with-param name="type">pattern</xsl:with-param>
	<xsl:with-param name="title">Patterns</xsl:with-param>
</xsl:call-template>

</xsl:template>

<xsl:template match="atommodule">
<xsl:text>
\section{Atom Module </xsl:text><xsl:value-of select="@name"/><xsl:text>}
</xsl:text>

<xsl:call-template name="list">
	<xsl:with-param name="type">command</xsl:with-param>
	<xsl:with-param name="title">Commands</xsl:with-param>
</xsl:call-template>

<xsl:call-template name="list">
	<xsl:with-param name="type">function</xsl:with-param>
	<xsl:with-param name="title">Functions</xsl:with-param>
</xsl:call-template>

<xsl:call-template name="list">
	<xsl:with-param name="type">pattern</xsl:with-param>
	<xsl:with-param name="title">Patterns</xsl:with-param>
</xsl:call-template>

</xsl:template>

<xsl:template match="*">
<xsl:text>
\noindent\begin{tabular}{p{1in} p{4.5in}}
{ </xsl:text><xsl:value-of select="@name"/><xsl:text>}
\index{ </xsl:text><xsl:value-of select="@name"/><xsl:text>}
 &amp;
</xsl:text>
<xsl:if test="comment">
	{<xsl:value-of select="comment"/>} \\
</xsl:if>
<xsl:if test="not(comment)">
<xsl:text>\\ 
</xsl:text>
</xsl:if>

<xsl:for-each select="instantiation">
	<xsl:text> &amp; {\it </xsl:text>
        <xsl:value-of select="signature"/>
	<xsl:text>}\\</xsl:text>
	<xsl:if test="comment">
		<xsl:text>&amp;</xsl:text>
        	<xsl:value-of select="comment"/>
		<xsl:text>\\</xsl:text>
	</xsl:if>
</xsl:for-each>
<xsl:text>
\end{tabular}
</xsl:text>
<xsl:text>
</xsl:text>

</xsl:template>

</xsl:stylesheet>
