<?xml version="1.0" encoding="ISO-8859-1"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
   version="1.0">
  <xsl:strip-space elements="*" />
  <xsl:template match="*">
    <xsl:copy>
      <xsl:if test="@*">
        <xsl:for-each select="@*">
          <xsl:element name="{name()}">
            <xsl:value-of select="." />
          </xsl:element>
        </xsl:for-each>
      </xsl:if>
      <xsl:apply-templates />
    </xsl:copy>
  </xsl:template>
</xsl:stylesheet>
