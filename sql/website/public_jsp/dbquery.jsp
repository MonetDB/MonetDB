<%@ page import="java.io.*, java.sql.*, java.util.*" %>
<%!
	public String toHtml(String in) {
		if (in == null) return(in);
		return(in.replaceAll("&", "&amp;").replaceAll("\\\"", "&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"));
	}
%>
<html>
  <head>
    <title>Querying on MonetDB/SQL</title>
  </head>
<%
	// make sure the driver is loaded
	Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
	Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/voc", "monetdb", "monetdb");
	try {
		DatabaseMetaData dbmd = con.getMetaData();
		Statement st = con.createStatement();

		String types[] = { "TABLE", "VIEW" };
		ResultSet rs = dbmd.getTables(null, "voc", null, types);
%>
  <body>
    <form method="post">
	  Query:
	  <select name="table">
<%
		while (rs.next()) {
			String fqtn = rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME");
			out.print("<option value='" + fqtn + "' " +
					(fqtn.equals(request.getParameter("table")) ? "selected='selected'" : "") +
					">" + toHtml(rs.getString("TABLE_TYPE").substring(0, 1) + " " + fqtn) + "</option>");
		}
		rs.close();
%>
	  </select>
	  <input type="text" name="query" size="60" value="<%=request.getParameter("query") == null ? "" : toHtml(request.getParameter("query"))%>" />
	  Results:
	  <select name="limit">
		<option value="100" <%="100".equals(request.getParameter("limit")) ? "selected='selected'" : ""%>>100</option>
		<option value="250" <%="250".equals(request.getParameter("limit")) ? "selected='selected'" : ""%>>250</option>
		<option value="1000" <%="1000".equals(request.getParameter("limit")) ? "selected='selected'" : ""%>>1000</option>
	    <option value="" <%="".equals(request.getParameter("limit")) ? "selected='selected'" : ""%>>(no limit)</option>
	  </select>
	  <input type="submit" value="query" />
	</form>
	<br />
<%
		String query = request.getParameter("query");
		if (query == null || query.trim().equals("")) query = "SELECT * FROM " + request.getParameter("table");
		if (!query.endsWith("null")) {
%>
	Executing query: <%=toHtml(query)%><br />
	<table border="1">
	  <tr style="background-color: LightGrey;">
<%
		if (request.getParameter("limit") != null && !request.getParameter("limit").trim().equals("")) {
			try {
				int lim = Integer.parseInt(request.getParameter("limit"));
				st.setMaxRows(lim);
			} catch (NumberFormatException e) {
				// too bad...
			}
		}
		rs = st.executeQuery(query);
		// get meta data and print columns with their type
		ResultSetMetaData md = rs.getMetaData();
		for (int i = 1; i <= md.getColumnCount(); i++) {
%>
		<td><%=md.getColumnName(i) + "<br />(" + toHtml(md.getColumnTypeName(i)) + ")"%></td>
<%
		}
%>
	  </tr>
<%
		for (int i = 0; rs.next(); i++) {
%>
	  <tr>
<%
			for (int j = 1; j <= md.getColumnCount(); j++) {
%>
		<td><%=toHtml(rs.getString(j))%></td>
<%
			}
%>
	  </tr>
<%
		}
%>
	</table>
<%
		} else {
			out.print("Invalid query: " + query);
		}
	} catch (Exception e) {
		out.println(e);
		//e.printStackTrace(new PrintWriter(out));
	} finally {
		con.close();
	}
%>
