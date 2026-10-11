package org.justybase.parity;

import java.sql.*;
import java.math.*;
import java.time.*;
import java.util.*;

public final class Worker {
  static String quote(String s) { if (s == null) return "null"; return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""; }
  static String norm(Object v) throws Exception {
    if (v == null) return "null";
    if (v instanceof Blob b) return quote(HexFormat.of().formatHex(b.getBytes(1, Math.toIntExact(b.length()))));
    if (v instanceof Clob c) return quote(c.getSubString(1, Math.toIntExact(c.length())));
    if (v instanceof SQLXML x) return quote(x.getString());
    if (v instanceof byte[] b) return quote(HexFormat.of().formatHex(b));
    if (v instanceof BigDecimal d) return quote(d.toPlainString());
    if (v instanceof BigInteger i) return quote(i.toString());
    if (v instanceof Float || v instanceof Double) return quote(v.toString());
    if (v instanceof java.sql.Date d) return quote(d.toLocalDate().toString());
    if (v instanceof Time t) return quote(t.toLocalTime().toString().replace(':','.'));
    if (v instanceof Timestamp t) return quote(t.toLocalDateTime().toString().replace('T','-').replace(':','.'));
    if (v instanceof Boolean b) return b.toString();
    if (v instanceof Number n) return quote(n.toString());
    return quote(v.toString());
  }
  static String type(int t, String name) { return switch(t) { case Types.SMALLINT -> "SMALLINT"; case Types.INTEGER -> "INTEGER"; case Types.BIGINT -> "BIGINT"; case Types.DECIMAL, Types.NUMERIC -> "DECIMAL"; case Types.REAL, Types.FLOAT, Types.DOUBLE -> "FLOAT"; case Types.DATE -> "DATE"; case Types.TIME, Types.TIME_WITH_TIMEZONE -> "TIME"; case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> "TIMESTAMP"; case Types.BINARY -> "BINARY"; case Types.VARBINARY -> "VARBINARY"; case Types.BLOB -> "BLOB"; case Types.CLOB -> "CLOB"; case Types.NCLOB -> "DBCLOB"; case Types.BOOLEAN, Types.BIT -> "BOOLEAN"; default -> name.toUpperCase(Locale.ROOT); }; }
  public static void main(String[] args) throws Exception {
    String url=System.getenv("DB2_TEST_JDBC_URL"), user=System.getenv("DB2_TEST_USER"), pass=System.getenv("DB2_TEST_PASSWORD");
    if(url==null) throw new IllegalArgumentException("DB2_TEST_JDBC_URL is required");
    String tier=System.getenv().getOrDefault("DB2_PARITY_TIER","smoke"); String file=System.getenv().getOrDefault("DB2_PARITY_CASES","tests/parity/cases.tsv");
    String selected=args.length==2 && args[0].equals("--case")?args[1]:null; boolean all=args.length==1&&args[0].equals("--all"); if(!all&&selected==null) throw new IllegalArgumentException("usage: --case <id> | --all");
    Properties props=new Properties(); if(user!=null)props.setProperty("user",user); if(pass!=null)props.setProperty("password",pass);
    try(Connection c=DriverManager.getConnection(url,props)) {
      for(String line:java.nio.file.Files.readAllLines(java.nio.file.Path.of(file))) {
        if(line.isEmpty()||line.startsWith("#"))continue; String[] p=line.split("\\|",3); if(selected==null&&!tier.equals("full")&&!p[1].equals("smoke"))continue; if(selected!=null&&!selected.equals(p[0]))continue;
        // Use execute(), not executeQuery(): JCC classifies a statement with an
        // unrecognized leading keyword (for example the syntax-error case
        // "SELEC ...") as a non-query and throws a client-side -4476
        // "Method executeQuery cannot be used for update" without contacting
        // the server. execute() always sends the SQL, so the real server
        // SQLCODE/SQLSTATE (-104/42601) is surfaced consistently.
        try(Statement s=c.createStatement()) {
          s.execute(p[2]);
          try(ResultSet r=s.getResultSet()) {
          ResultSetMetaData m=r.getMetaData(); int cols=m.getColumnCount(); StringBuilder out=new StringBuilder("{\"case\":").append(quote(p[0])).append(",\"rows\":["); boolean firstRow=true;
          while(r.next()) { if(!firstRow)out.append(','); firstRow=false;out.append('[');for(int i=1;i<=cols;i++){if(i>1)out.append(',');out.append(norm(r.getObject(i)));}out.append(']'); }
          out.append("],\"metadata\":[");for(int i=1;i<=cols;i++){if(i>1)out.append(',');String t=type(m.getColumnType(i),m.getColumnTypeName(i));boolean numeric=t.equals("DECIMAL")||t.equals("DECFLOAT")||t.equals("NUMERIC");out.append("{\"type\":").append(quote(t)).append(",\"precision\":").append(numeric?m.getPrecision(i):"null").append(",\"scale\":").append(numeric?m.getScale(i):"null").append(",\"nullable\":").append(m.isNullable(i)!=ResultSetMetaData.columnNoNulls).append('}');}
          out.append("],\"sqlcode\":0,\"sqlstate\":\"00000\"}");System.out.println(out);
          }
        } catch(SQLException e) {
          if(!p[0].equals("invalid_sql")&&!p[0].equals("missing_table"))throw e;
          System.out.println("{\"case\":"+quote(p[0])+",\"rows\":[],\"metadata\":[],\"sqlcode\":"+e.getErrorCode()+",\"sqlstate\":"+quote(e.getSQLState())+"}");
        }
      }
      if(args.length==1&&args[0].equals("--all")) {
      String schema=System.getenv("DB2_IT_SCHEMA"); if(schema==null)throw new IllegalArgumentException("DB2_IT_SCHEMA is required"); String table="\""+schema+"\".PARITY_DML";
      try(Statement s=c.createStatement()){s.executeUpdate("DROP TABLE "+table);}catch(SQLException ignored){}
      try(Statement s=c.createStatement()){s.executeUpdate("CREATE TABLE "+table+" (ID INTEGER NOT NULL PRIMARY KEY, AMOUNT DECIMAL(12,3), TXT VARCHAR(40))");}
      int[] counts=new int[7];
      try(PreparedStatement p=c.prepareStatement("INSERT INTO "+table+" VALUES (?, ?, ?)")){p.setInt(1,1);p.setBigDecimal(2,new BigDecimal("123.456"));p.setString(3,"Zażółć 東京");counts[0]=p.executeUpdate();}
      try(Statement s=c.createStatement()){counts[1]=s.executeUpdate("INSERT INTO "+table+" VALUES (2, -0.125, 'two'), (3, 0.000, '')");counts[2]=s.executeUpdate("UPDATE "+table+" SET TXT='x' WHERE ID=99");counts[3]=s.executeUpdate("UPDATE "+table+" SET TXT='one' WHERE ID=1");counts[4]=s.executeUpdate("UPDATE "+table+" SET AMOUNT=AMOUNT WHERE ID<=2");counts[5]=s.executeUpdate("DELETE FROM "+table+" WHERE ID=99");counts[6]=s.executeUpdate("DELETE FROM "+table+" WHERE ID<=2");}
      System.out.println("{\"case\":\"dml_prepared_counts\",\"rows\":[["+String.join(",",Arrays.stream(counts).mapToObj(Integer::toString).toList())+"]],\"metadata\":[],\"sqlcode\":0,\"sqlstate\":\"00000\"}");
      try(Statement s=c.createStatement();ResultSet r=s.executeQuery("SELECT ID, AMOUNT, TXT FROM "+table+" ORDER BY ID")){ResultSetMetaData m=r.getMetaData();StringBuilder o=new StringBuilder("{\"case\":\"dml_final_state\",\"rows\":[");boolean fr=true;while(r.next()){if(!fr)o.append(',');fr=false;o.append('[');for(int i=1;i<=m.getColumnCount();i++){if(i>1)o.append(',');o.append(norm(r.getObject(i)));}o.append(']');}System.out.println(o.append("],\"metadata\":[],\"sqlcode\":0,\"sqlstate\":\"00000\"}").toString());}
      int invisible,rolledBack,visible;
      c.setAutoCommit(false);try(Statement s=c.createStatement()){s.executeUpdate("INSERT INTO "+table+"(ID) VALUES (4)");}
      try(Connection observer=DriverManager.getConnection(url,props);Statement s=observer.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+table+" WHERE ID=4")){r.next();invisible=r.getInt(1);}c.rollback();
      try(Connection observer=DriverManager.getConnection(url,props);Statement s=observer.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+table+" WHERE ID=4")){r.next();rolledBack=r.getInt(1);}
      try(Statement s=c.createStatement()){s.executeUpdate("INSERT INTO "+table+"(ID) VALUES (4)");}c.commit();
      try(Connection observer=DriverManager.getConnection(url,props);Statement s=observer.createStatement();ResultSet r=s.executeQuery("SELECT COUNT(*) FROM "+table+" WHERE ID=4")){r.next();visible=r.getInt(1);}c.setAutoCommit(true);
      System.out.println("{\"case\":\"transaction_visibility\",\"rows\":[["+invisible+","+rolledBack+","+visible+"]],\"metadata\":[],\"sqlcode\":0,\"sqlstate\":\"00000\"}");
      try(Statement s=c.createStatement()){s.executeUpdate("DROP TABLE "+table);}
      }
    }
  }
}
