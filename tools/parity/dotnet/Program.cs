using System.Data.Common;
using System.Globalization;
using System.Text.Json;
using IBM.Data.Db2;

var dsn = Environment.GetEnvironmentVariable("DB2_TEST_DOTNET_CONNECTION_STRING") ?? throw new Exception("DB2_TEST_DOTNET_CONNECTION_STRING is required");
var casesPath = Environment.GetEnvironmentVariable("DB2_PARITY_CASES") ?? "tests/parity/cases.tsv";
var tier = Environment.GetEnvironmentVariable("DB2_PARITY_TIER") ?? "smoke";
var selected = args.Length == 2 && args[0] == "--case" ? args[1] : null;
var all = args.Length == 1 && args[0] == "--all";
if (!all && selected is null) throw new Exception("usage: --case <id> | --all");
var cases = File.ReadLines(casesPath).Where(x => x.Length > 0 && !x.StartsWith('#'))
    .Select(x => x.Split('|', 3)).Where(x => (selected is not null || tier == "full" || x[1] == "smoke") && (selected is null || x[0] == selected));
using var connection = new DB2Connection(dsn);
connection.Open();
foreach (var item in cases) {
    try {
    using var command = connection.CreateCommand(); command.CommandText = item[2];
    using var reader = command.ExecuteReader();
    var rows = new List<List<object?>>();
    while (reader.Read()) { var row = new List<object?>(); for (var i=0;i<reader.FieldCount;i++) { var type=CanonicalType(reader.GetDataTypeName(i)); var raw=reader.IsDBNull(i)?null:type is "CLOB" or "DBCLOB" or "DECIMAL" or "DECFLOAT" ? reader.GetString(i) : reader.GetValue(i); row.Add(Normalize(raw,type)); } rows.Add(row); }
    var metadata = Enumerable.Range(0, reader.FieldCount).Select(i => { var type=CanonicalType(reader.GetDataTypeName(i)); var numeric=type is "DECIMAL" or "NUMERIC" or "DECFLOAT"; return new { type, precision=numeric?SchemaValue(reader,i,"NumericPrecision"):null, scale=numeric?SchemaValue(reader,i,"NumericScale"):null, nullable=SchemaValue(reader,i,"AllowDBNull") }; }).ToArray();
    Console.WriteLine(JsonSerializer.Serialize(new { @case=item[0], rows, metadata, sqlcode=0, sqlstate="00000" }));
    } catch (DB2Exception ex) when (item[0] == "invalid_sql" || item[0] == "missing_table") {
        var error=ex.Errors.Count>0 ? ex.Errors[0] : null;
        Console.WriteLine(JsonSerializer.Serialize(new { @case=item[0], rows=Array.Empty<object>(), metadata=Array.Empty<object>(), sqlcode=error?.NativeError, sqlstate=error?.SQLState }));
    }
}
if (all) {
var schema=Environment.GetEnvironmentVariable("DB2_IT_SCHEMA") ?? throw new Exception("DB2_IT_SCHEMA is required");
var table=$"\"{schema}\".PARITY_DML";
try { using var drop=connection.CreateCommand(); drop.CommandText=$"DROP TABLE {table}"; drop.ExecuteNonQuery(); } catch { }
using (var create=connection.CreateCommand()) { create.CommandText=$"CREATE TABLE {table} (ID INTEGER NOT NULL PRIMARY KEY, AMOUNT DECIMAL(12,3), TXT VARCHAR(40))"; create.ExecuteNonQuery(); }
int Insert(int id, decimal amount, string txt) { using var cmd=connection.CreateCommand(); cmd.CommandText=$"INSERT INTO {table} VALUES (?, ?, ?)"; foreach(var value in new object[]{id,amount,txt}) { var p=cmd.CreateParameter(); p.Value=value; cmd.Parameters.Add(p); } return cmd.ExecuteNonQuery(); }
var counts=new List<int>{Insert(1,123.456m,"Zażółć 東京")};
int Exec(string sql) { using var cmd=connection.CreateCommand(); cmd.CommandText=sql; return cmd.ExecuteNonQuery(); }
counts.Add(Exec($"INSERT INTO {table} VALUES (2, -0.125, 'two'), (3, 0.000, '')"));
counts.Add(Exec($"UPDATE {table} SET TXT='x' WHERE ID=99")); counts.Add(Exec($"UPDATE {table} SET TXT='one' WHERE ID=1"));
counts.Add(Exec($"UPDATE {table} SET AMOUNT=AMOUNT WHERE ID<=2")); counts.Add(Exec($"DELETE FROM {table} WHERE ID=99")); counts.Add(Exec($"DELETE FROM {table} WHERE ID<=2"));
Console.WriteLine(JsonSerializer.Serialize(new { @case="dml_prepared_counts", rows=new[]{counts.Cast<object?>().ToArray()}, metadata=Array.Empty<object>(), sqlcode=0, sqlstate="00000" }));
using (var query=connection.CreateCommand()) { query.CommandText=$"SELECT ID, AMOUNT, TXT FROM {table} ORDER BY ID"; using var reader=query.ExecuteReader(); var rows=new List<List<object?>>(); while(reader.Read()){var row=new List<object?>();for(int i=0;i<reader.FieldCount;i++)row.Add(Normalize(reader.GetValue(i),CanonicalType(reader.GetDataTypeName(i))));rows.Add(row);} Console.WriteLine(JsonSerializer.Serialize(new { @case="dml_final_state", rows, metadata=Array.Empty<object>(), sqlcode=0, sqlstate="00000" })); }
int ReadCount() { using var observer=new DB2Connection(dsn); observer.Open(); using var cmd=observer.CreateCommand(); cmd.CommandText=$"SELECT COUNT(*) FROM {table} WHERE ID=4"; return Convert.ToInt32(cmd.ExecuteScalar(),CultureInfo.InvariantCulture); }
using (var tx=connection.BeginTransaction()) { using var cmd=connection.CreateCommand(); cmd.Transaction=tx;cmd.CommandText=$"INSERT INTO {table}(ID) VALUES (4)";cmd.ExecuteNonQuery(); var invisible=ReadCount(); tx.Rollback(); var rolledBack=ReadCount(); using var tx2=connection.BeginTransaction(); using var cmd2=connection.CreateCommand();cmd2.Transaction=tx2;cmd2.CommandText=$"INSERT INTO {table}(ID) VALUES (4)";cmd2.ExecuteNonQuery();tx2.Commit();var visible=ReadCount();Console.WriteLine(JsonSerializer.Serialize(new { @case="transaction_visibility",rows=new[]{new[]{invisible,rolledBack,visible}},metadata=Array.Empty<object>(),sqlcode=0,sqlstate="00000" })); }
Exec($"DROP TABLE {table}");
}

static object? Normalize(object? value, string type) => value switch {
    null or DBNull => null,
    bool b => b,
    byte[] bytes => Convert.ToHexString(bytes).ToLowerInvariant(),
    decimal d => d.ToString(CultureInfo.InvariantCulture),
    double d => d.ToString("R", CultureInfo.InvariantCulture),
    float f => f.ToString("R", CultureInfo.InvariantCulture),
    DateTime d when type == "DATE" => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
    DateTime d when type == "TIME" => d.ToString("HH.mm.ss", CultureInfo.InvariantCulture),
    DateTime d when type == "TIMESTAMP" => d.ToString("yyyy-MM-dd-HH.mm.ss.ffffff", CultureInfo.InvariantCulture).TrimEnd('0').TrimEnd('.'),
    TimeSpan t when type == "TIME" => t.ToString(@"hh\.mm\.ss", CultureInfo.InvariantCulture),
    _ => Convert.ToString(value, CultureInfo.InvariantCulture)
};
static object? SchemaValue(DbDataReader reader, int ordinal, string field) { try { var table=reader.GetSchemaTable(); if (table is null || ordinal>=table.Rows.Count || table.Rows[ordinal][field] is DBNull) return null; return table.Rows[ordinal][field]; } catch { return null; } }
static string CanonicalType(string type) => type.ToUpperInvariant() switch { "INT"=>"INTEGER", "DECIMAL"=>"DECIMAL", "NUMERIC"=>"DECIMAL", "DOUBLE"=>"FLOAT", "DOUBLE PRECISION"=>"FLOAT", "REAL"=>"FLOAT", _=>type.ToUpperInvariant() };
