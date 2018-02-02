using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.CommandLineUtils;
using Serilog;

namespace mysql2sqlite
{
    class ForeignKey
    {
        public ForeignKey()
        {
            columns = new List<string>();
            refColumns = new List<string>();
        }

        public string name;
        public string refTable;
        public List<string> columns;
        public List<string> refColumns;
    }

    class Column
    {
        public string name;
        public string type;
        public bool nullable;
        public bool autoincrement;
    }

    class Index
    {
        public Index()
        {
            columns = new List<string>();
        }

        public string name;
        public List<string> columns;
    }

    class Table
    {
        public Table()
        {
            columns = new Dictionary<string, Column>();
            indexes = new Dictionary<string, Index>();
            foreignKeys = new Dictionary<string, ForeignKey>();
        }

        public string name;
        public Dictionary<string, Column> columns;
        public Dictionary<string, Index> indexes;
        public Index primaryKey;
        public Dictionary<string, ForeignKey> foreignKeys;
    }

    class Program
    {
        static int Main(string[] args)
        {
            var app = new CommandLineApplication
            {
                Name = "dbcopy"
            };
            app.HelpOption("-?|-h|--help");

            app.Command("copy", (command) =>
            {
                command.Description = "Copy one database to another!";
                command.HelpOption("-?|-h|--help");

                var sourceOption = command.Option("-s|--source <connectionString>",
                                        "Specify source connection.",
                                        CommandOptionType.SingleValue);

                var sourceTypeOption = command.Option("-st|--source-type <type>",
                                        "Specify source type connection. (mysql) only by now.",
                                        CommandOptionType.SingleValue);

                var destOption = command.Option("-d|--destination <connectionString>",
                                        "Specify destination connection.",
                                        CommandOptionType.SingleValue);

                var destTypeOption = command.Option("-dt|--destination-type <type>",
                                        "Specify destination type connection. (sqlite) only by now.",
                                        CommandOptionType.SingleValue);

                var loadDataOption = command.Option("-L|--load-data",
                        "Load data from source to destination.",
                        CommandOptionType.NoValue);

                var logInfoOption = command.Option("-v",
                        "Log level information.",
                        CommandOptionType.NoValue);


                command.OnExecute(() =>
                {
                    var logConf = new LoggerConfiguration();
                    if (logInfoOption.HasValue())
                    {
                        logConf = logConf.MinimumLevel.Verbose();
                    }

                    Log.Logger = logConf
                        .Enrich.FromLogContext()
                        .WriteTo.Console()
                        .CreateLogger();

                    var source = sourceOption.Value();
                    var destination = destOption.Value();
                    var load = loadDataOption.HasValue();

                    DbConnection connection;
                    DbConnection connection2;

                    Log.Information("Opening source database connection.");
                    connection = new MySqlConnection(source);
                    connection.Open();
                    Log.Information("Done.");

                    Log.Information("Opening destination database connection.");
                    connection2 = new SqliteConnection(destination);
                    connection2.Open();
                    Log.Information("Done.");

                    var tables = LoadTablesInfo(connection);

                    LoadIndexes(connection, tables);

                    LoadForeignKeys(connection, tables);

                    DumpTo(tables, connection2);

                    if (load)
                    {
                        LoadData(tables, connection, connection2);
                    }

                    return 0;

                });
            });

            return app.Execute(args);
        }

        private static void LoadData(Dictionary<string, Table> tables, DbConnection connection, DbConnection connection2)
        {

            if (connection2 is SqliteConnection)
            {
                LoadDataToSQLite(tables, connection, connection2);
            }
        }

        private static void LoadDataToSQLite(Dictionary<string, Table> tables, DbConnection connection, DbConnection connection2)
        {
            var cmdPragma = connection2.CreateCommand();
            cmdPragma.CommandText = @"
                PRAGMA foreign_keys = OFF;
                PRAGMA synchronous = OFF;
                PRAGMA journal_mode = MEMORY;
            ";

            cmdPragma.ExecuteNonQuery();
            cmdPragma = null;

            foreach (var t in tables.Values)
            {
                var cmd = "SELECT ";
                var cmd2 = "INSERT ";

                bool first = true;
                foreach (var c in t.columns.Values)
                {
                    cmd += (first ? "" : ", ") + "`" + c.name + "`";
                    first = false;
                }
                cmd += " FROM " + t.name;

                var command = connection.CreateCommand();
                command.CommandText = cmd;

                Log.Verbose(cmd);

                var insertCommand = connection2.CreateCommand();
                cmd2 = "INSERT INTO " + t.name + " VALUES (";

                int ix = 0;

                foreach (var c in t.columns.Values)
                {
                    var pname = "$p" + ix;

                    cmd2 += (ix == 0 ? "" : ", ") + pname;
                    var parameter = insertCommand.CreateParameter();
                    parameter.ParameterName = pname;

                    var type = c.type.ToUpper();

                    parameter.DbType = System.Data.DbType.String;
                    if (type.StartsWith("INT")) parameter.DbType = System.Data.DbType.Int64;
                    if (type.StartsWith("BOOL")) parameter.DbType = System.Data.DbType.Int64;
                    if (type.StartsWith("DECIMAL")) parameter.DbType = System.Data.DbType.Decimal;
                    if (type.StartsWith("DATETIME")) parameter.DbType = System.Data.DbType.DateTime;
                    if (type.StartsWith("ENUM")) parameter.DbType = System.Data.DbType.String;
                    if (type.StartsWith("SET")) parameter.DbType = System.Data.DbType.String;
                    if (type.Contains("BLOB")) parameter.DbType = System.Data.DbType.Binary;
                    insertCommand.Parameters.Add(parameter);
                    ix++;
                }
                cmd2 += ");";
                insertCommand.CommandText = cmd2;

                Log.Verbose(cmd2);

                var reader = command.ExecuteReader();

                int count = 0;
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        for (ix = 0; ix < t.columns.Count; ix++)
                        {
                            var pname = "$p" + ix;
                            insertCommand.Parameters[pname].Value = reader[ix];
                        }

                        insertCommand.ExecuteNonQuery();
                        
                        Console.Write("#");
                        count++;
                    }
                    Console.WriteLine();
                    Log.Information("{tname} - {rowcount} rows inserted.", t.name, count);
                }

                reader.Close();
            }
        }

        private static void DumpTo(Dictionary<string, Table> tables, DbConnection connection)
        {
            Log.Information("Creating tables in destination.");
            if (connection is SqliteConnection)
            {
                DumpToSQLite(tables, connection);
            }
            Log.Information("Done.");
        }

        private static void DumpToSQLite(Dictionary<string, Table> tables, DbConnection connection)
        {
            bool first;
            foreach (var t in tables.Values)
            {
                string command = "";
                command += "CREATE TABLE `" + t.name + "` \n";
                command += "( \n";

                first = true;

                foreach (var c in t.columns.Values)
                {
                    string add = "";

                    if (t.primaryKey != null && t.primaryKey.columns.Contains(c.name) && c.autoincrement)
                    {
                        add += "PRIMARY KEY";

                        if (t.primaryKey.columns.Count > 1)
                        {
                            t.primaryKey.columns.RemoveAll(colName => colName != c.name);
                            Log.Error("WARNING!!!!!! MULTIPLE PK WITH AUTOINCREMENT!!!!!!! - {tablename}", t.name);
                        }
                    }

                    string type = c.type.ToUpper();
                    type = type.Replace("UNSIGNED", "");
                    if (type.StartsWith("INT") || type.StartsWith("BIT") || type.Contains("INT("))
                        type = "integer";

                    if (type.StartsWith("ENUM") || type.StartsWith("SET"))
                        type = "text";

                    command += (first ? "  " : ", ") + "`" + c.name + "`" + " " + type + " " + (c.nullable ? "NULL" : "NOT NULL") + " " + add + " " + (c.autoincrement ? "AUTOINCREMENT" : "") + "\n";
                    first = false;
                }

                if (t.primaryKey != null && t.primaryKey.columns.Count > 1)
                {
                    command += ", PRIMARY KEY (";

                    first = true;
                    foreach (var column in t.primaryKey.columns)
                    {
                        command += (first ? "" : ", ") + "`" + column + "`";
                        first = false;
                    }

                    command += ")\n";
                }

                foreach (var fk in t.foreignKeys.Values)
                {
                    command += ", CONSTRAINT `" + fk.name + "` FOREIGN KEY (";
                    first = true;
                    foreach (var col in fk.columns)
                    {
                        command += (first ? "" : ", ") + "`" + col + "`";
                        first = false;
                    }
                    command += ") REFERENCES `" + fk.refTable + "` (";
                    first = true;
                    foreach (var col in fk.columns)
                    {
                        command += (first ? "" : ", ") + "`" + col + "`";
                        first = false;
                    }
                    command += ")\n";
                }

                command += ");\n";

                Log.Verbose(command.ToString());

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = command.ToString();
                    cmd.ExecuteNonQuery();
                }
            }

            foreach (var t in tables.Values)
            {
                foreach (var i in t.indexes.Values)
                {
                    string ixName = i.name + "_" + t.name;
                    string command = "";
                    command += "CREATE INDEX `" + ixName + "` ON `" + t.name + "` (\n";

                    first = true;
                    for (int ix = 0; ix < i.columns.Count; ix++)
                    {
                        var columnName = i.columns[ix];
                        command += (first ? "" : ", ") + "`" + columnName + "`";
                        first = false;
                    }
                    command += ");\n";

                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = command.ToString();
                        cmd.ExecuteNonQuery();
                    }

                    Log.Verbose(command.ToString());
                }
            }
        }

        private static void LoadForeignKeys(DbConnection connection, Dictionary<string, Table> tables)
        {
            /*
            CONSTRAINT_CATALOG = def
            CONSTRAINT_SCHEMA = thundersclient01
            CONSTRAINT_NAME = FK_contracts_agents_BranchOfficeClientId
            TABLE_CATALOG = def
            TABLE_SCHEMA = thundersclient01
            TABLE_NAME = contracts
            COLUMN_NAME = BranchOfficeClientId
            ORDINAL_POSITION = 1
            POSITION_IN_UNIQUE_CONSTRAINT = 1
            REFERENCED_TABLE_SCHEMA = thundersclient01
            REFERENCED_TABLE_NAME = agents
            REFERENCED_COLUMN_NAME = AgentId
             */
            Log.Information("Loading foreign key information.");
            using (var datatable = connection.GetSchema("Foreign Key Columns"))
            {
                //DisplayData(datatable);

                foreach (System.Data.DataRow row in datatable.Rows)
                {
                    var fkName = row["CONSTRAINT_NAME"].ToString();
                    var tableName = row["TABLE_NAME"].ToString();
                    var columnName = row["COLUMN_NAME"].ToString();
                    var refTableName = row["REFERENCED_TABLE_NAME"].ToString();
                    var refColumnName = row["REFERENCED_COLUMN_NAME"].ToString();

                    var table = tables[tableName];

                    ForeignKey fk = null;
                    if (table.foreignKeys.ContainsKey(refTableName))
                        fk = table.foreignKeys[fkName];
                    else
                        fk = new ForeignKey { name = fkName };

                    fk.refTable = refTableName;

                    fk.columns.Add(columnName);
                    fk.refColumns.Add(refColumnName);

                    table.foreignKeys[fkName] = fk;
                }
            }
            Log.Information("Done.");
        }

        private static void LoadIndexes(DbConnection connection, Dictionary<string, Table> tables)
        {
            /*
            INDEX_CATALOG =
            INDEX_SCHEMA = thundersclient01
            INDEX_NAME = PRIMARY
            TABLE_NAME = actualfinancialguarantee
            COLUMN_NAME = ActualFinancialGuaranteeId
            ORDINAL_POSITION = 1
            SORT_ORDER = A
            */
            Log.Information("Loading indexes information.");
            using (var datatable = connection.GetSchema("IndexColumns"))
            {
                //DisplayData(datatable);

                foreach (System.Data.DataRow row in datatable.Rows)
                {
                    var tableName = row["TABLE_NAME"].ToString();
                    var indexName = row["INDEX_NAME"].ToString();
                    var columnName = row["COLUMN_NAME"].ToString();

                    var table = tables[tableName];

                    Index index = null;

                    if ("PRIMARY".Equals(indexName))
                        index = table.primaryKey;
                    else
                        table.indexes.TryGetValue(indexName, out index);

                    if (index == null) index = new Index { name = indexName };

                    index.columns.Add(columnName);

                    if ("PRIMARY".Equals(indexName))
                        table.primaryKey = index;
                    else
                        table.indexes[indexName] = index;

                }
            }
            Log.Information("Done.");
        }

        private static Dictionary<string, Table> LoadTablesInfo(DbConnection connection)
        {
            
            Dictionary<string, Table> tables = new Dictionary<string, Table>();

            /*
            TABLE_CATALOG = def
            TABLE_SCHEMA = thundersclient01
            TABLE_NAME = utilizationunits
            COLUMN_NAME = UtilizationValueUnit
            ORDINAL_POSITION = 4
            COLUMN_DEFAULT =
            IS_NULLABLE = NO
            DATA_TYPE = decimal
            CHARACTER_MAXIMUM_LENGTH =
            NUMERIC_PRECISION = 18
            NUMERIC_SCALE = 2
            DATETIME_PRECISION =
            CHARACTER_SET_NAME =
            COLLATION_NAME =
            COLUMN_TYPE = decimal(18,2)
            COLUMN_KEY =
            EXTRA =
            PRIVILEGES = select,insert,update,references
            COLUMN_COMMENT =
             */
            Log.Information("Loading tables definition");

            using (var datatable = connection.GetSchema("Columns"))
            {
                //DisplayData(datatable);

                foreach (System.Data.DataRow row in datatable.Rows)
                {
                    Table table = null;
                    string tableName = row["TABLE_NAME"].ToString();

                    if (!tables.ContainsKey(tableName))
                    {
                        table = new Table
                        {
                            name = tableName
                        };
                        tables[tableName] = table;
                    }
                    else
                    {
                        table = tables[tableName];
                    }

                    table.columns.Add(row["COLUMN_NAME"].ToString(), new Column
                    {
                        name = row["COLUMN_NAME"].ToString(),
                        type = row["COLUMN_TYPE"].ToString(),
                        nullable = row["IS_NULLABLE"].ToString().Equals("NO") ? false : true,
                        autoincrement = "auto_increment".Equals(row["EXTRA"].ToString())
                    });
                }
            }

            Log.Information("Done");
            return tables;
        }

        private static void DisplayData(System.Data.DataTable table)
        {
            foreach (System.Data.DataRow row in table.Rows)
            {
                foreach (System.Data.DataColumn col in table.Columns)
                {
                    Log.Verbose("{0} = {1}", col.ColumnName, row[col]);
                }
                Log.Verbose("============================");
            }
        }
    }


}
