using Microsoft.Extensions.Configuration;
using System;
using System.Collections;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;

namespace ConnectionSamples
{
    class Program
    {
        static void Main(string[] args)
        {
            Task.Run(async () => await TransactionSample()).Wait();
        }

        public static void OpenConnection()
        {
            string connectionString = @"server=(localdb)\MSSQLLocalDB;" + 
                "integrated security=SSPI;database=Books";
            var connection = new SqlConnection(connectionString);
            connection.Open();
            Console.WriteLine("Connection opened");
            // Do some job
            connection.Close();
        }

        public static string GetConnectionString()
        {
            var configurationBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config.json");
            IConfiguration configuration = configurationBuilder.Build();
            string connectionString = configuration["Data:DefaultConnection:ConnectionString"];
            Console.WriteLine(connectionString);
            return connectionString;
        }

        public static void ShowStatistic(IDictionary statistic)
        {
            Console.WriteLine("Statistic");
            foreach(var key in statistic.Keys)
            {
                Console.WriteLine($"{key}: {statistic[key]}");
            }
        }

        public static void ConnectionInformation()
        {
            using(var connection = new SqlConnection(GetConnectionString()))
            {
                connection.InfoMessage += (sender, e) =>
                {
                    Console.WriteLine($"warning or info {e.Message}");
                };

                connection.StateChange += (sender, e) =>
                {
                    Console.WriteLine($"Previous state: {e.OriginalState}, " +
                        $"Current state: {e.CurrentState}");
                };

                try
                {
                    connection.StatisticsEnabled = true;
                    connection.FireInfoMessageEventOnUserErrors = true;
                    connection.Open();
                    // Read some records
                    IDictionary statistic = connection.RetrieveStatistics();
                    ShowStatistic(statistic);
                    connection.ResetStatistics();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        public static void CreateCommand()
        {
            using(var connection = new SqlConnection(GetConnectionString()))
            {
                string sql = "SELECT * from [ProCSharp].[Books] WHERE lower([Title]) LIKE @Title";
                var command = new SqlCommand(sql, connection);
                command.Parameters.AddWithValue("Title", "Professional C#%");
                connection.Open();
            }
        }

        public static void ExecuteNonQuery()
        {
            try
            {
                using (var connection = new SqlConnection(GetConnectionString()))
                {
                    string sql = "INSERT INTO [ProCSharp].[Books] ([Title], [Publisher], [Isbn], [ReleaseDate]) " +
                        "VALUES (@Title, @Publisher, @Isbn, @ReleaseDate)";

                    var command = new SqlCommand(sql, connection);
                    command.Parameters.AddWithValue("Title", "Professional C# 7 and .NET Core 2.0");
                    command.Parameters.AddWithValue("Publisher", "Wrox Press");
                    command.Parameters.AddWithValue("Isbn", " 978-1119449270");
                    command.Parameters.AddWithValue("ReleaseDate", new DateTime(2018, 4, 2));

                    connection.Open();
                    int records = command.ExecuteNonQuery();
                    Console.WriteLine($"{records} record(s) inserted");
                }
            }
            catch (SqlException ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        public static void ExecuteScalar()
        {
            using(var connection = new SqlConnection(GetConnectionString()))
            {
                string sql = "SELECT COUNT(*) FROM ProCSharp.Books";
                var command = connection.CreateCommand();
                command.CommandText = sql;
                connection.Open();
                object count = command.ExecuteScalar();
                Console.WriteLine($"{count} books in db");
            }
        }

        public static void ExecuteReader()
        {
            var connection = new SqlConnection(GetConnectionString());
            string sql = "SELECT * FROM ProCSharp.Books";
            var command = new SqlCommand(sql, connection);

            connection.Open();
            using(SqlDataReader reader = command.ExecuteReader(CommandBehavior.CloseConnection))
            {
                while (reader.Read())
                {
                    int id = (int)reader["Id"];
                    string title = (string)reader["Title"];
                    string publisher = (string)reader["Publisher"];
                    DateTime? releaseDate = (DateTime?)reader["ReleaseDate"];
                    Console.WriteLine($"{id, 5}. {title, -50} {publisher, -25} {releaseDate:d}");
                }
            }
        }

        public static void StoredProcedure()
        {
            using(var connection = new SqlConnection(GetConnectionString()))
            {
                SqlCommand command = connection.CreateCommand();
                command.CommandText = "[ProCSharp].[GetBooksByPublisher]";
                command.CommandType = CommandType.StoredProcedure;
                var parameter = command.CreateParameter();
                parameter.SqlDbType = SqlDbType.NVarChar;
                parameter.ParameterName = "@Publisher";
                parameter.Value = "Wrox Press";
                command.Parameters.Add(parameter);
                connection.Open();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        int recursionLevel = (int)reader["Id"];
                        string title = (string)reader["Title"];
                        string pub = (string)reader["Publisher"];
                        DateTime? releaseDate = (DateTime?)reader["ReleaseDate"];
                        Console.WriteLine($"{title, -50} - {pub}; {releaseDate:d}");
                    }
                }
            }
        }

        public static async Task ReadAsync()
        {
            var connection = new SqlConnection(GetConnectionString());
            SqlCommand command = connection.CreateCommand();
            command.CommandText = "[ProCSharp].[GetBooksByPublisher]";
            command.CommandType = CommandType.StoredProcedure;
            var parameter = command.CreateParameter();
            parameter.SqlDbType = SqlDbType.NVarChar;
            parameter.ParameterName = "@Publisher";
            parameter.Value = "Wrox Press";
            command.Parameters.Add(parameter);
            await connection.OpenAsync();
            using (SqlDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.CloseConnection))
            {
                while (await reader.ReadAsync())
                {
                    int recursionLevel = (int)reader["Id"];
                    string title = (string)reader["Title"];
                    string pub = (string)reader["Publisher"];
                    DateTime? releaseDate = (DateTime?)reader["ReleaseDate"];
                    Console.WriteLine($"{title,-50} - {pub}; {releaseDate:d}");
                }
            }
        }

        public static async Task TransactionSample()
        {
            using(var connection = new SqlConnection(GetConnectionString()))
            {
                await connection.OpenAsync();
                SqlTransaction transaction = connection.BeginTransaction();

                try
                {
                    string sql = "INSERT INTO [ProCSharp].[Books] " +
                        "([Title], [Publisher], [Isbn], [ReleaseDate]) " +
                        "VALUES (@Title, @Publisher, @Isbn, @ReleaseDate);" +
                        "SELECT SCOPE_IDENTITY()";
                    var command = new SqlCommand(sql, connection, transaction);

                    var p1 = new SqlParameter("Title", SqlDbType.NVarChar, 50);
                    var p2 = new SqlParameter("Publisher", SqlDbType.NVarChar, 50);
                    var p3 = new SqlParameter("Isbn", SqlDbType.NVarChar, 20);
                    var p4 = new SqlParameter("ReleaseDate", SqlDbType.Date);
                    command.Parameters.AddRange(new SqlParameter[] { p1, p2, p3, p4 });

                    command.Parameters["Title"].Value = "Roka";
                    command.Parameters["Publisher"].Value = "Toch";
                    command.Parameters["Isbn"].Value = "42123";
                    command.Parameters["ReleaseDate"].Value = new DateTime(2000, 10, 8);

                    object id = await command.ExecuteScalarAsync();
                    Console.WriteLine($"Record with id {id} added");

                    command.Parameters["Title"].Value = "Programmer";
                    command.Parameters["Publisher"].Value = "Toch";
                    command.Parameters["Isbn"].Value = "42123";
                    command.Parameters["ReleaseDate"].Value = new DateTime(2010, 10, 8);

                    id = await command.ExecuteScalarAsync();
                    Console.WriteLine($"Record with id {id} added");

                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"error {ex.Message}, rolling back");
                    transaction.Rollback();
                }
            }
        }
    }
}
