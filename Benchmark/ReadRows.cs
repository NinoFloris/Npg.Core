using BenchmarkDotNet.Attributes;
using Npg.Core.Raw;
using Npgsql;
using System.Net;
using System.Threading.Tasks;
using BenchmarkDotNet.Engines;

namespace Benchmark
{
    [SimpleJob(RunStrategy.Throughput, warmupCount: 1, targetCount: 20)]
    public class ReadRows
    {
        const string PostgresUserPassword = "postgres123";
        const string DefaultConnectionString = $"Server=127.0.0.1;User ID=postgres;Password={PostgresUserPassword};Database=postgres;SSL Mode=Disable;Pooling=false;Max Auto Prepare=0;";

        [Params(1, 100, 1000, 8000)]
        public int NumRows { get; set; }

        NpgsqlCommand Command { get; set; } = default!;

        string RawQuery = string.Empty;
        public PipePgDB PipeRawDB;

        [GlobalSetup(Target = nameof(ReadNpgsql))]
        public void SetupNpgsql()
        {
            var conn = new NpgsqlConnection(DefaultConnectionString);
            conn.Open();
            Command = new NpgsqlCommand($"SELECT generate_series(1, {NumRows})", conn);
            //Command.Prepare();
        }

        [GlobalSetup(Targets = new[] { nameof(ReadPipeRawExtended) })]
        public void SetupPipeRaw()
        {
            var endpoint = IPEndPoint.Parse("127.0.0.1:5432");
            PipeRawDB = PipePgDB.OpenAsync(endpoint, "postgres", PostgresUserPassword, "postgres").GetAwaiter().GetResult();
            // PipeRawDB.ExecuteExtendedAsync(RawQuery).GetAwaiter().GetResult();
            RawQuery = $"SELECT generate_series(1, {NumRows})";
        }

        [Benchmark]
        public async ValueTask ReadNpgsql()
        {
            await using var reader = await Command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {

            }
        }

        [Benchmark]
        public async ValueTask ReadPipeRawExtended()
        {
            await PipeRawDB.ExecuteExtendedAsync(RawQuery);
            while (!TryReadUntilCode(BackendMessageCode.ReadyForQuery, out var messagesRead))
            {
                await PipeRawDB.MoveNextAsync();
            }
            
            bool TryReadUntilCode(BackendMessageCode messageCode, out int messagesRead)
            {
                messagesRead = 0;
                if (!PipeRawDB.TryGetMessageReader(out var reader))
                    return false;

                BackendMessageCode? lastCode = null;
                while (reader.MoveNext() && lastCode != messageCode)
                {
                    lastCode = reader.Current.Code;
                    messagesRead++;
                }

                PipeRawDB.Advance(reader.Consumed);
                return lastCode == messageCode;
            }
        }
    }
}
