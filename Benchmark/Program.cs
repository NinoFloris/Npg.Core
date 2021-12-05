using System;
using System.Collections.Generic;
using System.Diagnostics;
using BenchmarkDotNet.Running;
using System.Reflection;
using System.Threading.Tasks;

namespace Benchmark
{
    class Program
    {
        static async Task Main(string[] args)
        {
            const bool bdn = true;
            if (bdn)
            {
                new BenchmarkSwitcher(typeof(Program).GetTypeInfo().Assembly).Run(args);
                return;
            }
            
            const bool runNpgsql = true;
            var rr = new ReadRows();
            rr.NumRows = 1000;
            
            if (runNpgsql) rr.SetupNpgsql();
            else rr.SetupPipeRaw();
            
            if (runNpgsql) await rr.ReadNpgsql();
            else await rr.ReadPipeRawExtended();
            
            var count = 0;
            var sw = Stopwatch.StartNew();
            var timings = new List<double>(21);
            while (true)
            {
                count++;
                if (runNpgsql) await rr.ReadNpgsql();
                else await rr.ReadPipeRawExtended();
                if (count % 1000 == 0)
                {
                    var usPerQuery = sw.ElapsedTicks / (Stopwatch.Frequency / (1000L * 1000L)) / 1000;
                    timings.Add(usPerQuery); 
                    Console.WriteLine("us/query: " + usPerQuery);
                    sw.Restart();
                }

                if (count == 21000) break;
            }

            timings.Sort();
            Console.WriteLine("Median: " + timings[10]);
        } 
    }
}
