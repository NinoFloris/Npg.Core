using BenchmarkDotNet.Running;
using System.Reflection;
using System.Threading.Tasks;

namespace Benchmark
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // new BenchmarkSwitcher(typeof(Program).GetTypeInfo().Assembly).Run(args);
            // return;
            
            var rr = new ReadRows();
            rr.NumRows = 1000;
            await rr.SetupPipeRaw();
            while (true)
            {
                await rr.ReadPipeRawExtended();
            }
            
            
        } 
    }
}
