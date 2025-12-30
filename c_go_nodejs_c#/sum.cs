using System;
using System.Diagnostics;

class Sum
{
    static void Main(string[] args)
    {
        var stopwatch = Stopwatch.StartNew();

        long sum = 0; // int taşar, long kullanıyoruz
        for (int i = 1; i <= 100_000_000; i++)
        {
            sum += i;
        }

        stopwatch.Stop();
        Console.WriteLine($"Sum: {sum}");
        Console.WriteLine($"Time: {stopwatch.ElapsedMilliseconds} ms");
    }
}
