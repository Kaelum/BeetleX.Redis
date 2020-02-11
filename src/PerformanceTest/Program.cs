﻿using CodeBenchmark;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;
namespace PerformanceTest
{
    class Program
    {
        public const string Host = "192.168.2.19";

        static void Main(string[] args)
        {
            Benchmark benchmark = new Benchmark();
            benchmark.Register(typeof(Program).Assembly);
            benchmark.Start();
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
			{
				benchmark.OpenWeb();
			}

			Console.Read();
            //Test();
            //Console.Read();
        }
    }
}
