// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;
using Authlete.Util;

namespace com.github.dhoard.kafka
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Usage: .. <test properties");
                return;
            }

            string filename = args[0];

            IDictionary<String, String> properties;
            int batchSize;
            int recordSize;
            int recordCount;
            string topic;
            Timer timer;
            RandomStringGenerator randomStringGenerator;

            using (TextReader textReader = new StreamReader(filename))
            {
                properties = PropertiesLoader.Load(textReader);
            }

            topic = properties["topic"];
            recordSize = Int32.Parse(properties["record.size"]);
            recordCount = Int32.Parse(properties["record.count"]);
            batchSize = Int32.Parse(properties["batch.size"]);

            properties.Remove("topic");
            properties.Remove("record.size");
            properties.Remove("record.count");
            properties.Remove("key.serializer");
            properties.Remove("value.serializer");

            ClientConfig clientConfig = new ClientConfig();

            foreach (KeyValuePair<string, string> entry in properties)
            {
                clientConfig.Set(entry.Key, entry.Value);
            }

            timer = new Timer();
            randomStringGenerator = new RandomStringGenerator();

            using (var producer = new ProducerBuilder<string, byte[]>(clientConfig).Build())
            {
                for (int i = 0; i < recordCount; i++)
                {
                    try
                    {
                        byte[] value = Encoding.ASCII.GetBytes(randomStringGenerator.Generate(recordSize));
                        timer.Start();
                        var deliveryReport = await producer.ProduceAsync(topic, new Message<string, byte[]> { Key = null, Value = value });
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"failed to deliver message: {e.Message} [{e.Error.Code}]");
                    }
                    finally
                    {
                        timer.Stop();
                    }
                }
            }

            Console.WriteLine("topic        : " + topic);
            Console.WriteLine("record size  : " + recordSize + " bytes");
            Console.WriteLine("record count : " + recordCount);
            Console.WriteLine("batch size   : " + batchSize);
            Console.WriteLine("time         : " + timer.GetTime() + " ms");
            Console.WriteLine("min          : " + timer.GetMin() + " ms");
            Console.WriteLine("max          : " + timer.GetMax() + " ms");
            Console.WriteLine("median       : " + timer.GetMedian() + " ms");
            Console.WriteLine("mean         : " + timer.GetMean() + " ms");
            Console.WriteLine("99th %-tile  : " + timer.GetPercentile(99) + " ms");
            Console.WriteLine("95th %-tile  : " + timer.GetPercentile(95) + " ms");
            Console.WriteLine("90th %-tile  : " + timer.GetPercentile(90) + " ms");
            Console.WriteLine("80th %-tile  : " + timer.GetPercentile(80) + " ms");
            Console.WriteLine("70th %-tile  : " + timer.GetPercentile(70) + " ms");
            Console.WriteLine("60th %-tile  : " + timer.GetPercentile(60) + " ms");
            Console.WriteLine("50th %-tile  : " + timer.GetPercentile(50) + " ms");
            Console.WriteLine("40th %-tile  : " + timer.GetPercentile(40) + " ms");
            Console.WriteLine("30th %-tile  : " + timer.GetPercentile(30) + " ms");
            Console.WriteLine("20th %-tile  : " + timer.GetPercentile(20) + " ms");
            Console.WriteLine("10th %-tile  : " + timer.GetPercentile(10) + " ms");
            Console.WriteLine("rate         : " + ((double) recordCount) / (timer.GetTime() / 1000.0d) + " records per second");
        }
    }
}