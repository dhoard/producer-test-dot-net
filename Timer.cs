using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace com.github.dhoard.kafka
{
    public class Timer
    {
        private long startTime;
        
        private List<long> timeList;
        private List<long> timeListUnsorted;

        public Timer()
        {
            timeList = new List<long>();
            timeListUnsorted = new List<long>();
        }

        public void Start()
        {
            startTime = DateTimeOffset.Now.ToUnixTimeMilliseconds();
        }

        public void Stop()
        {
            long time = DateTimeOffset.Now.ToUnixTimeMilliseconds() - startTime;
            timeList.Add(time);
            timeListUnsorted.Add(time);
        }

        public long GetTime()
        {
            long time = 0;
            foreach (var value in timeList)
            {
                time += value;
            }

            return time;
        }

        public long GetMin()
        {
            timeList.Sort();
            return timeList[0];

        }

        public long GetMax()
        {
            timeList.Sort();
            timeList.Reverse();
            return timeList[0];
        }

        public long GetMedian()
        {
            timeList.Sort();
            return timeList[timeList.Count / 2];
        }

        public double GetMean()
        {
            return (double) GetTime() / (double) timeList.Count;
        }

        public long GetPercentile(double percentile)
        {
            if (percentile <= 0)
            {
                throw new IllegalArgumentException();
            }

            if (percentile >= 100)
            {
                throw new IllegalArgumentException();
            }

            timeList.Sort();
            return Percentile(timeList, percentile);
        }

        private static long Percentile(List<long> latencies, double percentile)
        {
            int index = (int)Math.Ceiling(percentile / 100.0 * latencies.Count);
            return latencies[index - 1];
        }
    }
}
