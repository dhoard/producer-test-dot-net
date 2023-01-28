using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace com.github.dhoard.kafka
{
    public class RandomStringGenerator
    {
        private static readonly Random random = new Random();

        public string Generate(int length)
        {
            char[] letters = new char[length];
            for (int i = 0; i < length; i++)
            {
                letters[i] = GenerateChar(random);
            }
            return new string(letters);
        }

        internal static char GenerateChar(Random random)
        {
            // 'Z' + 1 because the range is exclusive
            return (char)(random.Next('A', 'Z' + 1));
        }
    }
}
