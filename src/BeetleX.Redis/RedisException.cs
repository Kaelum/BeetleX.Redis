using System;

namespace BeetleX.Redis
{
    public class RedisException : Exception
    {
        public RedisException(string msg) : base(msg)
        {

        }

        public RedisException(string msg, Exception innerError) : base(msg, innerError) { }
    }
}
