﻿using System;

namespace BeetleX.Redis
{
    public interface IHostHandler
    {
        RedisHost AddWriteHost(string host, int port = 6379);


        RedisHost AddWriteHost(string host, int port, bool ssl);

        RedisHost AddReadHost(string host, int port = 6379);

        RedisHost AddReadHost(string host, int port, bool ssl);


        RedisHost GetWriteHost();


        RedisHost GetReadHost();

    }
}
