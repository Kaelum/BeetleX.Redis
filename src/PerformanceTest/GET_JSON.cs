using BeetleX.Redis;
using CodeBenchmark;
using Northwind.Data;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace PerformanceTest
{


    [System.ComponentModel.Category("Redis.Get")]
    public class BeetleX_Get : BeetleX_Base
    {
        public async override Task Execute()
        {
            await RedisDB.Get<Northwind.Data.Order>(OrderHelper.GetOrderID().ToString());
        }
    }

    [System.ComponentModel.Category("Redis.MGet")]
    public class BeetleX_MGet : BeetleX_Base
    {
        public async override Task Execute()
        {
            int id = OrderHelper.GetOrderID();
            await RedisDB.MGet<Northwind.Data.Order, Northwind.Data.Order>
                        (id.ToString(),
                        (id + 1).ToString());
        }
    }


    [System.ComponentModel.Category("Redis.Get")]
    public class StackExchange_AsyncGet :StackExchangeBase
    {
        public async override Task Execute()
        {
            int i = OrderHelper.GetOrderID();
            RedisValue data =  await RedisDB.StringGetAsync(i.ToString());
            Northwind.Data.Order item = Newtonsoft.Json.JsonConvert.DeserializeObject<Northwind.Data.Order>(data);
        }
    }

    [System.ComponentModel.Category("Redis.MGet")]
    public class StackExchange_AsyncMGet :StackExchangeBase
    {
        public async override Task Execute()
        {
            int i = OrderHelper.GetOrderID();
            RedisValue[] values = await RedisDB.StringGetAsync(new RedisKey[] { i.ToString(), (i + 1).ToString() });
            object item1, item2;
            if (!values[0].IsNullOrEmpty)
                item1 = Newtonsoft.Json.JsonConvert.DeserializeObject(values[0], typeof(Northwind.Data.Order));
            if (!values[1].IsNullOrEmpty)
                item2 = Newtonsoft.Json.JsonConvert.DeserializeObject(values[1], typeof(Northwind.Data.Order));
        }

    }

    [System.ComponentModel.Category("Redis.Get")]
    public class StackExchange_SyncGet :StackExchangeBase
    {
        public override Task Execute()
        {
            int i = OrderHelper.GetOrderID();
            RedisValue data = RedisDB.StringGet(i.ToString());
            Northwind.Data.Order item = Newtonsoft.Json.JsonConvert.DeserializeObject<Northwind.Data.Order>(data);
            return base.Execute();
        }
    }

    [System.ComponentModel.Category("Redis.MGet")]
    public class StackExchange_SyncMGet : StackExchangeBase
    {
        public override Task Execute()
        {
            int i = OrderHelper.GetOrderID();
            RedisValue[] values = RedisDB.StringGet(new RedisKey[] { i.ToString(), (i + 1).ToString() });
            object item1, item2;
            if (!values[0].IsNullOrEmpty)
                item1 = Newtonsoft.Json.JsonConvert.DeserializeObject(values[0], typeof(Northwind.Data.Order));
            if (!values[1].IsNullOrEmpty)
                item2 = Newtonsoft.Json.JsonConvert.DeserializeObject(values[1], typeof(Northwind.Data.Order));
            return base.Execute();
        }
    }

}
