using System;
using System.ComponentModel;
using System.Threading.Tasks;

using BeetleX.Redis;

using StackExchange.Redis;

namespace PerformanceTest
{


	[Category("Redis.Get")]
	public class BeetleX_Get : BeetleX_Base
	{
		public async override Task Execute()
		{
			await RedisDB.Get<Northwind.Data.Order>(OrderHelper.GetOrderID().ToString());
		}
	}

	[Category("Redis.MGet")]
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


	[Category("Redis.Get")]
	public class StackExchange_AsyncGet : StackExchangeBase
	{
		public async override Task Execute()
		{
			int i = OrderHelper.GetOrderID();
			RedisValue data = await RedisDB.StringGetAsync(i.ToString());
			Northwind.Data.Order item = Newtonsoft.Json.JsonConvert.DeserializeObject<Northwind.Data.Order>(data);
		}
	}

	[Category("Redis.MGet")]
	public class StackExchange_AsyncMGet : StackExchangeBase
	{
		public async override Task Execute()
		{
			int i = OrderHelper.GetOrderID();
			RedisValue[] values = await RedisDB.StringGetAsync(new RedisKey[] { i.ToString(), (i + 1).ToString() });

			if (!values[0].IsNullOrEmpty)
			{
				_ = Newtonsoft.Json.JsonConvert.DeserializeObject(values[0], typeof(Northwind.Data.Order));
			}

			if (!values[1].IsNullOrEmpty)
			{
				_ = Newtonsoft.Json.JsonConvert.DeserializeObject(values[1], typeof(Northwind.Data.Order));
			}
		}

	}

	[Category("Redis.Get")]
	public class StackExchange_SyncGet : StackExchangeBase
	{
		public override Task Execute()
		{
			int i = OrderHelper.GetOrderID();
			RedisValue data = RedisDB.StringGet(i.ToString());
			_ = Newtonsoft.Json.JsonConvert.DeserializeObject<Northwind.Data.Order>(data);

			return base.Execute();
		}
	}

	[Category("Redis.MGet")]
	public class StackExchange_SyncMGet : StackExchangeBase
	{
		public override Task Execute()
		{
			int i = OrderHelper.GetOrderID();
			RedisValue[] values = RedisDB.StringGet(new RedisKey[] { i.ToString(), (i + 1).ToString() });

			if (!values[0].IsNullOrEmpty)
			{
				_ = Newtonsoft.Json.JsonConvert.DeserializeObject(values[0], typeof(Northwind.Data.Order));
			}

			if (!values[1].IsNullOrEmpty)
			{
				_ = Newtonsoft.Json.JsonConvert.DeserializeObject(values[1], typeof(Northwind.Data.Order));
			}

			return base.Execute();
		}
	}
}
