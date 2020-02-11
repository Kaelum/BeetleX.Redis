using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BeetleX.Redis
{
	public class RedisDB : IHostHandler
	{
		private readonly List<RedisHost> _writeHosts = new List<RedisHost>();
		private readonly List<RedisHost> _readHosts = new List<RedisHost>();
		private readonly Timer _detectionTime;

		private RedisHost[] _writeActives = new RedisHost[0];
		private RedisHost[] _readActives = new RedisHost[0];


		#region Constructors

		public RedisDB(int db = 0, IDataFormater dataFormater = null, IHostHandler hostHandler = null)
		{
			DB = db;
			DataFormater = dataFormater;

			if (hostHandler == null)
			{
				_detectionTime = new Timer(OnDetection, null, 1000, 1000);

				Host = this;
			}
			else
			{
				Host = hostHandler;
			}
		}

		#endregion

		internal static RedisDB Default { get; } = new RedisDB();

		public IHostHandler Host { get; set; }

		private void OnDetection(object state)
		{
			_detectionTime.Change(-1, -1);

			RedisHost[] wHosts = _writeActives;
			foreach (RedisHost item in wHosts)
			{
				item.Ping();
			}

			RedisHost[] rHost = _readActives;
			foreach (RedisHost item in rHost)
			{
				item.Ping();
			}

			_detectionTime.Change(1000, 1000);

		}

		public IDataFormater DataFormater { get; set; }

		private bool OnClientPush(RedisClient client)
		{
			return true;
		}

		public int DB { get; set; }

		public RedisDB Cloneable(IDataFormater dataFormater = null)
		{
			RedisDB result = new RedisDB(DB, dataFormater, this);
			return result;
		}

		#region IHostHandler Implementation

		RedisHost IHostHandler.AddReadHost(string host, int port)
		{
			return ((IHostHandler)this).AddReadHost(host, port, false);
		}

		RedisHost IHostHandler.AddReadHost(string host, int port, bool ssl)
		{
			if (port == 0)
			{
				port = 6379;
			}

			RedisHost redisHost = new RedisHost(ssl, DB, host, port);
			_readHosts.Add(redisHost);
			_readActives = _readHosts.ToArray();
			return redisHost;
		}

		RedisHost IHostHandler.AddWriteHost(string host, int port)
		{
			return ((IHostHandler)this).AddWriteHost(host, port, false);
		}

		RedisHost IHostHandler.AddWriteHost(string host, int port, bool ssl)
		{
			if (port == 0)
			{
				port = 6379;
			}

			RedisHost redisHost = new RedisHost(ssl, DB, host, port);
			_writeHosts.Add(redisHost);
			_writeActives = _writeHosts.ToArray();
			return redisHost;
		}

		RedisHost IHostHandler.GetReadHost()
		{
			RedisHost[] items = _readActives;
			for (int i = 0; i < items.Length; i++)
			{
				if (items[i].Available)
				{
					return items[i];
				}
			}
			return Host.GetWriteHost();
		}

		RedisHost IHostHandler.GetWriteHost()
		{
			RedisHost[] items = _writeActives;
			for (int i = 0; i < items.Length; i++)
			{
				if (items[i].Available)
				{
					return items[i];
				}
			}
			return null;
		}

		#endregion

		public Subscriber Subscribe()
		{
			Subscriber result = new Subscriber(this);
			return result;
		}

		public async Task<Result> Execute(Command cmd, params Type[] types)
		{
			RedisHost host = cmd.Read ? Host.GetReadHost() : Host.GetWriteHost();
			if (host == null)
			{
				return new Result() { ResultType = ResultType.NetError, Messge = "redis server is not available" };
			}
			RedisClient client = await host.Pop();
			if (client == null)
			{
				return new Result() { ResultType = ResultType.NetError, Messge = "exceeding maximum number of connections" };
			}

			Result result = host.Connect(client);
			if (result.IsError)
			{
				host.Push(client);
				return result;
			}
			RedisRequest request = new RedisRequest(host, client, cmd, types);
			result = await request.Execute();
			return result;

		}

		public async ValueTask<string> Flushall()
		{
			Commands.FLUSHALL cmd = new Commands.FLUSHALL();
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<bool> Ping()
		{
			Commands.PING ping = new Commands.PING(null);
			Result result = await Execute(ping, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return true;
		}

		public async ValueTask<long> Del(params string[] key)
		{
			Commands.DEL del = new Commands.DEL(key);
			Result result = await Execute(del, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<string> Set(string key, object value)
		{
			return await Set(key, value, null, null);
		}

		public async ValueTask<string> Set(string key, object value, int? seconds)
		{
			return await Set(key, value, seconds, null);
		}

		public async ValueTask<string> Set(string key, object value, int? seconds, bool? nx)
		{
			Commands.SET set = new Commands.SET(key, value, DataFormater);
			if (seconds != null)
			{
				set.ExpireTimeType = ExpireTimeType.EX;
				set.TimeOut = seconds.Value;
			}
			set.NX = nx;
			Result result = await Execute(set, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;

		}

		public async ValueTask<string> Dump(string key)
		{
			Commands.DUMP dump = new Commands.DUMP(key);
			Result result = await Execute(dump, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<long> Exists(params string[] key)
		{
			Commands.EXISTS exists = new Commands.EXISTS(key);
			Result result = await Execute(exists, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Expire(string key, int seconds)
		{
			Commands.EXPIRE expire = new Commands.EXPIRE(key, seconds);
			Result result = await Execute(expire, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Ttl(string key)
		{
			Commands.TTL cmd = new Commands.TTL(key);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> PTtl(string key)
		{
			Commands.PTTL cmd = new Commands.PTTL(key);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Expireat(string key, long timestamp)
		{
			Commands.EXPIREAT cmd = new Commands.EXPIREAT(key, timestamp);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<string> MSet(params (string, object)[] datas)
		{
			Commands.MSET cmd = new Commands.MSET(DataFormater);
			foreach ((string, object) item in datas)
			{
				cmd = cmd[item.Item1, item.Item2];
			}
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<long> MSetNX(params (string, object)[] datas)
		{
			Commands.MSETNX cmd = new Commands.MSETNX(DataFormater);
			foreach ((string, object) item in datas)
			{
				cmd = cmd[item.Item1, item.Item2];
			}
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<T> Get<T>(string key)
		{
			Commands.GET cmd = new Commands.GET(key, DataFormater);
			Result result = await Execute(cmd, typeof(T));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
		}

		public async ValueTask<string[]> Keys(string pattern)
		{
			Commands.KEYS cmd = new Commands.KEYS(pattern);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (from a in result.Data select (string)a.Data).ToArray();
		}

		public async ValueTask<long> Move(string key, int db)
		{
			Commands.MOVE cmd = new Commands.MOVE(key, db);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Persist(string key)
		{
			Commands.PERSIST cmd = new Commands.PERSIST(key);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Pexpire(string key, long milliseconds)
		{
			Commands.PEXPIRE cmd = new Commands.PEXPIRE(key, milliseconds);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Pexpireat(string key, long timestamp)
		{
			Commands.PEXPIREAT cmd = new Commands.PEXPIREAT(key, timestamp);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<string> Randomkey()
		{
			Commands.RANDOMKEY cmd = new Commands.RANDOMKEY();
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<string> Rename(string key, string newkey)
		{
			Commands.RENAME cmd = new Commands.RENAME(key, newkey);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<long> Renamenx(string key, string newkey)
		{
			Commands.RENAMENX cmd = new Commands.RENAMENX(key, newkey);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Touch(params string[] keys)
		{
			Commands.TOUCH cmd = new Commands.TOUCH(keys);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<string> Type(string key)
		{
			Commands.TYPE cmd = new Commands.TYPE(key);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<long> Unlink(params string[] keys)
		{
			Commands.UNLINK cmd = new Commands.UNLINK(keys);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Decr(string key)
		{
			Commands.DECR cmd = new Commands.DECR(key);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Decrby(string key, int decrement)
		{
			Commands.DECRBY cmd = new Commands.DECRBY(key, decrement);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> SetBit(string key, int offset, bool value)
		{
			Commands.SETBIT cmd = new Commands.SETBIT(key, offset, value);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> GetBit(string key, int offset)
		{
			Commands.GETBIT cmd = new Commands.GETBIT(key, offset);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<string> GetRange(string key, int start, int end)
		{
			Commands.GETRANGE cmd = new Commands.GETRANGE(key, start, end);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<T> GetSet<T>(string key, object value)
		{
			Commands.GETSET cmd = new Commands.GETSET(key, value, DataFormater);
			Result result = await Execute(cmd, typeof(T));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
		}

		public async ValueTask<long> Incr(string key)
		{
			Commands.INCR cmd = new Commands.INCR(key);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Incrby(string key, int increment)
		{
			Commands.INCRBY cmd = new Commands.INCRBY(key, increment);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<float> IncrbyFloat(string key, float increment)
		{
			Commands.INCRBYFLOAT cmd = new Commands.INCRBYFLOAT(key, increment);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return float.Parse((string)result.Value);
		}

		public async ValueTask<(T, T1)> MGet<T, T1>(string key1, string key2)
		{
			string[] keys = { key1, key2 };
			Type[] types = { typeof(T), typeof(T1) };
			object[] items = await MGet(keys, types);
			return ((T)items[0], (T1)items[1]);
		}

		public async ValueTask<(T, T1, T2)> MGet<T, T1, T2>(string key1, string key2, string key3)
		{
			string[] keys = { key1, key2, key3 };
			Type[] types = { typeof(T), typeof(T1), typeof(T2) };
			object[] items = await MGet(keys, types);
			return ((T)items[0], (T1)items[1], (T2)items[2]);
		}

		public async ValueTask<(T, T1, T2, T3)> MGet<T, T1, T2, T3>(string key1, string key2, string key3, string key4)
		{
			string[] keys = { key1, key2, key3, key4 };
			Type[] types = { typeof(T), typeof(T1), typeof(T2), typeof(T3) };
			object[] items = await MGet(keys, types);
			return ((T)items[0], (T1)items[1], (T2)items[2], (T3)items[3]);
		}

		public async ValueTask<(T, T1, T2, T3, T4)> MGet<T, T1, T2, T3, T4>(string key1, string key2, string key3, string key4, string key5)
		{
			string[] keys = { key1, key2, key3, key4, key5 };
			Type[] types = { typeof(T), typeof(T1), typeof(T2), typeof(T3), typeof(T4) };
			object[] items = await MGet(keys, types);
			return ((T)items[0], (T1)items[1], (T2)items[2], (T3)items[3], (T4)items[4]);
		}

		public async ValueTask<object[]> MGet(string[] keys, Type[] types)
		{
			Commands.MGET cmd = new Commands.MGET(DataFormater, keys);
			Result result = await Execute(cmd, types);
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (from a in result.Data select a.Data).ToArray();

		}

		public async ValueTask<string> PSetEX(string key, long milliseconds, object value)
		{
			Commands.PSETEX cmd = new Commands.PSETEX(key, milliseconds, value, DataFormater);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<string> SetEX(string key, int seconds, object value)
		{
			Commands.SETEX cmd = new Commands.SETEX(key, seconds, value, DataFormater);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
		}

		public async ValueTask<long> SetNX(string key, object value)
		{
			Commands.SETNX cmd = new Commands.SETNX(key, value, DataFormater);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> SetRange(string key, int offset, string value)
		{
			Commands.SETRANGE cmd = new Commands.SETRANGE(key, offset, value);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public async ValueTask<long> Strlen(string key)
		{
			Commands.STRLEN cmd = new Commands.STRLEN(key);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}

		public Sequence CreateSequence(string key)
		{
			return new Sequence(this, key);
		}

		public RedisHashTable CreateHashTable(string key)
		{
			return new RedisHashTable(this, key, DataFormater);
		}

		public RedisList<T> CreateList<T>(string key)
		{
			return new RedisList<T>(this, key, DataFormater);
		}

		public async ValueTask<long> Publish(string channel, object data)
		{
			Commands.PUBLISH cmd = new Commands.PUBLISH(channel, data, DataFormater);
			Result result = await Execute(cmd, typeof(string));
			if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
		}
	}
}
