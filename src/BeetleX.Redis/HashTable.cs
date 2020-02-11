﻿using System;
using System.Linq;
using System.Threading.Tasks;

namespace BeetleX.Redis
{
	public class RedisHashTable
	{

		public RedisHashTable(RedisDB db, string key, IDataFormater dataFormater)
		{
			DB = db;
			Key = key;
			DataFormater = dataFormater;
		}

		public IDataFormater DataFormater { get; set; }

		public RedisDB DB { get; private set; }

		public string Key { get; set; }

		public async ValueTask<long> Del(params string[] fields)
		{
			Commands.HDEL cmd = new Commands.HDEL(Key, fields);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (long)result.Value;
		}

		public async ValueTask<bool> Exists(string field)
		{
			Commands.HEXISTS cmd = new Commands.HEXISTS(Key, field);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (long)result.Value > 0;
		}

		public async ValueTask<T> Get<T>(string field)
		{
			Commands.HGET cmd = new Commands.HGET(Key, field, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (T)result.Value;
		}

		public async ValueTask<(T, T1)> Get<T, T1>(string field, string field1)
		{
			string[] fields = { field, field1 };
			Type[] types = { typeof(T), typeof(T1), };
			object[] items = await Get(fields, types);
			return ((T)items[0], (T1)items[1]);
		}

		public async ValueTask<(T, T1, T2)> Get<T, T1, T2>(string field, string field1, string field2)
		{
			string[] fields = { field, field1, field2 };
			Type[] types = { typeof(T), typeof(T1), typeof(T2) };
			object[] items = await Get(fields, types);
			return ((T)items[0], (T1)items[1], (T2)items[2]);
		}

		public async ValueTask<(T, T1, T2, T3)> Get<T, T1, T2, T3>
			(string field, string field1, string field2, string field3)
		{
			string[] fields = { field, field1, field2, field3 };
			Type[] types = { typeof(T), typeof(T1), typeof(T2), typeof(T3) };
			object[] items = await Get(fields, types);
			return ((T)items[0], (T1)items[1], (T2)items[2], (T3)items[3]);
		}

		public async ValueTask<(T, T1, T2, T3, T4)> Get<T, T1, T2, T3, T4>
			(string field, string field1, string field2, string field3, string field4)
		{
			string[] fields = { field, field1, field2, field3, field4 };
			Type[] types = { typeof(T), typeof(T1), typeof(T2), typeof(T3), typeof(T4) };
			object[] items = await Get(fields, types);
			return ((T)items[0], (T1)items[1], (T2)items[2], (T3)items[3], (T4)items[4]);
		}

		public async ValueTask<object[]> Get(string[] fields, Type[] types)
		{
			Commands.HMGET cmd = new Commands.HMGET(Key, DataFormater, fields);
			Result result = await DB.Execute(cmd, types);
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (from a in result.Data select a.Data).ToArray();

		}

		public async ValueTask<long> Incrby(string field, int increment)
		{
			Commands.HINCRBY cmd = new Commands.HINCRBY(Key, field, increment);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (long)result.Value;
		}

		public async ValueTask<float> IncrbyFloat(string field, float increment)
		{
			Commands.HINCRBYFLOAT cmd = new Commands.HINCRBYFLOAT(Key, field, increment);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return float.Parse((string)result.Value);
		}

		public async ValueTask<string[]> Keys()
		{
			Commands.HKEYS cmd = new Commands.HKEYS(Key);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (from a in result.Data select (string)a.Data).ToArray();

		}

		public async ValueTask<long> Len()
		{
			Commands.HLEN cmd = new Commands.HLEN(Key);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (long)result.Value;
		}

		public async ValueTask<long> Set(string field, object data)
		{
			Commands.HSET cmd = new Commands.HSET(Key, field, data, DataFormater);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (long)result.Value;
		}

		public async ValueTask<string> MSet(params (string, object)[] datas)
		{
			Commands.HMSET cmd = new Commands.HMSET(Key, DataFormater);
			foreach ((string, object) item in datas)
			{
				cmd = cmd[item.Item1, item.Item2];
			}
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (string)result.Value;
		}

		public async ValueTask<long> SetNX(string field, object value)
		{
			Commands.HSETNX cmd = new Commands.HSETNX(Key, field, value, DataFormater);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (long)result.Value;
		}

		public async ValueTask<long> StrLen(string field)
		{
			Commands.HSTRLEN cmd = new Commands.HSTRLEN(Key, field);
			Result result = await DB.Execute(cmd, typeof(string));
			if (result.IsError)
				throw new RedisException(result.Messge);
			return (long)result.Value;
		}
	}
}
