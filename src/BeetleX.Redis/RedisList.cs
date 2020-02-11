using System.Linq;
using System.Threading.Tasks;

using BeetleX.Buffers;

namespace BeetleX.Redis
{
    public class RedisList<T>
    {
        public RedisList(RedisDB db, string key, IDataFormater dataFormater = null)
        {
            DB = db;
            Key = key;
            DataFormater = dataFormater;
        }

        public string Key { get; set; }

        public RedisDB DB { get; set; }

        public IDataFormater DataFormater { get; set; }

        private bool OnBlockingRead(Result r, PipeStream stream, RedisClient c)
        {
            if (r.ReadCount % 2 == 1)
            {
                if (r.BodyLength == -1 || r.BodyLength == 0)
                {
					ResultItem item = new ResultItem { Type = ResultType.Null, Data = null };
                }
                else
                {
                    if (DataFormater == null)
                    {
                        object value = stream.ReadString(r.BodyLength.Value);
                        r.Data.Add(new ResultItem { Type = ResultType.String, Data = value });
                    }
                    else
                    {
                        object value = DataFormater.DeserializeObject(
                            typeof(T), c, stream, r.BodyLength.Value);
                        r.Data.Add(new ResultItem { Type = ResultType.Object, Data = value });
                    }
                }
            }
            else
            {
                stream.ReadString(r.BodyLength.Value);
            }
            return false;
        }

        public async ValueTask<T> BRPop(int timeout = 0)
        {
            Commands.BRPOP cmd = new Commands.BRPOP(new string[] { Key }, timeout, DataFormater);
            cmd.Reader = OnBlockingRead;
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
        }

        public async ValueTask<T> BLPop(int timeout = 0)
        {
            Commands.BLPOP cmd = new Commands.BLPOP(new string[] { Key }, timeout, DataFormater);
            cmd.Reader = OnBlockingRead;
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
        }

        public async ValueTask<T> BRPopLPush(string dest, int timeout = 0)
        {
            Commands.BRPOPLPUSH cmd = new Commands.BRPOPLPUSH(Key, dest, timeout, DataFormater);
            cmd.Reader = OnBlockingRead;
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
        }

        public async ValueTask<T> Index(int index)
        {
            Commands.LINDEX cmd = new Commands.LINDEX(Key, index, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
        }

        public async ValueTask<long> Insert(bool before, T ofValue, T value)
        {
            Commands.LINSERT cmd = new Commands.LINSERT(
                Key, ofValue, before, value, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
        }

        public async ValueTask<long> Len()
        {
            Commands.LLEN cmd = new Commands.LLEN(Key);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
        }

        public async ValueTask<T> Pop()
        {
            Commands.LPOP cmd = new Commands.LPOP(Key, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
        }

        public async ValueTask<long> Push(params T[] values)
        {
            Commands.LPUSH cmd = new Commands.LPUSH(Key, DataFormater);
            for (int i = 0; i < values.Length; i++)
			{
				cmd.Values.Add(values[i]);
			}

			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
        }

        public async ValueTask<long> PushX(T value)
        {
            Commands.LPUSHX cmd = new Commands.LPUSHX(Key, value, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
        }

        public async ValueTask<T[]> Range(int start, int stop)
        {
            Commands.LRANGE cmd = new Commands.LRANGE(Key, start, stop,
                DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (from a in result.Data select (T)a.Data).ToArray();
        }

        public async ValueTask<long> Rem(int count, T value)
        {
            Commands.LREM cmd = new Commands.LREM(Key, count, value, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
        }

        public async ValueTask<string> Set(int index, T value)
        {
            Commands.LSET cmd = new Commands.LSET(Key, index, value, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
        }

        public async ValueTask<string> Trim(int start, int stop)
        {
            Commands.LTRIM cmd = new Commands.LTRIM(Key, start, stop);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (string)result.Value;
        }

        public async ValueTask<T> RPop()
        {
            Commands.RPOP cmd = new Commands.RPOP(Key, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
        }

        public async ValueTask<T> RPopLPush(string dest)
        {
            Commands.RPOPLPUSH cmd = new Commands.RPOPLPUSH(Key, dest, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (T)result.Value;
        }

        public async ValueTask<long> RPush(params T[] values)
        {
            Commands.RPUSH cmd = new Commands.RPUSH(Key, DataFormater);
            for (int i = 0; i < values.Length; i++)
            {
                cmd.Values.Add(values[i]);
            }
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
        }

        public async ValueTask<long> RPushX(T value)
        {
            Commands.RPUSHX cmd = new Commands.RPUSHX(Key, value, DataFormater);
			Result result = await DB.Execute(cmd, typeof(T));
            if (result.IsError)
			{
				throw new RedisException(result.Messge);
			}

			return (long)result.Value;
        }

    }
}
