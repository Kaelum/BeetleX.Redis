using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;

using MessagePack;

namespace BeetleX.Redis
{
	internal class SerializerExpand : IDisposable
	{
		private const int _bufferSize = 1024 * 4;
		private static readonly ConcurrentStack<SerializerExpand> _mPools = new ConcurrentStack<SerializerExpand>();

		private readonly byte[] _mBuffer = new byte[_bufferSize];

		private StreamReader _streamReader;
		private StreamWriter _streamWriter;


		#region Constructors

		public SerializerExpand()
		{
			Memory = new MemoryStream(1024 * 32);

			InitStream();
		}

		#endregion

		#region IDisposable Implementation

		public void Dispose()
		{
			Reset();
			Push(this);
		}

		#endregion

		#region Public Static Methods

		public static SerializerExpand Pop()
		{
			if (_mPools.TryPop(out SerializerExpand result))
			{
				return result;
			}

			return new SerializerExpand();
		}

		public static void Push(SerializerExpand jsonWriterExpand)
		{
			_mPools.Push(jsonWriterExpand);
		}

		#endregion

		#region Public Properties

		public MemoryStream Memory { get; private set; }

		#endregion

		#region Public Methods

		public object DeserializeJsonObject(Stream stream, int length, Type type)
		{
			try
			{
				while (length > 0)
				{
					int readcount = _bufferSize;

					if (length < _bufferSize)
					{
						readcount = length;
					}

					int len = stream.Read(_mBuffer, 0, readcount);
					Memory.Write(_mBuffer, 0, len);
					length -= len;
				}

				Memory.Position = 0;
				object result = JsonSerializer.Deserialize(Memory.GetBuffer(), type);
				return result;
			}
			catch (Exception e_)
			{
				InitStream();

				throw new RedisException($"JSON deserialization error {e_.Message}", e_);
			}
		}

		public ArraySegment<byte> GetBuffer()
		{
			ArraySegment<byte> result = new ArraySegment<byte>(Memory.GetBuffer(), 0, (int)Memory.Position);
			return result;
		}

		public void Reset()
		{
			Memory.SetLength(0);
			Memory.Position = 0;
		}

		public ArraySegment<byte> SerializeJsonObject(object data)
		{
			try
			{
				Task task = JsonSerializer.SerializeAsync(Memory, data, data.GetType());
				task.Wait();
				return GetBuffer();
			}
			catch (Exception e_)
			{
				InitStream();
				throw new RedisException($"json serialize error {e_.Message}", e_);
			}
		}

		public ArraySegment<byte> SerializeMessagePack(object data)
		{
			MessagePackSerializer.Serialize(data.GetType(), Memory, data);
			return GetBuffer();
		}

		#endregion

		#region Private Methods

		private void InitStream()
		{
			_streamReader = new StreamReader(Memory);
			_streamWriter = new StreamWriter(Memory);
		}

		#endregion
	}
}
