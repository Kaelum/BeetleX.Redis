using System;

using BeetleX.Buffers;

using MessagePack;

namespace BeetleX.Redis
{
	public interface IDataFormater
	{
		void SerializeObject(object data, RedisClient client, PipeStream stream);

		object DeserializeObject(Type type, RedisClient client, PipeStream stream, int length);

	}

	public class JsonFormater : IDataFormater
	{
		public object DeserializeObject(Type type, RedisClient client, PipeStream stream, int length)
		{
			using (SerializerExpand jsonExpend = client.SerializerExpand)
			{
				return jsonExpend.DeserializeJsonObject(stream, length, type);
			}
		}

		public void SerializeObject(object data, RedisClient client, PipeStream stream)
		{
			using (SerializerExpand jsonExpend = client.SerializerExpand)
			{
				ArraySegment<byte> buffer = jsonExpend.SerializeJsonObject(data);
				byte[] hdata = Command.GetBodyHeaderLenData(buffer.Count);
				if (hdata != null)
				{
					stream.Write(hdata, 0, hdata.Length);
				}
				else
				{
					string headerstr = $"${buffer.Count}\r\n";
					stream.Write(headerstr);
				}
				stream.Write(buffer.Array, buffer.Offset, buffer.Count);
			}
		}
	}

	public class MessagePackFormater : IDataFormater
	{
		public object DeserializeObject(Type type, RedisClient client, PipeStream stream, int length)
		{
			byte[] buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(length);
			try
			{
				stream.Read(buffer, 0, length);
				return MessagePackSerializer.Deserialize(type, new ReadOnlyMemory<byte>(buffer, 0, length));
			}
			finally
			{
				System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
			}
		}

		public void SerializeObject(object data, RedisClient client, PipeStream stream)
		{
			// MessagePackSerializer.NonGeneric.Serialize(data.GetType(), stream, data);
			using (SerializerExpand jsonExpend = client.SerializerExpand)
			{

				ArraySegment<byte> buffer = jsonExpend.SerializeMessagePack(data);
				byte[] hdata = Command.GetBodyHeaderLenData(buffer.Count);
				if (hdata != null)
				{
					stream.Write(hdata, 0, hdata.Length);
				}
				else
				{
					string headerstr = $"${buffer.Count}\r\n";
					stream.Write(headerstr);
				}
				stream.Write(buffer.Array, buffer.Offset, buffer.Count);
			}
		}
	}
}
