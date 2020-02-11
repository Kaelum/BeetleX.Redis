using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

using BeetleX.Buffers;

namespace BeetleX.Redis
{
	public abstract class Command
	{
		private static readonly List<byte[]> _msgHeaderLenData = new List<byte[]>();
		private static readonly List<byte[]> _bodyHeaderLenData = new List<byte[]>();
		private static byte[] _lineBytes;

		public const int MAX_LENGTH_TABLE = 1024 * 32;

		private readonly List<CommandParameter> _mParameters = new List<CommandParameter>();
		private readonly ConcurrentDictionary<string, byte[]> _mCommandBuffers = new ConcurrentDictionary<string, byte[]>();


		#region Constructors

		static Command()
		{
			_lineBytes = Encoding.ASCII.GetBytes("\r\n");

			for (int i = 1; i <= MAX_LENGTH_TABLE; i++)
			{
				_msgHeaderLenData.Add(Encoding.UTF8.GetBytes($"*{i}\r\n"));
				_bodyHeaderLenData.Add(Encoding.UTF8.GetBytes($"${i}\r\n"));
			}
		}

		public Command()
		{

		}

		#endregion

		#region Public Static Methods

		public static byte[] GetMsgHeaderLengthData(int length)
		{
			if (length > MAX_LENGTH_TABLE)
			{
				return null;
			}

			return _msgHeaderLenData[length - 1];
		}

		public static byte[] GetBodyHeaderLenData(int length)
		{
			if (length > MAX_LENGTH_TABLE)
			{
				return null;
			}

			return _bodyHeaderLenData[length - 1];
		}

		#endregion

		#region Public Properties

		public Func<Result, PipeStream, RedisClient, bool> Reader { get; set; }

		public abstract bool Read { get; }

		public IDataFormater DataFormater { get; set; }

		public abstract string Name { get; }

		#endregion

		#region Public Methods

		public Command AddData(object data)
		{
			_mParameters.Add(new CommandParameter { Value = data, DataFormater = DataFormater, Serialize = true });
			return this;
		}

		public Command AddText(object text)
		{
			_mParameters.Add(new CommandParameter { Value = text });
			return this;
		}

		public void Execute(RedisClient client, PipeStream stream)
		{
			OnExecute();
			byte[] data = GetMsgHeaderLengthData(_mParameters.Count);
			if (data != null)
			{
				stream.Write(data, 0, data.Length);
			}
			else
			{
				string headerStr = $"*{_mParameters.Count}\r\n";
				stream.Write(headerStr);
			}
			for (int i = 0; i < _mParameters.Count; i++)
			{
				_mParameters[i].Write(client, stream);
			}
		}

		public virtual void OnExecute()
		{
			if (!_mCommandBuffers.TryGetValue(Name, out byte[] cmdBuffer))
			{
				string value = $"${Name.Length}\r\n{Name}";
				cmdBuffer = Encoding.ASCII.GetBytes(value);
				_mCommandBuffers[Name] = cmdBuffer;
			}
			_mParameters.Add(new CommandParameter { ValueBuffer = cmdBuffer });
		}

		#endregion

		public class CommandParameter
		{
			[ThreadStatic]
			private static byte[] _buffer = null;


			public IDataFormater DataFormater { get; set; }

			public object Value { get; set; }

			internal byte[] ValueBuffer { get; set; }

			public bool Serialize { get; set; } = false;

			public void Write(RedisClient client, PipeStream stream)
			{
				if (ValueBuffer != null)
				{
					stream.Write(ValueBuffer, 0, ValueBuffer.Length);
				}
				else if (Serialize && DataFormater != null)
				{
					DataFormater.SerializeObject(Value, client, stream);
				}
				else
				{
					if (!(Value is string value))
					{
						value = Value.ToString();
					}

					if (_buffer == null)
					{
						_buffer = new byte[1024 * 1024];
					}

					int len = Encoding.UTF8.GetBytes(value, 0, value.Length, _buffer, 0);
					byte[] data = GetBodyHeaderLenData(len);

					if (data != null)
					{
						stream.Write(data, 0, data.Length);
					}
					else
					{
						stream.Write($"${len}\r\n");
					}

					stream.Write(_buffer, 0, len);
				}

				stream.Write(_lineBytes, 0, 2);
			}
		}
	}
}
