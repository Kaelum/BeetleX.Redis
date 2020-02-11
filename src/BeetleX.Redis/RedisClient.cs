using System;

using BeetleX.Buffers;
using BeetleX.Clients;

namespace BeetleX.Redis
{
	public class RedisClient
	{
		public RedisClient(bool ssl, string host, int port = 6379)
		{
			if (ssl)
			{
				TcpClient = BeetleX.SocketFactory.CreateSslClient<AsyncTcpClient>(host, port, "beetlex");

				TcpClient.CertificateValidationCallback = (o, e, f, d) =>
				{
					return true;
				};
			}
			else
			{
				TcpClient = BeetleX.SocketFactory.CreateClient<AsyncTcpClient>(host, port);
			}
		}

		public AsyncTcpClient TcpClient { get; private set; }

		internal SerializerExpand SerializerExpand
		{
			get
			{
				return SerializerExpand.Pop();
			}
		}

		public void Send(Command cmd)
		{
			PipeStream stream = TcpClient.Stream.ToPipeStream();

			cmd.Execute(this, stream);

			TcpClient.Stream.Flush();
		}
	}
}
