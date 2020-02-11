using System;

using BeetleX.Dispatchs;

namespace BeetleX.Redis
{
    class ResultDispatch
    {
        static ResultDispatch()
        {
            DispatchCenter = new DispatchCenter<RedisRequest>(OnExecute);
        }

        public static DispatchCenter<RedisRequest> DispatchCenter { get; set; }

        private static void OnExecute(RedisRequest request)
        {
            request.TaskCompletion();
        }

    }
}
