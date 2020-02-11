using System;

namespace BeetleX.Redis
{
    public enum ResultType
    {
        Simple,
        Error,
        Integers,
        Bulck,
        Arrays,
        NetError,
        DataError,
        Object,
        String,
        Null,
        NotFound
    }

    public enum ResultStatus
    {
        None,
        Loading,
        Completed
    }
}
