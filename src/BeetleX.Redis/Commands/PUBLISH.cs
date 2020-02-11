﻿using System;
using System.Collections.Generic;
using System.Text;

namespace BeetleX.Redis.Commands
{
    public class PUBLISH : Command
    {

        public PUBLISH(string channel, object message, IDataFormater dataFormater)
        {
            DataFormater = dataFormater;
            Channel = channel;
            Message = message;
        }

        public string Channel { get; set; }

        public object Message { get; set; }

        public override bool Read => false;

        public override string Name => "PUBLISH";

        public override void OnExecute()
        {
            base.OnExecute();
            AddText(Channel);
            AddData(Message);
        }
    }
}
