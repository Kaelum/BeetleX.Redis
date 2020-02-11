﻿using System;
using System.Collections.Generic;
using System.Text;

namespace BeetleX.Redis.Commands
{
    public class PERSIST : Command
    {

        public PERSIST(string key)
        {
            Key = key;
        }

        public string Key { get; set; }

        public override bool Read => false;

        public override string Name => "PERSIST";

        public override void OnExecute()
        {
            base.OnExecute();
            AddText(Key);
        }
    }
}
