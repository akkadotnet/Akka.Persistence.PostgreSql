using Akka.Serialization;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Akka.Persistence.PostgreSql
{
    internal class SerializationResult
    {
        public SerializationResult(NpgsqlDbType dbType, object payload, Serializer serializer)
        {
            DbType = dbType;
            Payload = payload;
            Serializer = serializer;
        }

        public NpgsqlDbType DbType { get; private set; }
        public object Payload { get; private set; }
        public Serializer Serializer { get; private set; }

    }
}

