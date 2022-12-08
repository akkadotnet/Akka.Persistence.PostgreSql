// -----------------------------------------------------------------------
//  <copyright file="PostgreTest.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Text;
using Akka.Actor;
using Akka.Serialization;

namespace Akka.Persistence.PostgreSql.Tests.Serialization
{
    internal static class Test
    {
        public class MySnapshot
        {
            public MySnapshot(string data)
            {
                Data = data;
            }

            public string Data { get; }
        }

        public class MySnapshot2
        {
            public MySnapshot2(string data)
            {
                Data = data;
            }

            public string Data { get; }
        }

        public class MySnapshotSerializer : Serializer
        {
            public MySnapshotSerializer(ExtendedActorSystem system) : base(system) { }
            public override int Identifier => 77124;
            public override bool IncludeManifest => true;

            public override byte[] ToBinary(object obj)
            {
                if (obj is MySnapshot snapshot) return Encoding.UTF8.GetBytes($".{snapshot.Data}");
                throw new ArgumentException($"Can't serialize object of type [{obj.GetType()}] in [{nameof(MySnapshotSerializer2)}]");
            }

            public override object FromBinary(byte[] bytes, Type type)
            {
                if (type == typeof(MySnapshot)) return new MySnapshot($"{Encoding.UTF8.GetString(bytes)}.");
                throw new ArgumentException($"Unimplemented deserialization of message with manifest [{type}] in serializer {nameof(MySnapshotSerializer)}");
            }
        }

        public class MySnapshotSerializer2 : SerializerWithStringManifest
        {
            private const string ContactsManifest = "A";

            public MySnapshotSerializer2(ExtendedActorSystem system) : base(system) { }
            public override int Identifier => 77126;

            public override byte[] ToBinary(object obj)
            {
                if (obj is MySnapshot2 snapshot) return Encoding.UTF8.GetBytes($".{snapshot.Data}");
                throw new ArgumentException($"Can't serialize object of type [{obj.GetType()}] in [{nameof(MySnapshotSerializer2)}]");
            }

            public override string Manifest(object obj)
            {
                if (obj is MySnapshot2) return ContactsManifest;
                throw new ArgumentException($"Can't serialize object of type [{obj.GetType()}] in [{nameof(MySnapshotSerializer2)}]");
            }

            public override object FromBinary(byte[] bytes, string manifest)
            {
                if (manifest == ContactsManifest) return new MySnapshot2(Encoding.UTF8.GetString(bytes) + ".");
                throw new ArgumentException($"Unimplemented deserialization of message with manifest [{manifest}] in serializer {nameof(MySnapshotSerializer2)}");
            }
        }
    }
}