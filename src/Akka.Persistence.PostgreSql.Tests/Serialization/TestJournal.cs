// -----------------------------------------------------------------------
//  <copyright file="Model.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Text;
using Akka.Actor;
using Akka.Persistence.Journal;
using Akka.Serialization;
using Akka.Util;

namespace Akka.Persistence.PostgreSql.Tests.Serialization
{
    internal static class TestJournal
    {
        public class MyPayload
        {
            public MyPayload(string data) => Data = data;

            public string Data { get; }
        }

        public class MyPayload2
        {
            public MyPayload2(string data, int n)
            {
                Data = data;
                N = n;
            }

            public string Data { get; }
            public int N { get; }
        }

        public class MyPayload3
        {
            public MyPayload3(string data) => Data = data;

            public string Data { get; }
        }

        public class MyPayloadSerializer : Serializer
        {
            public MyPayloadSerializer(ExtendedActorSystem system) : base(system) { }

            public override int Identifier => 77123;
            public override bool IncludeManifest => true;

            public override byte[] ToBinary(object obj)
            {
                if (obj is MyPayload myPayload) return Encoding.UTF8.GetBytes("." + myPayload.Data);
                if (obj is MyPayload3 myPayload3) return Encoding.UTF8.GetBytes("." + myPayload3.Data);
                throw new ArgumentException($"Can't serialize object of type [{obj.GetType()}] in [{nameof(MyPayloadSerializer)}]");
            }

            public override object FromBinary(byte[] bytes, Type type)
            {
                if (type == typeof(MyPayload)) return new MyPayload($"{Encoding.UTF8.GetString(bytes)}.");
                if (type == typeof(MyPayload3)) return new MyPayload3($"{Encoding.UTF8.GetString(bytes)}.");
                throw new ArgumentException($"Unimplemented deserialization of message with manifest [{type}] in serializer {nameof(MyPayloadSerializer)}");
            }
        }

        public class MyPayload2Serializer : SerializerWithStringManifest
        {
            private readonly string _manifestV1 = typeof(MyPayload).TypeQualifiedName();
            private readonly string _manifestV2 = "MyPayload-V2";

            public MyPayload2Serializer(ExtendedActorSystem system) : base(system)
            {
            }

            public override int Identifier => 77125;

            public override byte[] ToBinary(object obj)
            {
                if (obj is MyPayload2)
                    return Encoding.UTF8.GetBytes(string.Format(".{0}:{1}", ((MyPayload2)obj).Data, ((MyPayload2)obj).N));
                return null;
            }

            public override string Manifest(object o)
            {
                return _manifestV2;
            }

            public override object FromBinary(byte[] bytes, string manifest)
            {
                if (manifest.Equals(_manifestV2))
                {
                    var parts = Encoding.UTF8.GetString(bytes).Split(':');
                    return new MyPayload2(parts[0] + ".", int.Parse(parts[1]));
                }
                if (manifest.Equals(_manifestV1))
                    return new MyPayload2(Encoding.UTF8.GetString(bytes) + ".", 0);
                throw new ArgumentException("unexpected manifest " + manifest);
            }
        }

        public class MyWriteAdapter : IWriteEventAdapter
        {
            public string Manifest(object evt)
            {
                switch (evt)
                {
                    case MyPayload3 p when p.Data.Equals("item1"):
                        return "First-Manifest";
                    default:
                        return string.Empty;
                }
            }
            
            public object ToJournal(object evt)
            {
                return evt;
            }
        }
    }
}