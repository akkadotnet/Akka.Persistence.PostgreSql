// -----------------------------------------------------------------------
//  <copyright file="StoredAsExtensions.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.CompilerServices;
using Akka.Configuration;

namespace Akka.Persistence.PostgreSql
{
    public static class StoredAsTypeExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static StoredAsType ToStoredAsType(this string type)
        {
            if (!Enum.TryParse(type, true, out StoredAsType storedAs))
            {
                throw new ConfigurationException($"Value [{type}] is not valid. Valid values: bytea, json, jsonb.");
            }
            return storedAs;
        }

        public static StoredAsType GetStoredAsType(this Config config, string key)
            => config.GetString(key).ToStoredAsType();

        public static string ToHocon(StoredAsType? type)
        {
            if (type is null)
                throw new ArgumentNullException(nameof(type));
            return type.ToString().ToLowerInvariant();
        }
        
    }
}