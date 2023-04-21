using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Akka.Util;
using Docker.DotNet;
using Docker.DotNet.Models;
using Xunit;

namespace Akka.Persistence.PostgreSql.Performance.Tests
{
    [CollectionDefinition("PostgreSqlSpec")]
    public sealed class PostgresSpecsFixture : ICollectionFixture<PostgresFixture>
    {
    }

    /// <summary>
    ///     Fixture used to run PostgresSQL Server
    /// </summary>
    public class PostgresFixture : IAsyncLifetime
    {
        protected readonly string PostgresContainerName = $"postgresSqlServer-{Guid.NewGuid():N}";
        protected DockerClient Client;

        public PostgresFixture()
        {
            DockerClientConfiguration config;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                config = new DockerClientConfiguration(new Uri("unix://var/run/docker.sock"));
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                config = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine"));
            else
                throw new NotSupportedException($"Unsupported OS [{RuntimeInformation.OSDescription}]");

            Client = config.CreateClient();
        }

        protected string ImageName => "postgres";
        protected string Tag => "latest";

        protected string PostgresImageName => $"{ImageName}:{Tag}";

        public string ConnectionString { get; private set; }

        public async Task InitializeAsync()
        {
            var images = await Client.Images.ListImagesAsync(new ImagesListParameters
            {
                Filters = new Dictionary<string, IDictionary<string, bool>>
                {
                    {
                        "reference",
                        new Dictionary<string, bool>
                        {
                            {PostgresImageName, true}
                        }
                    }
                }
            });

            if (images.Count == 0)
                await Client.Images.CreateImageAsync(
                    new ImagesCreateParameters { FromImage = ImageName, Tag = Tag }, null,
                    new Progress<JSONMessage>(message =>
                    {
                        Console.WriteLine(!string.IsNullOrEmpty(message.ErrorMessage)
                            ? message.ErrorMessage
                            : $"{message.ID} {message.Status} {message.ProgressMessage}");
                    }));

            var sqlServerHostPort = ThreadLocalRandom.Current.Next(9000, 10000);

            // create the container
            await Client.Containers.CreateContainerAsync(new CreateContainerParameters
            {
                Image = PostgresImageName,
                Name = PostgresContainerName,
                Tty = true,
                ExposedPorts = new Dictionary<string, EmptyStruct>
                {
                    {"5432/tcp", new EmptyStruct()}
                },
                HostConfig = new HostConfig
                {
                    PortBindings = new Dictionary<string, IList<PortBinding>>
                    {
                        {
                            "5432/tcp",
                            new List<PortBinding>
                            {
                                new PortBinding
                                {
                                    HostPort = $"{sqlServerHostPort}"
                                }
                            }
                        }
                    }
                },
                Env = new[]
                {
                    "POSTGRES_PASSWORD=postgres",
                    "POSTGRES_USER=postgres"
                }
            });

            // start the container
            await Client.Containers.StartContainerAsync(PostgresContainerName, new ContainerStartParameters());

            // Provide a 10 second startup delay
            await Task.Delay(TimeSpan.FromSeconds(10));

            ConnectionString = $"Server=127.0.0.1;Port={sqlServerHostPort};" +
                               "Database=postgres;User Id=postgres;Password=postgres";
        }

        public async Task DisposeAsync()
        {
            if (Client != null)
            {
                await Client.Containers.StopContainerAsync(PostgresContainerName, new ContainerStopParameters());
                await Client.Containers.RemoveContainerAsync(PostgresContainerName,
                    new ContainerRemoveParameters { Force = true });
                Client.Dispose();
            }
        }
    }
}
