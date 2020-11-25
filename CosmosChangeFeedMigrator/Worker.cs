using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace CosmosChangeFeedMigrator
{
    public class Worker : IHostedService
    {
        private readonly CosmosConfiguration _configuration;
        private readonly ILogger<Worker> _logger;
        private ChangeFeedProcessor _changeFeedProcessor;
        private Container _destinationContainer;

        private const string LeaseContainerPartitionPathKeyPath = "/id";

        private const string TenantToMigrate = "Tenant1";
        private const string DestinationContainer = "Tenant1-Premium";
        private const int DestinationContainerThroughput = 400;

        public Worker(CosmosConfiguration configuration, ILogger<Worker> logger)
        {
            _configuration = configuration;
            _logger = logger;
        }
        
        private async Task HandleChangesAsync(IReadOnlyCollection<JObject> changes, CancellationToken cancellationtoken)
        {
            _logger.LogInformation("Processing {Count} changes...", changes.Count);

            foreach (var change in changes)
            {
                _logger.LogInformation("Upserting item {Id}", change.GetValue("id"));
                
                await _destinationContainer.UpsertItemAsync(change, cancellationToken: cancellationtoken)
                    .ConfigureAwait(false);
            }
        }
        
        #region Start

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var cosmosClient =
                new CosmosClient(_configuration.AccountEndpoint, _configuration.AuthKey);

            var databaseName = _configuration.DatabaseName;

            var database = cosmosClient.GetDatabase(_configuration.DatabaseName);
            
            var leaseContainer = await database.CreateContainerIfNotExistsAsync(
                id: _configuration.LeaseContainerName,
                partitionKeyPath: LeaseContainerPartitionPathKeyPath,
                throughput: _configuration.LeaseContainerThroughput,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            var sourceContainer = cosmosClient
                .GetContainer(databaseName, TenantToMigrate);
            var sourceContainerInfo = await sourceContainer.ReadContainerAsync().ConfigureAwait(false);
            
            _changeFeedProcessor = sourceContainer
                .GetChangeFeedProcessorBuilder<JObject>("MigrationChangeFeedProcessor", HandleChangesAsync)
                .WithInstanceName("MigrationChangeFeedProcessor")
                .WithLeaseContainer(leaseContainer)
                .WithStartTime(DateTime.MinValue.ToUniversalTime())
                .Build();

            _destinationContainer = await database.CreateContainerIfNotExistsAsync(
                id: DestinationContainer,
                partitionKeyPath: sourceContainerInfo.Resource.PartitionKeyPath,
                throughput: DestinationContainerThroughput,
                cancellationToken: cancellationToken).ConfigureAwait(false);

            await _changeFeedProcessor.StartAsync().ConfigureAwait(false);
            
            _logger.LogInformation("Change feed processor is ready.");
        }
        
        #endregion

        #region Stop

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _changeFeedProcessor.StopAsync().ConfigureAwait(false);
        }
        
        #endregion
    }
}