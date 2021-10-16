using Amazon.DynamoDBv2.DataModel;
using MTFHDataLanding.Domain;
using MTFHDataLanding.Factories;
using MTFHDataLanding.Gateway.Interfaces;
using MTFHDataLanding.Infrastructure;
using Hackney.Core.Logging;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace MTFHDataLanding.Gateway
{
    public class DynamoDbEntityGateway : IDbEntityGateway
    {
        private readonly IDynamoDBContext _dynamoDbContext;
        private readonly ILogger<DynamoDbEntityGateway> _logger;

        public DynamoDbEntityGateway(IDynamoDBContext dynamoDbContext, ILogger<DynamoDbEntityGateway> logger)
        {
            _logger = logger;
            _dynamoDbContext = dynamoDbContext;
        }

        [LogCall]
        public async Task<DomainEntity> GetEntityAsync(Guid id)
        {
            _logger.LogDebug($"Calling IDynamoDBContext.LoadAsync for id {id}");
            var dbEntity = await _dynamoDbContext.LoadAsync<DbEntity>(id).ConfigureAwait(false);
            return dbEntity?.ToDomain();
        }

        [LogCall]
        public async Task SaveEntityAsync(DomainEntity entity)
        {
            _logger.LogDebug($"Calling IDynamoDBContext.SaveAsync for id {entity.Id}");
            await _dynamoDbContext.SaveAsync(entity.ToDatabase()).ConfigureAwait(false);
        }
    }
}
