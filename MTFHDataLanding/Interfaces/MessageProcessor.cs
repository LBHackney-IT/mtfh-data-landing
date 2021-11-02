using System;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Microsoft.Extensions.Logging;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.UseCase.Interfaces;

namespace MTFHDataLanding.Interfaces
{
    public class MessageProcessor : IMessageProcessor
    {
        private readonly ITenureDataFactory _tenureDataFactory;
        private ILogger<MessageProcessor> Logger { get; }

        public MessageProcessor(ILogger<MessageProcessor> logger, ITenureDataFactory tenureDataFactory)
        {
            _tenureDataFactory = tenureDataFactory;
            Logger = logger;
        }

        public async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
        {
            context.Logger.LogLine($"Processing message {message.MessageId}");

            var entityEvent = JsonSerializer.Deserialize<EntityEventSns>(message.Body, Helpers.Json.CreateJsonOptions());

            using (Logger.BeginScope("CorrelationId: {CorrelationId}", entityEvent.CorrelationId))
            {
                try
                {
                    IMessageProcessing processor = _tenureDataFactory.CreateMessagingProcessor(entityEvent, message);
                    await processor.ProcessMessageAsync(entityEvent).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex, $"Exception processing message id: {message.MessageId}; type: {entityEvent.EventType}; entity id: {entityEvent.EntityId}");
                    throw; // AWS will handle retry/moving to the dead letter queue
                }
            }
        }


    }
}
