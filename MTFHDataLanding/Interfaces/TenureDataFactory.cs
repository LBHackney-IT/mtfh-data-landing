using System;
using Amazon.Lambda.SQSEvents;
using Microsoft.Extensions.DependencyInjection;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.UseCase.Interfaces;

namespace MTFHDataLanding.Interfaces
{
    public class TenureDataFactory : ITenureDataFactory
    {
        private readonly IServiceProvider _serviceProvider;

        public TenureDataFactory(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public IMessageProcessing CreateMessagingProcessor(EntityEventSns entityEvent, SQSEvent.SQSMessage message)
        {
            switch (entityEvent.EventType)
            {
                case EventTypes.PersonCreatedEvent:
                {
                    return _serviceProvider.GetService<ILandPersonData>();
                }
                case EventTypes.PersonUpdatedEvent:
                {
                    return _serviceProvider.GetService<ILandPersonData>();
                }
                case EventTypes.TenureCreatedEvent:
                {
                    return _serviceProvider.GetService<ILandTenureData>();
                }
                case EventTypes.TenureUpdatedEvent:
                {
                    return _serviceProvider.GetService<ILandTenureData>();
                }
                default:
                    throw new ArgumentException($"Unknown event type: {entityEvent.EventType} on message id: {message.MessageId}");
            }
        }
    }
}
