using Amazon.Lambda.SQSEvents;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.UseCase.Interfaces;

namespace MTFHDataLanding.Interfaces
{
    public interface ITenureDataFactory
    {
        IMessageProcessing CreateMessagingProcessor(EntityEventSns entityEvent, SQSEvent.SQSMessage message);
    }
}
