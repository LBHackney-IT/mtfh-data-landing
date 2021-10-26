using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;

namespace MTFHDataLanding.Interfaces
{
    public interface IMessageProcessor
    {
        Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context);
    }
}
