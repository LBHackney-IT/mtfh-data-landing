using MTFHDataLanding.Boundary;
using System.Threading.Tasks;

namespace MTFHDataLanding.UseCase.Interfaces
{
    public interface IMessageProcessing
    {
        Task ProcessMessageAsync(EntityEventSns message);
    }
}
