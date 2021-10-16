using MTFHDataLanding.Domain;
using System;
using System.Threading.Tasks;

namespace MTFHDataLanding.Gateway.Interfaces
{
    public interface IDbEntityGateway
    {
        Task<DomainEntity> GetEntityAsync(Guid id);
        Task SaveEntityAsync(DomainEntity entity);
    }
}
