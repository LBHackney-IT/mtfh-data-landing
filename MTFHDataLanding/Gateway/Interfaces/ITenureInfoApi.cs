using Hackney.Shared.Tenure.Boundary.Response;
using System;
using System.Threading.Tasks;

namespace MTFHDataLanding.Gateway.Interfaces
{
    public interface ITenureInfoApi
    {
        Task<TenureResponseObject> GetTenureInfoByIdAsync(Guid id, Guid correlationId);
    }
}
