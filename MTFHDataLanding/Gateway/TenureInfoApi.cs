using Hackney.Core.Logging;
using Hackney.Shared.Tenure.Boundary.Response;
using MTFHDataLanding.Gateway.Interfaces;
using System;
using System.Threading.Tasks;

namespace MTFHDataLanding.Gateway
{
    public class TenureInfoApi : ITenureInfoApi
    {
        private const string ApiName = "Tenure";
        private const string TenureApiUrl = "TenureApiUrl";
        private const string TenureApiToken = "TenureApiToken";

        private readonly IApiGateway _apiGateway;

        public TenureInfoApi(IApiGateway apiGateway)
        {
            _apiGateway = apiGateway;
            _apiGateway.Initialise(ApiName, TenureApiUrl, TenureApiToken);
        }

        [LogCall]
        public async Task<TenureResponseObject> GetTenureInfoByIdAsync(Guid id, Guid correlationId)
        {
            var route = $"{_apiGateway.ApiRoute}/tenures/{id}";
            return await _apiGateway.GetByIdAsync<TenureResponseObject>(route, id, correlationId);
        }
    }
}
