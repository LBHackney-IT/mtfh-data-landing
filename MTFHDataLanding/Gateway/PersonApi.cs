using Hackney.Core.Logging;
using Hackney.Shared.Person.Boundary.Response;
using System;
using System.Threading.Tasks;
using MTFHDataLanding.Gateway.Interfaces;

namespace MTFHDataLanding.Gateway
{
    public class PersonApi : IPersonApi
    {
        private const string ApiName = "Person";
        private const string PersonApiUrl = "PersonApiUrl";
        private const string PersonApiToken = "PersonApiToken";

        private readonly IApiGateway _apiGateway;

        public PersonApi(IApiGateway apiGateway)
        {
            _apiGateway = apiGateway;
            _apiGateway.Initialise(ApiName, PersonApiUrl, PersonApiToken);
        }

        [LogCall]
        public async Task<PersonResponseObject> GetPersonByIdAsync(Guid id, Guid correlationId)
        {
            var route = $"{_apiGateway.ApiRoute}/persons/{id}";
            return await _apiGateway.GetByIdAsync<PersonResponseObject>(route, id, correlationId);
        }
    }
}
