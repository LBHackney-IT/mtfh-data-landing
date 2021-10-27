using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AutoFixture;
using Hackney.Shared.Person.Boundary.Response;
using MTFHDataLanding.Gateway.Interfaces;

namespace MTFHDataLanding.Tests.E2ETests.Steps
{
    public class PersonApiStub : IPersonApi
    {
        private Dictionary<Guid, PersonResponseObject> _existingPersons;
        private readonly Fixture _fixture = new Fixture();

        public PersonApiStub()
        {
            _existingPersons = new Dictionary<Guid, PersonResponseObject>();
        }

        public Task<PersonResponseObject> GetPersonByIdAsync(Guid id, Guid correlationId)
        {
            if (!_existingPersons.ContainsKey(id))
            {
                _existingPersons.Add(id, _fixture.Build<PersonResponseObject>()
                    .With(x => x.Id, id)
                    .Create());
            }

            return Task.FromResult(_existingPersons[id]);
        }
    }
}
