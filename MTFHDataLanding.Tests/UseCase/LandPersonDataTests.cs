using AutoFixture;
using Hackney.Shared.Person.Boundary.Response;
using Microsoft.Extensions.Logging;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Gateway.Interfaces;
using MTFHDataLanding.Infrastructure.Exceptions;
using MTFHDataLanding.UseCase;
using FluentAssertions;
using Moq;
using System;
using System.Threading.Tasks;
using Xunit;

namespace MTFHDataLanding.Tests.UseCase
{
    [Collection("LogCall collection")]
    public class LandPersonDataTests
    {
        private readonly Mock<IPersonApi> _mockPersonApi;
        private readonly Mock<ILogger<LandPersonData>> _mockLogger;
        private readonly LandPersonData _sut;

        private readonly EntityEventSns _message;
        private readonly PersonResponseObject _person;

        private readonly Fixture _fixture;
        private static readonly Guid _correlationId = Guid.NewGuid();
        private const string DateTimeFormat = "yyyy-MM-ddTHH\\:mm\\:ss.fffffffZ";

        public LandPersonDataTests()
        {
            _fixture = new Fixture();

            _mockPersonApi = new Mock<IPersonApi>();
            _mockLogger = new Mock<ILogger<LandPersonData>>();
            _sut = new LandPersonData(_mockPersonApi.Object, _mockLogger.Object);

            _message = CreateMessage();
            _person = CreatePerson(_message.EntityId);
        }
        private PersonResponseObject CreatePerson(Guid entityId)
        {
            var tenures = _fixture.CreateMany<TenureResponseObject>(1);
            return _fixture.Build<PersonResponseObject>()
                           .With(x => x.Id, entityId)
                           .With(x => x.Tenures, tenures)
                           .With(x => x.DateOfBirth, DateTime.UtcNow.AddYears(-30).ToString(DateTimeFormat))
                           .Create();
        }

        private EntityEventSns CreateMessage(string eventType = EventTypes.PersonUpdatedEvent)
        {
            return _fixture.Build<EntityEventSns>()
                           .With(x => x.EventType, eventType)
                           .With(x => x.CorrelationId, _correlationId)
                           .Create();
        }

        [Fact]
        public void ProcessMessageAsyncTestNullMessageThrows()
        {
            Func<Task> func = async () => await _sut.ProcessMessageAsync(null).ConfigureAwait(false);
            func.Should().ThrowAsync<ArgumentNullException>();
        }

        [Fact]
        public void ProcessMessageAsyncTestGetPersonExceptionThrown()
        {
            var exMsg = "This is an error";
            _mockPersonApi.Setup(x => x.GetPersonByIdAsync(_message.EntityId, _message.CorrelationId))
                                       .ThrowsAsync(new Exception(exMsg));

            Func<Task> func = async () => await _sut.ProcessMessageAsync(_message).ConfigureAwait(false);
            func.Should().ThrowAsync<Exception>().WithMessage(exMsg);
        }

        [Fact]
        public void ProcessMessageAsyncTestGetPersonReturnsNullThrows()
        {
            _mockPersonApi.Setup(x => x.GetPersonByIdAsync(_message.EntityId, _message.CorrelationId))
                                       .ReturnsAsync((PersonResponseObject) null);

            Func<Task> func = async () => await _sut.ProcessMessageAsync(_message).ConfigureAwait(false);
            func.Should().ThrowAsync<PersonNotFoundException>();
        }
    }
}
