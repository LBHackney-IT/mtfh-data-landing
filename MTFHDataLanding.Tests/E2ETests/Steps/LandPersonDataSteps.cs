using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.TestUtilities;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Infrastructure.Exceptions;
using FluentAssertions;
using Moq;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using AutoFixture;
using Microsoft.Extensions.DependencyInjection;
using MTFHDataLanding.Gateway.Interfaces;
using Xunit;

namespace MTFHDataLanding.Tests.E2ETests.Steps
{
    public class LandPersonDataSteps : BaseSteps
    {
        private readonly Fixture _fixture = new Fixture();
        private Exception _lastException;
        protected readonly Guid _correlationId = Guid.NewGuid();
        private ServiceCollection _services;
        private ILambdaContext _lambdaContext;

        public LandPersonDataSteps()
        {
            _services = new ServiceCollection();
            _services.AddScoped<IAmazonS3>(x => CreateTestS3Client());

            var mockLambdaLogger = new Mock<ILambdaLogger>();
            _lambdaContext = new TestLambdaContext()
            {
                Logger = mockLambdaLogger.Object
            };
        }

        public async Task WhenTheFunctionIsTriggered(Guid personId, bool personExists = true)
        {
            if (personExists)
            {
                _services.AddSingleton<IPersonApi, PersonApiStub>();
            }

            var sqsEvent = _fixture.Build<SQSEvent>()
                                   .With(x => x.Records, new List<SQSEvent.SQSMessage> { CreateMessage(personId) })
                                   .Create();

            Func<Task> func = async () =>
            {
                var fn = new SqsFunction(_services);
                await fn.FunctionHandler(sqsEvent, _lambdaContext).ConfigureAwait(false);
            };

            _lastException = await Record.ExceptionAsync(func);
        }
        public void ThenTheCorrelationIdWasUsedInTheApiCall(string receivedCorrelationId)
        {
            receivedCorrelationId.Should().Be(_correlationId.ToString());
        }

        public void ThenAPersonNotFoundExceptionIsThrown(Guid tenureId)
        {
            _lastException.Should().NotBeNull();
            _lastException.Should().BeOfType(typeof(PersonNotFoundException));
            (_lastException as PersonNotFoundException).Id.Should().Be(tenureId);
        }

        #region Private Methods

        private SQSEvent.SQSMessage CreateMessage(Guid personId, string eventType = EventTypes.PersonCreatedEvent)
        {
            var personSns = _fixture.Build<EntityEventSns>()
                .With(x => x.EntityId, personId)
                .With(x => x.EventType, eventType)
                .With(x => x.CorrelationId, _correlationId)
                .Create();

            var msgBody = JsonSerializer.Serialize(personSns, _jsonOptions);
            return _fixture.Build<SQSEvent.SQSMessage>()
                .With(x => x.Body, msgBody)
                .With(x => x.MessageAttributes, new Dictionary<string, SQSEvent.MessageAttribute>())
                .Create();
        }

        private IAmazonS3 CreateTestS3Client()
        {
            return new AmazonS3Client(new BasicAWSCredentials("A", "B"), new AmazonS3Config
            {
                ServiceURL = "http://localhost:4566",
                ForcePathStyle = true
            });
        }

        #endregion Private Methods
    }
}
