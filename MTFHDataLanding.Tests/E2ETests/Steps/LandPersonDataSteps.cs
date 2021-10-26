using Amazon.DynamoDBv2.DataModel;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.TestUtilities;
using Hackney.Shared.Person.Boundary.Response;
using Hackney.Shared.Person.Domain;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Infrastructure;
using MTFHDataLanding.Infrastructure.Exceptions;
using FluentAssertions;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using AutoFixture;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace MTFHDataLanding.Tests.E2ETests.Steps
{
    public class LandPersonDataSteps : BaseSteps
    {
        private readonly Fixture _fixture = new Fixture();
        private Exception _lastException;
        protected readonly Guid _correlationId = Guid.NewGuid();
        public LandPersonDataSteps()
        { }

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
        public async Task WhenTheFunctionIsTriggered(Guid personId)
        {
            var mockLambdaLogger = new Mock<ILambdaLogger>();
            ILambdaContext lambdaContext = new TestLambdaContext()
            {
                Logger = mockLambdaLogger.Object
            };

            var sqsEvent = _fixture.Build<SQSEvent>()
                                   .With(x => x.Records, new List<SQSEvent.SQSMessage> { CreateMessage(personId) })
                                   .Create();

            Func<Task> func = async () =>
            {
                var services = new ServiceCollection();
                services.TryAddScoped<IAmazonS3>(x =>
                {
                    return new AmazonS3Client(new AmazonS3Config
                    {
                        ServiceURL = "http://localhost:4566"
                    });
                });
                var fn = new SqsFunction(services);
                await fn.FunctionHandler(sqsEvent, lambdaContext).ConfigureAwait(false);
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
    }
}
