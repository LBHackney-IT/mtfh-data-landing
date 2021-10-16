using MTFHDataLanding.Tests.E2ETests.Fixtures;
using MTFHDataLanding.Tests.E2ETests.Steps;
using System;
using TestStack.BDDfy;
using Xunit;

namespace MTFHDataLanding.Tests.E2ETests.Stories
{
    [Story(
        AsA = "SQS Entity Listener",
        IWant = "a function to process the person message",
        SoThat = "The correct details are pushed to s3")]
    public class LandPersonDataTests : IDisposable
    {
        private readonly PersonApiFixture _personApiFixture;

        private readonly LandPersonDataSteps _steps;

        public LandPersonDataTests()
        {
            _personApiFixture = new PersonApiFixture();
            _steps = new LandPersonDataUseCaseSteps();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private bool _disposed;
        protected virtual void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                _personApiFixture.Dispose();
                _disposed = true;
            }
        }

        [Fact]
        public void UpdatedPersonNotFound()
        {
            var personId = Guid.NewGuid();
            this.Given(g => _personApiFixture.GivenThePersonDoesNotExist(personId))
                .When(w => _steps.WhenTheFunctionIsTriggered(personId))
                .Then(t => _steps.ThenTheCorrelationIdWasUsedInTheApiCall(_personApiFixture.ReceivedCorrelationIds.First()))
                .Then(t => _steps.ThenAPersonNotFoundExceptionIsThrown(personId))
                .BDDfy();
        }
    }
}
