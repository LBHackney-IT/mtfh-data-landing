using AutoFixture;
using FluentAssertions;
using Hackney.Shared.Person.Boundary.Response;
using Moq;
using System;
using System.Threading.Tasks;
using MTFHDataLanding.Gateway;
using MTFHDataLanding.Gateway.Interfaces;
using Xunit;

namespace MTFHDataLanding.Tests.Gateway
{
    [Collection("LogCall collection")]
    public class PersonApiTests
    {
        private readonly Mock<IApiGateway> _mockApiGateway;

        private static readonly Guid _id = Guid.NewGuid();
        private static readonly Guid _correlationId = Guid.NewGuid();

        private const string ApiName = "Person";
        private const string PersonApiUrlKey = "PersonApiUrl";
        private const string PersonApiTokenKey = "PersonApiToken";

        private const string PersonApiRoute = "https://some-domain.com/api/";
        private const string PersonApiToken = "dksfghjskueygfakseygfaskjgfsdjkgfdkjsgfdkjgf";

        public PersonApiTests()
        {
            _mockApiGateway = new Mock<IApiGateway>();

            _mockApiGateway.SetupGet(x => x.ApiName).Returns(ApiName);
            _mockApiGateway.SetupGet(x => x.ApiRoute).Returns(PersonApiRoute);
            _mockApiGateway.SetupGet(x => x.ApiToken).Returns(PersonApiToken);
        }

        private static string Route => $"{PersonApiRoute}/persons/{_id}";

        [Fact]
        public void ConstructorTestInitialisesApiGateway()
        {
            new PersonApi(_mockApiGateway.Object);
            _mockApiGateway.Verify(x => x.Initialise(ApiName, PersonApiUrlKey, PersonApiTokenKey, null),
                                   Times.Once);
        }

        [Fact]
        public void GetPersonByIdAsyncGetExceptionThrown()
        {
            var exMessage = "This is an exception";
            _mockApiGateway.Setup(x => x.GetByIdAsync<PersonResponseObject>(Route, _id, _correlationId))
                           .ThrowsAsync(new Exception(exMessage));

            var sut = new PersonApi(_mockApiGateway.Object);
            Func<Task<PersonResponseObject>> func =
                async () => await sut.GetPersonByIdAsync(_id, _correlationId).ConfigureAwait(false);

            func.Should().ThrowAsync<Exception>().WithMessage(exMessage);
        }

        [Fact]
        public async Task GetPersonByIdAsyncNotFoundReturnsNull()
        {
            var sut = new PersonApi(_mockApiGateway.Object);
            var result = await sut.GetPersonByIdAsync(_id, _correlationId).ConfigureAwait(false);

            result.Should().BeNull();
        }

        [Fact]
        public async Task GetPersonByIdAsyncCallReturnsPerson()
        {
            var person = new Fixture().Create<PersonResponseObject>();

            _mockApiGateway.Setup(x => x.GetByIdAsync<PersonResponseObject>(Route, _id, _correlationId))
                           .ReturnsAsync(person);

            var sut = new PersonApi(_mockApiGateway.Object);
            var result = await sut.GetPersonByIdAsync(_id, _correlationId).ConfigureAwait(false);

            result.Should().BeEquivalentTo(person);
        }
    }
}
