using AutoFixture;
using MTFHDataLanding.Domain;
using MTFHDataLanding.Factories;
using MTFHDataLanding.Infrastructure;
using FluentAssertions;
using Xunit;

namespace MTFHDataLanding.Tests.Factories
{
    public class EntityFactoryTest
    {
        private readonly Fixture _fixture = new Fixture();

        [Fact]
        public void CanMapADatabaseEntityToADomainObject()
        {
            var databaseEntity = _fixture.Create<DbEntity>();
            var entity = databaseEntity.ToDomain();

            databaseEntity.Should().BeEquivalentTo(entity);
        }

        [Fact]
        public void CanMapADomainEntityToADatabaseObject()
        {
            var entity = _fixture.Create<DomainEntity>();
            var databaseEntity = entity.ToDatabase();

            databaseEntity.Should().BeEquivalentTo(entity);
        }
    }
}
