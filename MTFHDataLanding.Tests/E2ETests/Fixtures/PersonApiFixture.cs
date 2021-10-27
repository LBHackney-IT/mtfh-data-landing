using AutoFixture;
using Hackney.Shared.Person.Boundary.Response;
using Hackney.Shared.Person.Domain;
using System;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using MTFHDataLanding.Helpers;

namespace MTFHDataLanding.Tests.E2ETests.Fixtures
{
    public class PersonApiFixture : BaseApiFixture<PersonResponseObject>
    {
        private readonly Fixture _fixture = new Fixture();
        public static string PersonApiRoute => "http://localhost:5678/api/v1/";
        public static string PersonApiToken => "sdjkhfgsdkjfgsdjfgh";

        public PersonApiFixture()
            : base(PersonApiRoute, PersonApiToken)
        {
            Environment.SetEnvironmentVariable("PersonApiUrl", PersonApiRoute);
            Environment.SetEnvironmentVariable("PersonApiToken", PersonApiToken);

            try
            {
                var bucketResult = CreateTestS3Client().PutBucketAsync(new PutBucketRequest
                {
                    BucketName = Constants.BUCKET_NAME,
                    UseClientRegion = true
                }).Result;
            }
            catch
            {
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_disposed)
            {
                base.Dispose(disposing);
            }
        }

        public PersonResponseObject GivenThePersonExistsWithMultipleTenures(Guid personId, int numberOfTenures)
        {
            return GivenThePersonExistsWithMultipleTenures(personId, numberOfTenures, PersonType.Tenant);
        }
        public PersonResponseObject GivenThePersonExistsWithMultipleTenures(Guid personId, int numberOfTenures, PersonType personType)
        {
            ResponseObject = _fixture.Build<PersonResponseObject>()
                                      .With(x => x.Id, personId)
                                      .With(x => x.PersonTypes, new PersonType[] { personType })
                                      .With(x => x.DateOfBirth, DateTime.UtcNow.AddYears(-30).ToString("yyyy-MM-ddTHH\\:mm\\:ss.fffffffZ"))
                                      .With(x => x.Tenures, _fixture.CreateMany<TenureResponseObject>(numberOfTenures))
                                      .Create();
            return ResponseObject;
        }

        public void GivenThePersonDoesNotExist(Guid personId)
        {
            // Nothing to do here
        }

        public PersonResponseObject GivenThePersonExists(Guid personId)
        {
            return GivenThePersonExists(personId, true, PersonType.Tenant);
        }

        public PersonResponseObject GivenThePersonExists(Guid personId, bool hasTenure, PersonType personType)
        {
            int numberOfTenures = hasTenure ? 1 : 0;
            return GivenThePersonExistsWithMultipleTenures(personId, numberOfTenures, personType);
        }
        private IAmazonS3 CreateTestS3Client()
        {
            return new AmazonS3Client(new BasicAWSCredentials("A", "B"), new AmazonS3Config
            {
                ServiceURL = "http://localhost:4566",
                ForcePathStyle = true
            });
        }
    }
}
