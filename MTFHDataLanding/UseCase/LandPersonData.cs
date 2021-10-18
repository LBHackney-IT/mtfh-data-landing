using Microsoft.Extensions.Logging;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Domain;
using MTFHDataLanding.Gateway.Interfaces;
using MTFHDataLanding.Infrastructure.Exceptions;
using MTFHDataLanding.UseCase.Interfaces;
using Hackney.Core.Logging;
using System;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Data;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;

namespace MTFHDataLanding.UseCase
{
    public class LandPersonData : ILandPersonData
    {
        private readonly IPersonApi _personApi;
        private readonly ILogger<LandPersonData> _logger;

        public LandPersonData(IPersonApi personApi,
            ILogger<LandPersonData> logger)
        {
            _personApi = personApi;
            _logger = logger;
        }

        [LogCall]
        public async Task ProcessMessageAsync(EntityEventSns message)
        {
            String bucketName = "mtfh-data-landing-spike";
            String keyName = "landing/persons/";
            RegionEndpoint bucketRegion = RegionEndpoint.EUWest2;

            IAmazonS3 s3Client = new AmazonS3Client(bucketRegion);
            var fileTransferUtility = new TransferUtility(s3Client);

            if (message is null) throw new ArgumentNullException(nameof(message));

            // 1. Get Person from Person service API
            var person = await _personApi.GetPersonByIdAsync(message.EntityId, message.CorrelationId)
                                         .ConfigureAwait(false);
            if (person is null) throw new PersonNotFoundException(message.EntityId);

            _logger.LogWarning($"Person record (id: {person.Id}): " + person);
            var ds = new DataSet(
                        new DataField<string>("id"),
                        new DataField<string>("title"),
                        new DataField<string>("preferredTitle"),
                        new DataField<string>("preferredFirstName"),
                        new DataField<string>("preferredMiddleName"),
                        new DataField<string>("preferredSurname"),
                        new DataField<string>("firstName"),
                        new DataField<string>("middleName"),
                        new DataField<string>("surname"),
                        new DataField<string>("placeOfBirth"),
                        new DataField<string>("dateOfBirth"),
                        new DataField<string>("reason"),
                        new DataField<string>("dateTime")
                    );
            ds.Add(person.Id, person.Title, person.PreferredTitle, person.PreferredFirstName, person.PreferredMiddleName, person.PreferredSurname,
                   person.FirstName, person.MiddleName, person.Surname, person.PlaceOfBirth, person.DateOfBirth, person.Reason, message.DateTime);

            using (MemoryStream ms = new MemoryStream())
            {
                using (var writer = new ParquetWriter(ds.GetXmlSchema(), ms))
                {
                    writer.Write(ds);
                }
                await fileTransferUtility.UploadAsync(ms, bucketName, keyName + message.DateTime);
            }
        }
    }
}
