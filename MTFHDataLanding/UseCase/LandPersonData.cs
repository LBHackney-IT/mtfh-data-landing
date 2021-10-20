using Microsoft.Extensions.Logging;
using MTFHDataLanding.Boundary;
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
            String keyName = "landing/mtfh/persons/";
            RegionEndpoint bucketRegion = RegionEndpoint.EUWest2;

            IAmazonS3 s3Client = new AmazonS3Client(bucketRegion);
            var fileTransferUtility = new TransferUtility(s3Client);

            if (message is null) throw new ArgumentNullException(nameof(message));

            // 1. Get Person from Person service API
            var person = await _personApi.GetPersonByIdAsync(message.EntityId, message.CorrelationId)
                                         .ConfigureAwait(false);
            if (person is null) throw new PersonNotFoundException(message.EntityId);

            _logger.LogWarning($"Person record (id: {person.Id}): ");
            var intsPersonTypes = new int[person.PersonTypes.Count()];
            for (int i = 1; i < person.PersonTypes.Count(); i++)
            {
                intsPersonTypes[i] = 1;
            }
            var newPersonTypes = person.PersonTypes.ToArray().Select(t => t.ToString()).ToArray();

            var id = new DataColumn(new DataField<string>("id"), new string[] { person.Id.ToString() });
            var title = new DataColumn(new DataField<string>("title"), new string[] { person.Title.ToString() });
            var preferredTitle = new DataColumn(new DataField<string>("preferredTitle"), new string[] { person.PreferredTitle.ToString() });
            var preferredFirstName = new DataColumn(new DataField<string>("preferredFirstName"), new string[] { person.PreferredFirstName });
            var preferredMiddleName = new DataColumn(new DataField<string>("preferredMiddleName"), new string[] { person.PreferredMiddleName });
            var preferredSurname = new DataColumn(new DataField<string>("preferredSurname"), new string[] { person.PreferredSurname });
            var firstName = new DataColumn(new DataField<string>("firstName"), new string[] { person.FirstName });
            var middleName = new DataColumn(new DataField<string>("middleName"), new string[] { person.MiddleName });
            var surname = new DataColumn(new DataField<string>("surname"), new string[] { person.Surname });
            var placeOfBirth = new DataColumn(new DataField<string>("placeOfBirth"), new string[] { person.PlaceOfBirth });
            var dateOfBirth = new DataColumn(new DataField<string>("dateOfBirth"), new string[] { person.DateOfBirth });
            var personTypes = new DataColumn(new DataField<IEnumerable<string>>("personTypes"), newPersonTypes, intsPersonTypes);
            var reason = new DataColumn(new DataField<string>("reason"), new string[] { person.Reason });
            var dateTime = new DataColumn(new DataField<string>("dateTime"), new string[] { message.DateTime.ToString("o") });
            var userName = new DataColumn(new DataField<string>("userName"), new string[] { message.User.Name });
            var userEmail = new DataColumn(new DataField<string>("userEmail"), new string[] { message.User.Email });
            var eventType = new DataColumn(new DataField<string>("eventType"), new string[] { message.EventType });

            var schema = new Schema(id.Field, title.Field, preferredTitle.Field, preferredFirstName.Field, preferredMiddleName.Field,
            preferredSurname.Field, firstName.Field, middleName.Field, surname.Field, placeOfBirth.Field, dateOfBirth.Field, personTypes.Field,
            reason.Field, dateTime.Field, new StructField("user", userName.Field, userEmail.Field), eventType.Field);

            using (MemoryStream ms = new MemoryStream())
            {
                using (var parquetWriter = new ParquetWriter(schema, ms))
                {
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        groupWriter.WriteColumn(id);
                        groupWriter.WriteColumn(title);
                        groupWriter.WriteColumn(preferredTitle);
                        groupWriter.WriteColumn(preferredFirstName);
                        groupWriter.WriteColumn(preferredMiddleName);
                        groupWriter.WriteColumn(preferredSurname);
                        groupWriter.WriteColumn(firstName);
                        groupWriter.WriteColumn(middleName);
                        groupWriter.WriteColumn(surname);
                        groupWriter.WriteColumn(placeOfBirth);
                        groupWriter.WriteColumn(dateOfBirth);
                        groupWriter.WriteColumn(personTypes);
                        groupWriter.WriteColumn(reason);
                        groupWriter.WriteColumn(dateTime);
                        groupWriter.WriteColumn(userName);
                        groupWriter.WriteColumn(userEmail);
                        groupWriter.WriteColumn(eventType);
                    }
                }
                string year = message.DateTime.ToString("yyyy");
                string month = message.DateTime.ToString("MM");
                string day = message.DateTime.ToString("dd");
                await fileTransferUtility.UploadAsync(ms, bucketName, keyName + "year=" + year + "/month=" + month + "/day=" + day + "/" +
                message.DateTime.ToString("HH\\:mm\\:ss.fffffff") + ".parquet");
            }
        }
    }
}
