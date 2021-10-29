using Microsoft.Extensions.Logging;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Gateway.Interfaces;
using MTFHDataLanding.Infrastructure.Exceptions;
using MTFHDataLanding.UseCase.Interfaces;
using Hackney.Core.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using System.Linq;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using ChoETL;
using Hackney.Shared.Person.Boundary.Response;
using Hackney.Shared.Person.Domain;
using MTFHDataLanding.Helpers;
using Newtonsoft.Json;

namespace MTFHDataLanding.UseCase
{
    public class LandPersonData : ILandPersonData
    {
        private readonly IPersonApi _personApi;
        private readonly ILogger<LandPersonData> _logger;
        private readonly IAmazonS3 _s3Client;

        public LandPersonData(IPersonApi personApi,
            ILogger<LandPersonData> logger,
            IAmazonS3 s3Client)
        {
            _personApi = personApi;
            _logger = logger;
            _s3Client = s3Client;
        }

        [LogCall]
        public async Task ProcessMessageAsync(EntityEventSns message)
        {
            var fileTransferUtility = new TransferUtility(_s3Client);

            if (message is null) throw new ArgumentNullException(nameof(message));

            // 1. Get Person from Person service API
            var person = await _personApi.GetPersonByIdAsync(message.EntityId, message.CorrelationId)
                                         .ConfigureAwait(false);
            if (person is null) throw new PersonNotFoundException(message.EntityId);

            var choPerson = new PersonResponseObjectParquet
            {
                FirstName = person.FirstName,
                DateOfBirth = person.DateOfBirth,
                Id = person.Id,
                Links = person.Links,
                MiddleName = person.MiddleName,
                PersonTypes = person.PersonTypes,
                PlaceOfBirth = person.PlaceOfBirth,
                PreferredFirstName = person.PreferredFirstName,
                PreferredMiddleName = person.PreferredMiddleName,
                PreferredSurname = person.PreferredSurname,
                PreferredTitle = person.PreferredTitle,
                Reason = person.Reason,
                Surname = person.Surname,
                Tenures = person.Tenures,
                Title = person.Title,
            };

            //using (var jsonReader = ChoJSONReader.LoadText(JsonConvert.SerializeObject(person))
            //    .WithField("FirstName")
            //    .WithField("DateOfBirth")
            //    .WithField("Id")
            //    .WithField("Links")
            //    .WithField("MiddleName")
            //    .WithField("PlaceOfBirth")
            //    .WithField("PreferredFirstName")
            //    .WithField("PreferredMiddleName")
            //    .WithField("PreferredSurname")
            //    .WithField("PreferredTitle")
            //    .WithField("Reason")
            //    .WithField("Surname")
            //    .WithField("Title")
            //    .WithField("PersonTypes", customSerializer: o => o.ToNString().Replace(Environment.NewLine, String.Empty).Replace("  ", String.Empty))
            //    .WithField("Tenures", customSerializer: o => o.ToNString().Replace(Environment.NewLine, String.Empty).Replace("  ", String.Empty))
            //    .WithField("Links", customSerializer: o => o.ToNString().Replace(Environment.NewLine, String.Empty).Replace("  ", String.Empty))
            //)
            //{
            //    using (var sw = new FileStream("test.parquet", FileMode.Create))
            //    using (var w = new ChoParquetWriter(sw))
            //    {
            //        w.Write(jsonReader);
            //        _logger.LogWarning($"Person record (id: {person.Id}): ");
            //    }
            //}

            using (var jsonReader = ChoJSONReader.LoadText(JsonConvert.SerializeObject(person))
                .WithField("FirstName")
                .WithField("DateOfBirth")
                .WithField("Id")
                .WithField("Links")
                .WithField("MiddleName")
                .WithField("PlaceOfBirth")
                .WithField("PreferredFirstName")
                .WithField("PreferredMiddleName")
                .WithField("PreferredSurname")
                .WithField("PreferredTitle")
                .WithField("Reason")
                .WithField("Surname")
                .WithField("Title")
                .WithField("PersonTypes", customSerializer: o => o.ToNString().Replace(Environment.NewLine, String.Empty).Replace("  ", String.Empty))
                .WithField("Tenures", customSerializer: o => o.ToNString().Replace(Environment.NewLine, String.Empty).Replace("  ", String.Empty))
                .WithField("Links", customSerializer: o => o.ToNString().Replace(Environment.NewLine, String.Empty).Replace("  ", String.Empty))
            )
            {
                using (var memoryStream = new MemoryStream())
                {
                    using (var w = new ChoParquetWriter(memoryStream))
                    {
                        w.Write(jsonReader);
                        _logger.LogWarning($"Person record (id: {person.Id}): ");
                    }

                    string year = message.DateTime.ToString("yyyy");
                    string month = message.DateTime.ToString("MM");
                    string day = message.DateTime.ToString("dd");

                    PutObjectRequest putRequest = new PutObjectRequest
                    {
                        BucketName = Constants.BUCKET_NAME,
                        Key = Constants.KEY_NAME + "year=" + year + "/month=" + month + "/day=" + day + "/" + message.DateTime.ToString("HH\\:mm\\:ss.fffffff") + ".parquet",
                        InputStream = memoryStream
                    };

                    try
                    {
                        await _s3Client.PutObjectAsync(putRequest);
                    }
                    catch (Exception ex)
                    {
                        var x = ex;
                    }
                }
            }
        }
    }

    public class PersonResponseObjectParquet
    {
        public Guid Id { get; set; }

        public Hackney.Shared.Person.Domain.Title? Title { get; set; }

        public Hackney.Shared.Person.Domain.Title? PreferredTitle { get; set; }

        public string PreferredFirstName { get; set; }

        public string PreferredSurname { get; set; }

        public string PreferredMiddleName { get; set; }

        public string FirstName { get; set; }

        public string MiddleName { get; set; }

        public string Surname { get; set; }

        public string PlaceOfBirth { get; set; }

        public string DateOfBirth { get; set; }

        public string Reason { get; set; }

        public IEnumerable<PersonType> PersonTypes { get; set; }

        public IEnumerable<ApiLink> Links { get; set; }

        public IEnumerable<TenureResponseObject> Tenures { get; set; }
    }
}
