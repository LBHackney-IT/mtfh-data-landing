using Microsoft.Extensions.Logging;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Gateway.Interfaces;
using MTFHDataLanding.Infrastructure.Exceptions;
using MTFHDataLanding.UseCase.Interfaces;
using Hackney.Core.Logging;
using System;
using System.Threading.Tasks;
using System.IO;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using ChoETL;
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

            using (var jsonReader = new ChoJSONReader(new JsonTextReader(new StringReader(JsonConvert.SerializeObject(person)))))
            {
                using (var memoryStream = new MemoryStream())
                {
                    using (var w = new ChoParquetWriter(memoryStream))
                    {
                        w.Write(jsonReader);
                        _logger.LogWarning($"Person record (id: {person.Id}): ");

                        //await fileTransferUtility.UploadAsync(memoryStream, Constants.BUCKET_NAME, Constants.KEY_NAME + "year=" + year + "/month=" + month + "/day=" + day + "/" +
                        //    message.DateTime.ToString("HH\\:mm\\:ss.fffffff") + ".parquet");
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
                        var result = await _s3Client.PutObjectAsync(putRequest);
                    }
                    catch (Exception ex)
                    {
                        var x = ex;
                    }
                }
            }
        }
    }
}
