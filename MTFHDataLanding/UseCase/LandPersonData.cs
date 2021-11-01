using Microsoft.Extensions.Logging;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Gateway.Interfaces;
using MTFHDataLanding.Infrastructure.Exceptions;
using MTFHDataLanding.UseCase.Interfaces;
using Hackney.Core.Logging;
using System;
using System.Threading.Tasks;
using System.IO;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
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


            using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(person))))
            {
                string year = message.DateTime.ToString("yyyy");
                string month = message.DateTime.ToString("MM");
                string day = message.DateTime.ToString("dd");

                PutObjectRequest putRequest = new PutObjectRequest
                {
                    BucketName = Constants.BUCKET_NAME,
                    Key = Strings.GenerateFileName(message, year, month, day),
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
