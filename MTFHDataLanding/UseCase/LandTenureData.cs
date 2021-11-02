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
using System.IO;
using System.Linq;
using System.Text;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using MTFHDataLanding.Helpers;
using Newtonsoft.Json;

namespace MTFHDataLanding.UseCase
{
    public class LandTenureData : ILandTenureData
    {
        private readonly ITenureInfoApi _tenureApi;
        private readonly IAmazonS3 _s3Client;

        public LandTenureData(ITenureInfoApi tenureApi,
            IAmazonS3 s3Client)
        {
            _tenureApi = tenureApi;
            _s3Client = s3Client;
        }

        [LogCall]
        public async Task ProcessMessageAsync(EntityEventSns message)
        {
            if (message is null) throw new ArgumentNullException(nameof(message));

            // #1 - Get the tenure
            var tenure = await _tenureApi.GetTenureInfoByIdAsync(message.EntityId, message.CorrelationId)
                                             .ConfigureAwait(false);
            if (tenure is null) throw new TenureNotFoundException(message.EntityId);


            using (var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(tenure))))
            {
                string year = message.DateTime.ToString("yyyy");
                string month = message.DateTime.ToString("MM");
                string day = message.DateTime.ToString("dd");

                PutObjectRequest putRequest = new PutObjectRequest
                {
                    BucketName = Constants.BUCKET_NAME,
                    Key = Strings.GenerateFileName(message, Constants.TENURES_KEY_NAME, year, month, day),
                    InputStream = memoryStream
                };

                await _s3Client.PutObjectAsync(putRequest);
            }
        }
    }
}
