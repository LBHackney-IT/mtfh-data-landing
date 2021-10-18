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
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;

namespace MTFHDataLanding.UseCase
{
    public class LandTenureData : ILandTenureData
    {
        private readonly ITenureInfoApi _tenureApi;
        private readonly ILogger<LandTenureData> _logger;

        public LandTenureData(ITenureInfoApi tenureApi,
            ILogger<LandTenureData> logger)
        {
            _tenureApi = tenureApi;
            _logger = logger;
        }

        [LogCall]
        public async Task ProcessMessageAsync(EntityEventSns message)
        {
            String bucketName = "mtfh-data-landing-spike";
            String keyName = "landing/mtfh/tenures/";
            RegionEndpoint bucketRegion = RegionEndpoint.EUWest2;

            IAmazonS3 s3Client = new AmazonS3Client(bucketRegion);
            var fileTransferUtility = new TransferUtility(s3Client);

            if (message is null) throw new ArgumentNullException(nameof(message));

            // #1 - Get the tenure
            var tenure = await _tenureInfoApi.GetTenureInfoByIdAsync(message.EntityId, message.CorrelationId)
                                             .ConfigureAwait(false);
            if (tenure is null) throw new EntityNotFoundException<TenureResponseObject>(message.EntityId);

            _logger.LogWarning($"Tenure record (id: {tenure.Id})");
            var id = new DataColumn(new DataField<string>("id"), new string[] { tenure.Id.ToString() });
            var paymentReference = new DataColumn(new DataField<string>("paymentReference"), new string[] { tenure.PaymentReference });
            var startOfTenureDate = new DataColumn(new DataField<string>("startOfTenureDate"), new string[] { tenure.StartOfTenureDate.ToString() });
            var endOfTenureDate = new DataColumn(new DataField<string>("endOfTenureDate"), new string[] { tenure.EndOfTenureDate.ToString() });
            var dateTime = new DataColumn(new DataField<string>("dateTime"), new string[] { message.DateTime.ToString("o") });
            var userName = new DataColumn(new DataField<string>("userName"), new string[] { message.User.Name });
            var userEmail = new DataColumn(new DataField<string>("userEmail"), new string[] { message.User.Email });
            var eventType = new DataColumn(new DataField<string>("eventType"), new string[] { message.EventType });

            var schema = new Schema(id.Field, paymentReference.Field, startOfTenureDate.Field, endOfTenureDate.Field,
            dateTime.Field, userName.Field, userEmail.Field, eventType.Field);

            using (MemoryStream ms = new MemoryStream())
            {
                using (var parquetWriter = new ParquetWriter(schema, ms))
                {
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        groupWriter.WriteColumn(id);
                        groupWriter.WriteColumn(paymentReference);
                        groupWriter.WriteColumn(startOfTenureDate);
                        groupWriter.WriteColumn(endOfTenureDate);
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
