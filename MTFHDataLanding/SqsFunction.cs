using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Gateway;
using MTFHDataLanding.Gateway.Interfaces;
using MTFHDataLanding.UseCase;
using MTFHDataLanding.UseCase.Interfaces;
using Hackney.Core.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Microsoft.Extensions.DependencyInjection.Extensions;
using MTFHDataLanding.Interfaces;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace MTFHDataLanding
{
    /// <summary>
    /// Lambda function triggered by an SQS message
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class SqsFunction : BaseFunction
    {
        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public SqsFunction(IServiceCollection services = null) : base(services)
        { }

        protected override void ConfigureServices()
        {
            services.AddHttpClient();
            services.AddScoped<ILandPersonData, LandPersonData>();
            services.AddScoped<ILandTenureData, LandTenureData>();
            services.AddScoped<IPersonApi, PersonApi>();
            services.AddScoped<ITenureInfoApi, TenureInfoApi>();
            services.AddScoped<ITenureDataFactory, TenureDataFactory>();
            services.AddScoped<IMessageProcessor, MessageProcessor>();
            services.AddScoped<IApiGateway, ApiGateway>();
            services.AddScoped(typeof(ILogger<>), typeof(Logger<>));
            services.TryAddScoped<IAmazonS3>(x =>
            {
                return new AmazonS3Client(new AmazonS3Config { RegionEndpoint = RegionEndpoint.EUWest2 });
            });

            base.ConfigureServices();
        }


        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
        /// to respond to SQS messages.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            var messageProcessor = ServiceProvider.GetService<IMessageProcessor>();
            // Do this in parallel???
            foreach (var message in evnt.Records)
            {
                await messageProcessor.ProcessMessageAsync(message, context).ConfigureAwait(false);
            }
        }
    }
}
