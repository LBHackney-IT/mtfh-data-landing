using Amazon.XRay.Recorder.Core;
using Amazon.XRay.Recorder.Core.Strategies;
using Amazon.XRay.Recorder.Handlers.AwsSdk;
using MTFHDataLanding.Infrastructure;
using Hackney.Core.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Reflection;
using System.Text.Json;

namespace MTFHDataLanding
{
    /// <summary>
    /// Base class used for all functions
    /// Sets up the necessary DI container for the function.
    /// Any function-specific registration and configuration should be done in the derived class.
    /// </summary>
    [ExcludeFromCodeCoverage]
    public abstract class BaseFunction
    {
        protected readonly static JsonSerializerOptions _jsonOptions = JsonOptions.CreateJsonOptions();

        protected IConfigurationRoot Configuration { get; }
        protected IServiceProvider ServiceProvider { get; }
        protected ILogger Logger { get; }
        protected IServiceCollection services;

        internal BaseFunction(IServiceCollection services = null)
        {
            AWSSDKHandler.RegisterXRayForAllServices();

            if (services == null)
                this.services = new ServiceCollection();
            else
                this.services = services;

            var builder = new ConfigurationBuilder();

            Configure(builder);
            Configuration = builder.Build();
            this.services.AddSingleton<IConfiguration>(Configuration);

            this.services.ConfigureLambdaLogging(Configuration);
            this.services.AddLogCallAspect();

            ConfigureServices();

            ServiceProvider = this.services.BuildServiceProvider();
            ServiceProvider.UseLogCall();

            Logger = ServiceProvider.GetRequiredService<ILogger<BaseFunction>>();
        }

        /// <summary>
        /// Base implementation
        /// Automatically adds environment variables and the appsettings file
        /// </summary>
        /// <param name="builder"></param>
        protected virtual void Configure(IConfigurationBuilder builder)
        {
            builder.AddJsonFile("appsettings.json");
            var environment = Environment.GetEnvironmentVariable("ENVIRONMENT");
            if (!string.IsNullOrEmpty(environment))
            {
                var path = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), $"appsettings.{environment}.json");
                if (File.Exists(path))
                    builder.AddJsonFile(path);
            }
            builder.AddEnvironmentVariables();
        }

        /// <summary>>
        /// Base implementation
        /// Automatically adds LogCallAspect
        /// </summary>
        protected virtual void ConfigureServices()
        {
            services.AddLogCallAspect();
        }
    }
}
