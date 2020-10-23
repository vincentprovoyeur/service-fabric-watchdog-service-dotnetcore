using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Description;
using System.Fabric.Health;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.ServiceFabric.WatchdogService;
using Microsoft.ServiceFabric.WatchdogService.Interfaces;
using Microsoft.ServiceFabric.WatchdogService.Models;
using Microsoft.ServiceFabric.WatchDogService;
using WatchdogService;

namespace Microsoft.ServiceFabric.WatchdogService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class WatchdogService : StatefulService, IWatchdogService
    {

        #region Constants
        /// <summary>
        /// Constant values. The metrics names must match the values in the ServiceManifest.
        /// </summary>
        private const string HealthCheckCountMetricName = "HealthCheckCount";
        private const string WatchdogConfigSectionName = "Watchdog";
        #endregion

        #region Members

        /// <summary>
        /// Service Fabric client instance.
        /// </summary>
        private static FabricClient _client = null;

        /// <summary>
        /// Service Fabric client instance.
        /// </summary>
        private static StatefulServiceContext _context = null;

        /// <summary>
        /// HealthCheckController operations class instance.
        /// </summary>
        private HealthCheckOperations _healthCheckOperations = null;

        /// <summary>
        /// CancellationToken instance assigned in RunAsync.
        /// </summary>
        private CancellationToken _runAsyncCancellationToken = CancellationToken.None;

        /// <summary>
        /// Health report interval. Can be changed based on configuration.
        /// </summary>
        private TimeSpan HealthReportInterval = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Configuration package instance.
        /// </summary>
        private ConfigurationSettings _settings = null;

        /// <summary>
        /// AI telemetry instance.
        /// </summary>
        private IWatchdogTelemetry _telemetry = null;

        public ConfigurationSettings Settings => this._settings;

        /// <summary>
        /// HealthCheckController operations class instance.
        /// </summary>
        public HealthCheckOperations HealthCheckOperations => this._healthCheckOperations;

        #endregion

        /// <summary>
        /// Static WatchdogService constructor.
        /// </summary>
        static WatchdogService()
        {
            _client = new FabricClient(FabricClientRole.User);
        }


        public WatchdogService(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// WatchdogService constructor.
        /// </summary>
        /// <param name="context">StatefulServiceContext instance.</param>
        /// <param name="stateManagerReplica">ReliableStateManagerReplica interface.</param>
        public WatchdogService(StatefulServiceContext context, InitializationCallbackAdapter adapter)
            : base(context, new ReliableStateManager(context, new ReliableStateManagerConfiguration(onInitializeStateSerializersEvent: adapter.OnInitialize)))
        {
            adapter.StateManager = this.StateManager;
        }


        FabricClient IWatchdogService.Client => _client;

        //IReliableStateManager IWatchdogService.StateManager => throw new NotImplementedException();

        StatefulServiceContext IWatchdogService.Context => _context;

        PartitionAccessStatus IWatchdogService.ReadStatus => this.Partition.ReadStatus;

        PartitionAccessStatus IWatchdogService.WriteStatus => this.Partition.WriteStatus;

        ConfigurationSettings IWatchdogService.Settings => throw new NotImplementedException();

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        //protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        //{
        //    return new ServiceReplicaListener[0];
        //}

        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new ServiceReplicaListener[]
            {
                new ServiceReplicaListener(serviceContext =>
                    new KestrelCommunicationListener(serviceContext, (url, listener) =>
                    {
                        ServiceEventSource.Current.ServiceMessage(serviceContext, $"Starting Kestrel on {url}");

                        return new WebHostBuilder()
                                    .UseKestrel()
                                    .ConfigureServices(
                                        services => services
                                            .AddSingleton<StatefulServiceContext>(serviceContext)
                                            .AddSingleton<IReliableStateManager>(this.StateManager))
                                    .UseContentRoot(Directory.GetCurrentDirectory())
                                    .UseStartup<Startup>()
                                    .UseServiceFabricIntegration(listener, ServiceFabricIntegrationOptions.UseUniqueServiceUrl)
                                    .UseUrls(url)
                                    .Build();
                    }))
            };
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            ServiceEventSource.Current.ServiceMessage(this.Context, "RunAsync called");

            _context = this.Context;
            _settings = this.Context.CodePackageActivationContext.GetConfigurationPackageObject("Config").Settings;
            
            // Check if settings are null. If they are, throw.
            if (null == this.Settings)
            {
                throw new ArgumentNullException("Settings are null, check Config/Settings exist.");

            }

            // Create the operations classes.
            this._healthCheckOperations = new HealthCheckOperations(
                this,
                this._telemetry,
                this.GetConfigValueAsTimeSpan(WatchdogConfigSectionName, "HealthCheckInterval", TimeSpan.FromMinutes(5)),
                cancellationToken);

            // Register the watchdog health check.
            await this.RegisterHealthCheckAsync(cancellationToken).ConfigureAwait(false);

            // Loop waiting for cancellation.
            while (false == cancellationToken.IsCancellationRequested)
            {
                // Report the health and metrics of the watchdog to Service Fabric.
                this.ReportWatchdogHealth();
                // TODO: we removed telemetry because this is using applicationinsight which is not available for us
                //await this.ReportWatchdogMetricsAsync(cancellationToken);


                // Delay up to the time for the next health report.
                await Task.Delay(this.HealthReportInterval, cancellationToken);
            }
        }

        void IWatchdogService.RefreshFabricClient()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets a configuration value or the specified default value.
        /// </summary>
        /// <param name="sectionName">Name of the section containing the parameter.</param>
        /// <param name="parameterName">Name of the parameter containing the value.</param>
        /// <param name="value">Default value.</param>
        /// <returns>Configuraiton value or default.</returns>
        private TimeSpan GetConfigValueAsTimeSpan(string sectionName, string parameterName, TimeSpan value = default(TimeSpan))
        {
            if (null != this.Settings)
            {
                ConfigurationSection section = this.Settings.Sections[sectionName];
                if (null != section)
                {
                    ConfigurationProperty parameter = section.Parameters[parameterName];
                    if (null != parameter)
                    {
                        if (TimeSpan.TryParse(
                            parameter.Value,
                            out TimeSpan
                        val))
                        {
                            value = val;
                        }
                    }
                }
            }

            return value;
        }

        private async Task RegisterHealthCheckAsync(CancellationToken token)
        {
            HttpClient client = new HttpClient();

            // Called from RunAsync, don't let an exception out so the service will start, but log the exception because the service won't work.
            try
            {
                // Use the reverse proxy to locate the service endpoint.
                //fabric:/ WatchDogServiceCore / WatchdogService
                //fabric:/ Watchdog / WatchdogService
                string postUrl = "http://localhost:19081/WatchDogService/WatchdogService/healthcheck";
                HealthCheck hc = new HealthCheck("Watchdog Health Check", this.Context.ServiceName, this.Context.PartitionId, "watchdog/health");
                HttpResponseMessage msg = await client.PostAsJsonAsync(postUrl, hc);

                // Log a success or error message based on the returned status code.
                if (HttpStatusCode.OK == msg.StatusCode)
                {
                    ServiceEventSource.Current.Write(nameof(this.RegisterHealthCheckAsync), Enum.GetName(typeof(HttpStatusCode), msg.StatusCode));
                }
                else
                {
                    ServiceEventSource.Current.Error(nameof(this.RegisterHealthCheckAsync), Enum.GetName(typeof(HttpStatusCode), msg.StatusCode));
                }
            }
            catch (Exception ex)
            {
                ServiceEventSource.Current.Error($"Exception: {ex.Message} at {ex.StackTrace}.","");
            }
        }

        private async Task ReportClusterHealthAsync(CancellationToken cancellationToken)
        {
            // Called from RunAsync, don't let an exception out so the service will start, but log the exception because the service won't work.
            try
            {
                ClusterHealth health = await _client.HealthManager.GetClusterHealthAsync(TimeSpan.FromSeconds(4), cancellationToken);
                if (null != health)
                {
                    // TODO: we don't need telemetry 
                    // Report the aggregated cluster health.
                    await
                        this._telemetry.ReportHealthAsync(
                            this.Context.ServiceName.AbsoluteUri,
                            this.Context.PartitionId.ToString(),
                            this.Context.ReplicaOrInstanceId.ToString(),
                            "Cluster",
                            "Aggregated Cluster Health",
                            health.AggregatedHealthState,
                            cancellationToken);

                    // Get the state of each of the applications running within the cluster. Report anything that is unhealthy.
                    foreach (ApplicationHealthState appHealth in health.ApplicationHealthStates)
                    {
                        if (HealthState.Ok != appHealth.AggregatedHealthState)
                        {
                            await
                                this._telemetry.ReportHealthAsync(
                                    appHealth.ApplicationName.AbsoluteUri,
                                    this.Context.ServiceName.AbsoluteUri,
                                    this.Context.PartitionId.ToString(),
                                    this.Context.ReplicaOrInstanceId.ToString(),
                                    this.Context.NodeContext.NodeName,
                                    appHealth.AggregatedHealthState,
                                    cancellationToken);
                        }
                    }

                    // Get the state of each of the nodes running within the cluster.
                    foreach (NodeHealthState nodeHealth in health.NodeHealthStates)
                    {
                        if (HealthState.Ok != nodeHealth.AggregatedHealthState)
                        {
                            await
                                this._telemetry.ReportHealthAsync(
                                    this.Context.NodeContext.NodeName,
                                    this.Context.ServiceName.AbsoluteUri,
                                    this.Context.PartitionId.ToString(),
                                    this.Context.NodeContext.NodeType,
                                    this.Context.NodeContext.IPAddressOrFQDN,
                                    nodeHealth.AggregatedHealthState,
                                    cancellationToken);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ServiceEventSource.Current.Write($"Exception: {ex.Message} at {ex.StackTrace}.");
            }
        }

        /// <summary>
        /// Compares the proposed health state with the current value and returns the least healthy.
        /// </summary>
        /// <param name="current">Current health state.</param>
        /// <param name="proposed">Proposed health state.</param>
        /// <returns>Selected health state value.</returns>
        private HealthState CompareHealthState(HealthState current, HealthState proposed)
        {
            if ((HealthState.Ok == current) && ((HealthState.Warning == proposed) || (HealthState.Error == proposed)))
            {
                return proposed;
            }
            if ((HealthState.Warning == current) && (HealthState.Error == proposed))
            {
                return proposed;
            }
            if ((HealthState.Invalid == current) || (HealthState.Unknown == current))
            {
                return proposed;
            }

            return current;
        }

        public HealthState CheckWatchdogHealth(StringBuilder description)
        {
            HealthState current = HealthState.Ok;
            if (null == ServiceEventSource.Current)
            {
                current = this.CompareHealthState(current, HealthState.Error);
                description.AppendLine("ServiceEventSource is null.");
            }

            if (null == this._healthCheckOperations)
            {
                current = this.CompareHealthState(current, HealthState.Error);
                description.AppendLine("HealthCheckOperations is null.");
            }

            //if (null == this._metricsOperations)
            //{
            //    current = this.CompareHealthState(current, HealthState.Error);
            //    description.AppendLine("MetricOperations is null.");
            //}

            // Check the number of endpoints listening.
            if (0 == this.Context.CodePackageActivationContext.GetEndpoints().Count)
            {
                current = this.CompareHealthState(current, HealthState.Error);
                description.AppendLine("Endpoints listening is zero.");
            }

            return current;
        }

        private void ReportWatchdogHealth()
        {
            // Called from RunAsync, don't let an exception out so the service will start, but log the exception because the service won't work.
            try
            {
                // Collect the health information from the local service state.
                TimeSpan interval = this.HealthReportInterval.Add(TimeSpan.FromSeconds(30));
                StringBuilder sb = new StringBuilder();
                HealthState hs = this.CheckWatchdogHealth(sb);

                // Issue a health report for the watchdog service.
                HealthInformation hi = new HealthInformation(this.Context.ServiceName.AbsoluteUri, "WatchdogServiceHealth", hs)
                {
                    TimeToLive = interval,
                    Description = sb.ToString(),
                    RemoveWhenExpired = false,
                    SequenceNumber = HealthInformation.AutoSequenceNumber,
                };
                this.Partition.ReportPartitionHealth(hi);

                hi = new HealthInformation(this.Context.ServiceName.AbsoluteUri, "HealthCheckOperations", this._healthCheckOperations.Health);
                hi.TimeToLive = interval;
                hi.RemoveWhenExpired = false;
                hi.SequenceNumber = HealthInformation.AutoSequenceNumber;
                this.Partition.ReportPartitionHealth(hi);

            }
            catch (Exception ex)
            {
                ServiceEventSource.Current.Error($"Exception: {ex.Message} at {ex.StackTrace}.","");
            }
        }

        /// <summary>
        /// Called when a configuration package is modified.
        /// </summary>
        private void CodePackageActivationContext_ConfigurationPackageModifiedEvent(object sender, PackageModifiedEventArgs<ConfigurationPackage> e)
        {
            if ("Config" == e.NewPackage.Description.Name)
            {
                Interlocked.Exchange<ConfigurationSettings>(ref this._settings, e.NewPackage.Settings);

                // Update the configured values.
                if (null != this._telemetry)
                {
                    this._telemetry.Key = this.Settings.Sections[WatchdogConfigSectionName].Parameters["AIKey"].Value;
                }

                this.HealthReportInterval = this.GetConfigValueAsTimeSpan(WatchdogConfigSectionName, "WatchdogHealthReportInterval", TimeSpan.FromSeconds(60));

                this._healthCheckOperations.TimerInterval = this.GetConfigValueAsTimeSpan(
                    WatchdogConfigSectionName,
                    "HealthCheckInterval",
                    TimeSpan.FromMinutes(5));
                //this._metricsOperations.TimerInterval = this.GetConfigValueAsTimeSpan(WatchdogConfigSectionName, "MetricInterval", TimeSpan.FromMinutes(5));
                //this._cleanupOperations.Endpoint = this.GetConfigValueAsString(WatchdogConfigSectionName, "DiagnosticEndpoint");
                //this._cleanupOperations.SasToken = this.GetConfigValueAsString(WatchdogConfigSectionName, "DiagnosticSasToken");
                //this._cleanupOperations.TimerInterval = this.GetConfigValueAsTimeSpan(WatchdogConfigSectionName, "DiagnosticInterval", TimeSpan.FromMinutes(2));
                //this._cleanupOperations.TimeToKeep = this.GetConfigValueAsTimeSpan(WatchdogConfigSectionName, "DiagnosticTimeToKeep", TimeSpan.FromDays(10));
                //this._cleanupOperations.TargetCount = this.GetConfigValueAsInteger(WatchdogConfigSectionName, "DiagnosticTargetCount", 8000);
            }
        }
    }
}
