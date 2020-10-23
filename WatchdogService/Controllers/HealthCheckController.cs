// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.WatchdogService.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;
    using System.Web.Http;
    using global::WatchdogService;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.ServiceFabric.WatchdogService.Models;

    /// <summary>
    /// HealthCheckController.
    /// </summary>
    [Route("[controller]")]
    [ApiController]
    public sealed class HealthCheckController : ControllerBase
    {
        /// <summary>
        /// TelemetryService instance.
        /// </summary>
        private readonly HealthCheckOperations _operations = null;

        /// <summary>
        /// HealthCheckController constructor.
        /// </summary>
        /// <param name="service">WatchdogService class instance.</param>
        internal HealthCheckController(WatchdogService service)
        {
            this._operations = service.HealthCheckOperations;
        }

        #region Watchdog Health for Self Monitoring

        [HttpGet]
        [Route(@"health")]
        public async Task<IActionResult> GetWatchdogHealth()
        {
            // Check that an operations class exists.
            if (null == this._operations)
            {
                //return this.Request.CreateResponse(HttpStatusCode.InternalServerError);
                return StatusCode(500);
            }

            // Check that there are items being monitored.
            IList<HealthCheck> items = await this._operations.GetHealthChecksAsync();
            if (0 == items.Count)
            {
                return NoContent();
            }

            // Return the status.
            return Ok();
        }

        #endregion

        #region Health Check Operations

        [HttpGet]
        [Route(@"{application?}/{service?}/{partition=guid?}")]
        public async Task<IActionResult> GetHealthCheck(string application = null, string service = null, Guid? partition = null)
        {
            try
            {
                ServiceEventSource.Current.ServiceRequestStart(nameof(this.GetHealthCheck));

                // Get the list of health check items.
                IList<HealthCheck> items = await this._operations.GetHealthChecksAsync(application, service, partition);
                ServiceEventSource.Current.ServiceRequestStop(nameof(this.GetHealthCheck));

                return Ok(items);
            }
            catch (Exception ex)
            {
                //TODO: ServiceEventSource.Current.Exception(ex.Message, ex.GetType().Name, nameof(this.GetHealthCheck));
                ServiceEventSource.Current.Write(ex.Message + ex.GetType().Name + nameof(this.GetHealthCheck));
                //return this.Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
                throw ex;
            }
        }

        [HttpPost]
        [Route(@"")] 
        public async Task<IActionResult> PostHealthCheck([FromBody] HealthCheck hcm)
        {


            // Check required parameters.
            if (hcm.Equals(HealthCheck.Default))
            {
                //return this.Request.CreateResponse(HttpStatusCode.BadRequest);
                return BadRequest();
            }
            if (null == this._operations)
            {
                //return this.Request.CreateResponse(HttpStatusCode.InternalServerError);
                return StatusCode(500);
            }

            try
            {
                ServiceEventSource.Current.ServiceRequestStart(nameof(this.PostHealthCheck));

                // Call the operations class to add the health check.
                await this._operations.AddHealthCheckAsync(hcm);

                ServiceEventSource.Current.ServiceRequestStop(nameof(this.PostHealthCheck));
                //return this.Request.CreateResponse(HttpStatusCode.OK);
                return Ok();
            }
            catch (ArgumentException ex)
            {
                //TODO: ServiceEventSource.Current.Exception(ex.Message, ex.GetType().Name, nameof(this.PostHealthCheck));
                ServiceEventSource.Current.Write(ex.Message + ex.GetType().Name + nameof(this.PostHealthCheck));
                //return this.Request.CreateResponse(HttpStatusCode.BadRequest);
                return BadRequest();

            }
            catch (Exception ex)
            {
                //TODO: ServiceEventSource.Current.Exception(ex.Message, ex.GetType().Name, nameof(this.PostHealthCheck));
                ServiceEventSource.Current.Write(ex.Message + ex.GetType().Name + nameof(this.PostHealthCheck));
                //return this.Request.CreateErrorResponse(HttpStatusCode.InternalServerError, ex);
                throw ex;
            }
        }

        #endregion
    }
}