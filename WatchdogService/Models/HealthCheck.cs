﻿// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.ServiceFabric.WatchdogService.Models
{
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Http;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    /// <summary>
    /// HealthCheck structure definition.
    /// </summary>
    public struct HealthCheck : IEquatable<HealthCheck>
    {
        /// <summary>
        /// Default HealthCheck instance.
        /// </summary>
        public static HealthCheck Default = new HealthCheck();

        #region Public Fields

        ///// BEFORE
        ///// <summary>
        ///// Gets the name of the test.
        ///// </summary>
        //[Id(5)]
        //[JsonProperty(PropertyName = "name", Order = 5, Required = Required.Always)]
        //public string Name { get; private set; }

        /// <summary>
        /// Gets the name of the test.
        /// </summary>
        [JsonPropertyName("name")]
        public string Name { get; private set; }

        /// <summary>
        /// Gets the service name.
        /// </summary>
        [JsonPropertyName("serviceName")]
        public Uri ServiceName { get; private set; }

        /// <summary>
        /// Gets the service version.
        /// </summary>
        [JsonPropertyName("partition")]
        public Guid Partition { get; private set; }

        /// <summary>
        /// Gets the name of the endpoint. If more than one endpoint is exposed, this will be required.
        /// </summary>
        [JsonPropertyName("endpoint")]
        public string Endpoint { get; private set; }

        /// <summary>
        /// Gets the a number containing frequency the test call is made. Defaults to 60.
        /// </summary>
        [JsonPropertyName("frequency")]
        public TimeSpan Frequency { get; private set; }

        /// <summary>
        /// Gets the suffix path and query parameters to call when conducting the test. Required.
        /// </summary>
        [JsonPropertyName("suffixPath")]
        public string SuffixPath { get; private set; }

        /// <summary>
        /// Gets the HTTP verb to use when calling the URI. Defaults to GET.
        /// </summary>
        [JsonPropertyName("method")]
        public HttpMethod Method { get; private set; }

        /// <summary>
        /// Gets the content to send in the body of the test request.
        /// </summary>
        [JsonPropertyName("content")]
        public string Content { get; private set; }

        /// <summary>
        /// Gets the content to send in the body of the test request.
        /// </summary>
        [JsonPropertyName("mediaType")]
        public string MediaType { get; private set; }

        /// <summary>
        /// Gets the expected duration of the test call in milliseconds. Defaults to 200ms.
        /// </summary>
        [JsonPropertyName("expectedDuration")]
        public TimeSpan ExpectedDuration { get; private set; }

        /// <summary>
        /// Gets the maximum duration of the test call in milliseconds. The test call will be terminated after this interval as passed. Default is 5000 milliseconds.
        /// </summary>
        [JsonPropertyName("maximumDuration")]
        public TimeSpan MaximumDuration { get; private set; }

        /// <summary>
        /// Gets the headers to add to the request.
        /// </summary>
        [JsonPropertyName("headers")]
        public Dictionary<string, string> Headers { get; private set; }

        /// <summary>
        /// Gets the headers to add to the request.
        /// </summary>
        [JsonPropertyName("warningStatusCodes")]
        public List<int> WarningStatusCodes { get; private set; }

        /// <summary>
        /// Gets the headers to add to the request.
        /// </summary>
        [JsonPropertyName("errorStatusCodes")]
        public List<int> ErrorStatusCodes { get; private set; }

        /// <summary>
        /// Gets the expected duration of the test call in milliseconds. Defaults to 200ms.
        /// </summary>
        [JsonPropertyName("lastAttempt")]
        public DateTimeOffset LastAttempt { get; private set; }

        /// <summary>
        /// Gets the expected duration of the test call in milliseconds. Defaults to 200ms.
        /// </summary>
        [JsonPropertyName("failureCount")]
        public long FailureCount { get; private set; }

        /// <summary>
        /// Gets the expected duration of the test call in milliseconds. Defaults to 200ms.
        /// </summary>
        [JsonPropertyName("resultCode")]
        public HttpStatusCode ResultCode { get; private set; }

        /// <summary>
        /// Gets the expected duration of the test call in milliseconds. Defaults to 200ms.
        /// </summary>
        [JsonPropertyName("duration")]
        public long Duration { get; private set; }

        #endregion

        #region Constructors

        /// <summary>
        /// HealthCheck constructor.
        /// </summary>
        /// <param name="name">Name of the test. Should be uniquely identify the test within the Telemetry output.</param>
        /// <param name="serviceName">Uri containing the service name of the service.</param>
        /// <param name="partition">GUID containing the partition identifier.</param>
        /// <param name="suffixPath">Suffix path and query parameters of the URL to call during the test.</param>
        /// <param name="endpoint">Name of the endpoint to connect to.</param>
        /// <param name="content">String containing the content of the test request.</param>
        /// <param name="mediaType">Media type of the content. Must be specified if content is specified.</param>
        /// <param name="method">Method for the test request.</param>
        /// <param name="frequency">Frequency of the test requests.</param>
        /// <param name="expected">The expected duration of the test request.</param>
        /// <param name="maximum">The maximum duration of the test request.</param>
        /// <param name="headers">Headers to set for the test request.</param>
        /// <param name="errorStatusCodes">Error return status codes.</param>
        /// <param name="warningStatusCodes">Warning return status codes.</param>
        /// <param name="lastAttempt">Date and time of the last attempt.</param>
        /// <param name="failureCount">Number of times this request has consecutively failed.</param>
        /// <param name="resultCode">Result code returned from the execution of another.</param>
        /// <param name="duration">Duration of the last call.</param>
        public HealthCheck(
            string name,
            Uri serviceName,
            Guid partition,
            string suffixPath,
            string endpoint = null,
            string content = null,
            string mediaType = null,
            HttpMethod method = default(HttpMethod),
            TimeSpan frequency = default(TimeSpan),
            TimeSpan expected = default(TimeSpan),
            TimeSpan maximum = default(TimeSpan),
            Dictionary<string, string> headers = null,
            List<int> errorStatusCodes = null,
            List<int> warningStatusCodes = null,
            DateTimeOffset lastAttempt = default(DateTimeOffset),
            long failureCount = default(long),
            HttpStatusCode resultCode = default(HttpStatusCode),
            long duration = default(long))
        {
            // Check required parameters.
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException(nameof(name));
            }
            if (null == serviceName)
            {
                throw new ArgumentNullException(nameof(serviceName));
            }
            if ((false == string.IsNullOrWhiteSpace(content)) && (string.IsNullOrWhiteSpace(mediaType)))
            {
                throw new ArgumentException("mediaType must be specified if content is provided.", nameof(mediaType));
            }
            if (string.IsNullOrWhiteSpace(suffixPath))
            {
                throw new ArgumentException(nameof(suffixPath));
            }
            if (default(HttpMethod) == method)
            {
                method = HttpMethod.Get;
            }

            this.Name = name;
            this.ServiceName = serviceName;
            this.Endpoint = endpoint;
            this.Content = content;
            this.MediaType = mediaType;
            this.Partition = partition;
            this.Method = method;
            this.SuffixPath = suffixPath;

            this.Frequency = (default(TimeSpan) == frequency) ? TimeSpan.FromSeconds(60) : frequency;
            this.ExpectedDuration = (default(TimeSpan) == expected) ? TimeSpan.FromMilliseconds(200) : expected;
            this.MaximumDuration = (default(TimeSpan) == maximum) ? TimeSpan.FromSeconds(5) : maximum;

            this.Headers = headers;

            this.WarningStatusCodes = warningStatusCodes;
            this.ErrorStatusCodes = errorStatusCodes;

            this.LastAttempt = lastAttempt;
            this.FailureCount = failureCount;
            this.ResultCode = resultCode;
            this.Duration = duration;
        }

        /// <summary>
        /// HealthCheck constructor.
        /// </summary>
        /// <param name="other">Copies the values of an already initialized HealthCheck instance.</param>
        public HealthCheck(HealthCheck other)
        {
            this.Name = other.Name;
            this.ServiceName = other.ServiceName;
            this.Endpoint = other.Endpoint;
            this.Content = other.Content;
            this.MediaType = other.MediaType;
            this.Partition = other.Partition;
            this.Method = other.Method;
            this.SuffixPath = other.SuffixPath;
            this.Frequency = other.Frequency;
            this.ExpectedDuration = other.ExpectedDuration;
            this.MaximumDuration = other.MaximumDuration;
            this.Headers = other.Headers;
            this.WarningStatusCodes = other.WarningStatusCodes;
            this.ErrorStatusCodes = other.ErrorStatusCodes;
            this.LastAttempt = other.LastAttempt;
            this.FailureCount = other.FailureCount;
            this.ResultCode = other.ResultCode;
            this.Duration = other.Duration;
        }

        #endregion

        /// <summary>
        /// Overrides the default equals operator.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (null == obj)
            {
                return false;
            }

            if (obj is HealthCheck)
            {
                return this.Equals((HealthCheck) obj);
            }

            return false;
        }

        /// <summary>
        /// Equals operator.
        /// </summary>
        /// <param name="other">HealthCheck instance to compare.</param>
        /// <returns>True if they are equal, otherwise false.</returns>
        public static bool operator ==(HealthCheck hc1, HealthCheck hc2)
        {
            return hc1.Equals(hc2);
        }

        /// <summary>
        /// Not equals operator.
        /// </summary>
        /// <param name="other">HealthCheck instance to compare.</param>
        /// <returns>True if not equals, otherwise false.</returns>
        public static bool operator !=(HealthCheck hc1, HealthCheck hc2)
        {
            return !hc1.Equals(hc2);
        }

        public override int GetHashCode()
        {
            return JsonConvert.SerializeObject(this).GetHashCode();
        }

        #region Public Methods

        /// <summary>
        /// Get the key for the HealthCheck instance.
        /// </summary>
        public string Key
        {
            get
            {
                if (this.ServiceName.IsAbsoluteUri)
                {
                    return $"{this.ServiceName.AbsolutePath}/{this.Partition}";
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// Updates the HealthCheck with the results of a call.
        /// </summary>
        /// <param name="update"></param>
        /// <param name="failureCount"></param>
        /// <param name="duration"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public HealthCheck UpdateWith(DateTimeOffset update, long failureCount, long duration, HttpStatusCode result)
        {
            return new HealthCheck(
                this.Name,
                this.ServiceName,
                this.Partition,
                this.SuffixPath,
                this.Endpoint,
                this.Content,
                this.MediaType,
                this.Method,
                this.Frequency,
                this.ExpectedDuration,
                this.MaximumDuration,
                this.Headers,
                this.ErrorStatusCodes,
                this.WarningStatusCodes,
                update,
                failureCount,
                result,
                duration);
        }

        #endregion

        #region IEquatable Interface Methods

        /// <summary>
        /// Compares to HealthCheck instances for equality.
        /// </summary>
        /// <param name="other">HealthCheck instance to compare.</param>
        /// <returns>True if they are equal, otherwise false.</returns>
        public bool Equals(HealthCheck other)
        {
            if (this.Content != other.Content)
            {
                return false;
            }
            if (this.MediaType != other.MediaType)
            {
                return false;
            }
            if (this.Endpoint != other.Endpoint)
            {
                return false;
            }
            if (this.ExpectedDuration != other.ExpectedDuration)
            {
                return false;
            }
            if (this.Frequency != other.Frequency)
            {
                return false;
            }
            if (this.Method != other.Method)
            {
                return false;
            }
            if (this.MaximumDuration != other.MaximumDuration)
            {
                return false;
            }
            if (this.Name != other.Name)
            {
                return false;
            }
            if (this.Partition != other.Partition)
            {
                return false;
            }
            if (this.ServiceName != other.ServiceName)
            {
                return false;
            }
            if (this.SuffixPath != other.SuffixPath)
            {
                return false;
            }
            if (this.LastAttempt != other.LastAttempt)
            {
                return false;
            }
            if (this.FailureCount != other.FailureCount)
            {
                return false;
            }
            if (this.ResultCode != other.ResultCode)
            {
                return false;
            }
            if (this.Duration != other.Duration)
            {
                return false;
            }

            if (false == Extensions.ListEquals(this.WarningStatusCodes, other.WarningStatusCodes))
            {
                return false;
            }
            if (false == Extensions.ListEquals(this.ErrorStatusCodes, other.ErrorStatusCodes))
            {
                return false;
            }

            if (false == Extensions.DictionaryEquals<string, string>(this.Headers, other.Headers))
            {
                return false;
            }

            return true;
        }

        #endregion
    }
}