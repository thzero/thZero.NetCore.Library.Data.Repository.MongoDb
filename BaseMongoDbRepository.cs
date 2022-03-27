/* ------------------------------------------------------------------------- *
thZero.NetCore.Library.Data.Repository.MongoDb
Copyright (C) 2016-2022 thZero.com

<development [at] thzero [dot] com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 * ------------------------------------------------------------------------- */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using MongoDB.Bson;
using MongoDB.Driver;

namespace thZero.Data.Repository.MongoDb
{
	public abstract class BaseMongoDbRepository<TService, TData, TBaseData> : RepositoryLoggableBase<MongoDbRepositoryConnectionConfiguration, TService>
		where TData : class
	{
		public BaseMongoDbRepository(IOptions<MongoDbRepositoryConnectionConfiguration> config, ILogger<TService> logger) : base(config, logger)
		{
		}

		#region Protected Methods
		protected abstract ProjectionDefinition<TProjectionData> DefaultProjectionBuilder<TProjectionData>()
			where TProjectionData : TBaseData;

		protected async Task<bool> DropCollectionAsync(string key, string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => key);
			Enforce.AgainstNullOrEmpty(() => collectionName);

			MongoDatabaseResponse response = GetDatabase(key);
			Enforce.AgainstNull(() => response.Database);
			Enforce.AgainstNull(() => response.ConfigClient);
			await response.Database.DropCollectionAsync(GetCollectionName(response.ConfigClient, collectionName));
			return true;
		}

		protected async Task<bool> DropCollectionAsync<T>(string key)
		{
			Enforce.AgainstNullOrEmpty(() => key);

			MongoDatabaseResponse response = GetDatabase(key);
			Enforce.AgainstNull(() => response.Database);
			Enforce.AgainstNull(() => response.ConfigClient);
			await response.Database.DropCollectionAsync(GetCollectionName(response.ConfigClient, typeof(T).Name.ToLower()));
			return true;
		}

        protected bool DropCollection(string key, string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => key);
			Enforce.AgainstNullOrEmpty(() => collectionName);

			MongoDatabaseResponse response = GetDatabase(key);
			Enforce.AgainstNull(() => response.Database);
			Enforce.AgainstNull(() => response.ConfigClient);
			response.Database.DropCollection(GetCollectionName(response.ConfigClient, collectionName));
			return true;
        }

        protected bool DropCollection<T>(string key)
		{
			Enforce.AgainstNullOrEmpty(() => key);

			MongoDatabaseResponse response = GetDatabase(key);
			Enforce.AgainstNull(() => response.Database);
			Enforce.AgainstNull(() => response.ConfigClient);
			response.Database.DropCollection(GetCollectionName(response.ConfigClient, typeof(T).Name.ToLower()));
			return true;
        }

        protected MongoCollectionResponse<BsonDocument> GetCollection(string key, string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => key);
			Enforce.AgainstNullOrEmpty(() => collectionName);

			return GetCollection<BsonDocument>(key, collectionName);
		}

        protected MongoCollectionResponse<T> GetCollection<T>(string key)
		{
			Enforce.AgainstNullOrEmpty(() => key);

			CollectionNameAttribute attribute = thZero.Utilities.Attributes.GetCustomAttribute<CollectionNameAttribute>(typeof(T), true);
            if ((attribute == null) || !string.IsNullOrEmpty(attribute.Name))
				return GetCollection<T>(typeof(T).Name.ToLower());

			return GetCollection<T>(key, attribute.Name.ToLower());
		}

        protected virtual MongoCollectionResponse<T> GetCollection<T>(string key, string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => key);
			Enforce.AgainstNullOrEmpty(() => collectionName);

			MongoDatabaseResponse response = GetDatabase(key);
			Enforce.AgainstNull(() => response.Database);
			Enforce.AgainstNull(() => response.ConfigClient);
			Enforce.AgainstNull(() => response.Client);

			return new MongoCollectionResponse<T>(response.Database.GetCollection<T>(GetCollectionName(response.ConfigClient, collectionName)), response);
		}

		protected MongoDatabaseResponse GetDatabase(string key)
		{
			const string Declaration = "GetDatabase";

			try
			{
				MongoDbRepositoryConnectionConfiguration config = InitializeConnection(Config);

				MongoDbRepositoryClient configClient = config.Clients.Where(l => l.Key.EqualsIgnore(key)).FirstOrDefault();
				if (configClient == null)
					throw new MongoDbContextInvalidClientConfigurationException();

				if (string.IsNullOrEmpty(configClient.ConnectionString))
					throw new MongoDbContextInvalidConnectionStringException();

				if (string.IsNullOrEmpty(configClient.Database))
					throw new MongoDbContextInvalidDatabaseException();

				IMongoClient client = null;
				if (!_clients.ContainsKey(key))
				{
					lock (LockConnection)
					{
						if (!_clients.ContainsKey(key))
						{
							client = new MongoClient(configClient.ConnectionString);
							client = InitializeClient(client, configClient);
							_clients.Add(key, client);
						}
						else
							client = _clients[key];
					}
				}
				else
					client = _clients[key];

				return new MongoDatabaseResponse(client.GetDatabase(configClient.Database), configClient, client);
			}
			catch (Exception ex)
			{
				Logger?.LogError2(Declaration, ex);
				throw;
			}
		}

		protected virtual MongoDbRepositoryConnectionConfiguration InitializeConnection(MongoDbRepositoryConnectionConfiguration config)
        {
			return config;
		}

		protected virtual IMongoClient InitializeClient(IMongoClient client, MongoDbRepositoryClient configClient)
        {
			return client;
		}
		#endregion

		#region Private Methods
		private static string GetCollectionName(MongoDbRepositoryClient configClient, string key)
		{
			Enforce.AgainstNull(() => configClient);
			Enforce.AgainstNullOrEmpty(() => key);

			MongoDbRepositoryClientCollection collection = configClient.Collections.Where(l => l.Key.EqualsIgnore(key)).FirstOrDefault();
			return collection != null ? collection.Name : key;
		}
		#endregion

		#region Fields
		private readonly IDictionary<string, IMongoClient> _clients = new Dictionary<string, IMongoClient>();

		protected readonly ReplaceOptions UpsertOptions = new() { IsUpsert = true };

		private static readonly object LockConnection = new();
		#endregion
    }

	public struct MongoCollectionResponse<T>
	{
		public MongoCollectionResponse(IMongoCollection<T> collection, MongoDatabaseResponse database)
		{
			Collection = collection;
			Client = database.Client;
			ConfigClient = database.ConfigClient;
			Database = database.Database;
		}

		#region Public Properties
		public IMongoClient Client { get; set; }
		public IMongoCollection<T> Collection { get; set; }
		public MongoDbRepositoryClient ConfigClient { get; set; }
		public IMongoDatabase Database { get; set; }
		#endregion
	}

	public struct MongoDatabaseResponse
	{
		public MongoDatabaseResponse(IMongoDatabase database, MongoDbRepositoryClient configClient, IMongoClient client)
        {
			Database = database;
			ConfigClient = configClient;
			Client = client;
		}

        #region Public Properties
        public IMongoClient Client { get; set; }
		public MongoDbRepositoryClient ConfigClient { get; set; }
		public IMongoDatabase Database { get; set; }
		#endregion
	}

	[Serializable]
    public sealed class MongoDbContextInvalidConnectionConfigurationException : Exception
    {
        public MongoDbContextInvalidConnectionConfigurationException() : base() { }
        public MongoDbContextInvalidConnectionConfigurationException(string message) : base(message) { }
        public MongoDbContextInvalidConnectionConfigurationException(string message, Exception inner) : base(message, inner) { }
#pragma warning disable CS0628 // New protected member declared in sealed class
        protected MongoDbContextInvalidConnectionConfigurationException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
#pragma warning restore CS0628 // New protected member declared in sealed class
	}

	[Serializable]
	public sealed class MongoDbContextInvalidClientConfigurationException : Exception
	{
		public MongoDbContextInvalidClientConfigurationException() : base() { }
		public MongoDbContextInvalidClientConfigurationException(string message) : base(message) { }
		public MongoDbContextInvalidClientConfigurationException(string message, Exception inner) : base(message, inner) { }
#pragma warning disable CS0628 // New protected member declared in sealed class
		protected MongoDbContextInvalidClientConfigurationException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
#pragma warning restore CS0628 // New protected member declared in sealed class
	}

	[Serializable]
    public sealed class MongoDbContextInvalidConnectionStringException : Exception
    {
        public MongoDbContextInvalidConnectionStringException() : base() { }
        public MongoDbContextInvalidConnectionStringException(string message) : base(message) { }
        public MongoDbContextInvalidConnectionStringException(string message, Exception inner) : base(message, inner) { }
#pragma warning disable CS0628 // New protected member declared in sealed class
        protected MongoDbContextInvalidConnectionStringException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
#pragma warning restore CS0628 // New protected member declared in sealed class
    }

    [Serializable]
    public sealed class MongoDbContextInvalidDatabaseException : Exception
    {
        public MongoDbContextInvalidDatabaseException() : base() { }
        public MongoDbContextInvalidDatabaseException(string message) : base(message) { }
        public MongoDbContextInvalidDatabaseException(string message, Exception inner) : base(message, inner) { }
#pragma warning disable CS0628 // New protected member declared in sealed class
        protected MongoDbContextInvalidDatabaseException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context) : base(info, context) { }
#pragma warning restore CS0628 // New protected member declared in sealed class
    }
}