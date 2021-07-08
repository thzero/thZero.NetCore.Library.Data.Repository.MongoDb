/* ------------------------------------------------------------------------- *
thZero.NetCore.Library.Data.Repository.MongoDb
Copyright (C) 2016-2021 thZero.com

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
	public abstract class BaseMongoDbRepository<TService> : RepositoryLoggableBase<MongoDbRepositoryConnectionConfiguration, TService>
	{
		public BaseMongoDbRepository(IOptions<MongoDbRepositoryConnectionConfiguration> config, ILogger<TService> logger) : base(config, logger)
		{
			InitializeConventions();
			InitializeDataMappings();
		}

		#region Protected Methods
		protected async Task<bool> DropCollectionAsync(string key, string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => key);
			Enforce.AgainstNullOrEmpty(() => collectionName);

			IMongoDatabase database = GetDatabase(key);
			Enforce.AgainstNull(() => database);
			await database.DropCollectionAsync(collectionName);
			return true;
		}

		protected async Task<bool> DropCollectionAsync<T>(string key)
		{
			Enforce.AgainstNullOrEmpty(() => key);

			IMongoDatabase database = GetDatabase(key);
			Enforce.AgainstNull(() => database);
			await database.DropCollectionAsync(typeof(T).Name.ToLower());
			return true;
		}

        protected bool DropCollection(string key, string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => key);
			Enforce.AgainstNullOrEmpty(() => collectionName);

			IMongoDatabase database = GetDatabase(key);
			Enforce.AgainstNull(() => database);
			database.DropCollection(collectionName);
            return true;
        }

        protected bool DropCollection<T>(string key)
		{
			Enforce.AgainstNullOrEmpty(() => key);

			IMongoDatabase database = GetDatabase(key);
			Enforce.AgainstNull(() => database);
			database.DropCollection(typeof(T).Name.ToLower());
            return true;
        }

        protected IMongoCollection<BsonDocument> GetCollection(string key, string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => key);
			Enforce.AgainstNullOrEmpty(() => collectionName);

			return GetCollection<BsonDocument>(key, collectionName);
        }

        protected IMongoCollection<T> GetCollection<T>(string key)
		{
			Enforce.AgainstNullOrEmpty(() => key);

			CollectionNameAttribute attribute = thZero.Utilities.Attributes.GetCustomAttribute<CollectionNameAttribute>(typeof(T), true);
            if ((attribute == null) || !string.IsNullOrEmpty(attribute.Name))
				return GetCollection<T>(typeof(T).Name.ToLower());

            return GetCollection<T>(key, attribute.Name.ToLower());
        }

        protected virtual IMongoCollection<T> GetCollection<T>(string key, string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => key);
			Enforce.AgainstNullOrEmpty(() => collectionName);

			IMongoDatabase database = GetDatabase(key);
			Enforce.AgainstNull(() => database);
			return database.GetCollection<T>(collectionName);
		}

		protected IMongoDatabase GetDatabase(string key)
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

				return client.GetDatabase(configClient.Database);
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

		protected virtual void InitializeConventions()
		{
			MongoDB.Bson.Serialization.Conventions.ConventionPack pack = new();
			InitializeConventions(pack);
			MongoDB.Bson.Serialization.Conventions.ConventionRegistry.Register("additional", pack, t => true);
		}

		/// <summary>
		/// If you need to avoid the _id to Id mapping by convention, then the following with needs to be registered with the class that contains
		/// the Id property.  Unfortunately it must also contain the _id property.
		/// <code>
		///BsonClassMap.RegisterClassMap<DataClass>(cm => 
		///{
		///    cm.AutoMap();
		///    cm.SetIdMember(cm.GetMemberMap(c => c._id));
		///    cm.IdMemberMap.SetSerializer(new StringSerializer(BsonType.ObjectId));
		///    cm.GetMemberMap(c => c.Id).SetElementName("id");
		///    //cm.GetMemberMap(c => c.CreatedTimestamp).SetElementName("createdTimestamp");
		///    //cm.GetMemberMap(c => c.UpdatedTimestamp).SetElementName("updatedTimestamp");
		///});
		/// </code>
		/// </summary>
		protected virtual void InitializeDataMappings()
		{
		}

		protected virtual void InitializeConventions(MongoDB.Bson.Serialization.Conventions.ConventionPack pack)
		{
			pack.Add(new MongoDB.Bson.Serialization.Conventions.DataAnnotationAttributeConvention());
			pack.Add(new MongoDB.Bson.Serialization.Conventions.IgnoreExtraElementsConvention(true));
			pack.Add(new MongoDB.Bson.Serialization.Conventions.IgnoreIfNullConvention(true));
			pack.Add(new MongoDB.Bson.Serialization.Conventions.LowerCaseElementNameConvention());
		}

		protected virtual IMongoClient InitializeClient(IMongoClient client, MongoDbRepositoryClient configClient)
        {
			return client;
		}
		#endregion

		#region Fields
		private readonly IDictionary<string, IMongoClient> _clients = new Dictionary<string, IMongoClient>();
		private static readonly object LockConnection = new();
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