/* ------------------------------------------------------------------------- *
thZero.NetCore.Library.Data.Repository.MongoDb
Copyright (C) 2016-2018 thZero.com

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
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

namespace thZero.Data.Repository.MongoDb
{
	public abstract class BaseMongoDbRepository : RepositoryBase
	{
		private static readonly thZero.Services.IServiceLog log = thZero.Factory.Instance.RetrieveLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		#region Public Methods
		public override void Initialize(IRepositoryConnectionConfiguration connectionConfiguration)
        {
            const string Declaration = "Initialize";

            Enforce.AgainstNull(() => connectionConfiguration);

            try
            {
				Connection = (MongoDbRepositoryConnectionConfiguration)connectionConfiguration;
                if (Connection == null)
                    throw new MongoDbContextInvalidConnectionConfigurationException();

				InitializeConnection(Connection);

                //StringBuilder connectionString = new StringBuilder();
                //if (string.IsNullOrEmpty(connectionM.ConnectionString))
                //{
                //    bool hasUserPassword = false;
                //    bool requiresScheme = false;

                //    // format: mongodb://<dbuser>:<dbpassword>@<address>:<port>/<database>
                //    if (!string.IsNullOrEmpty(connectionM.User) && !string.IsNullOrEmpty(connectionM.Password))
                //    {
                //        requiresScheme = true;
                //        hasUserPassword = true;
                //        connectionString.Append(connectionM.User).Append(":").Append(connectionM.Password);
                //    }

                //    if (!string.IsNullOrEmpty(connectionM.Address))
                //    {
                //        requiresScheme = true;
                //        if (hasUserPassword)
                //            connectionString.Append("@");

                //        connectionString.Append(connectionM.Address);
                //        if (!string.IsNullOrEmpty(connectionM.Port))
                //            connectionString.Append(":").Append(connectionM.Port);
                //    }

                //    if (!string.IsNullOrEmpty(connectionM.Database))
                //    {
                //        if ((connectionString.Length > 0) && (connectionString[connectionString.Length - 1] != '/'))
                //            connectionString.Append("/");

                //        connectionString.Append(connectionM.Database);
                //    }

                //    if (connectionString.Length == 0)
                //        throw new MongoDbContextInvalidConnectionStringException();

                //    if (requiresScheme)
                //        connectionString.Append(scheme, 0, scheme.Length);
                //}
                //else
                //    connectionString.Append(connectionM.ConnectionString);

                //if (connectionString.Length == 0)
                //    throw new MongoDbContextInvalidConnectionStringException();

                if (string.IsNullOrEmpty(Connection.ConnectionString))
                    throw new MongoDbContextInvalidConnectionStringException();

                if (string.IsNullOrEmpty(Connection.Database))
                    throw new MongoDbContextInvalidDatabaseException();

				/*

				if (!_clients.ContainsKey(connectionM.Key))
				{
					lock (Lock)
					{
						if (!_clients.ContainsKey(connectionM.Key))
						{
							Client = new MongoClient(connectionM.ConnectionString);
							Client = InitializeClient(Client, connectionM);
							_clients.Add(connectionM.Key, Client);
						}
						else
							Client = _clients[connectionM.Key];
					}
				}
				else
					Client = _clients[connectionM.Key];

				Database = Client.GetDatabase(connectionM.Database);
				*/
			}
			catch (Exception ex)
            {
                log.Error(Declaration, ex);
                throw;
            }
        }
		#endregion

		#region Protected Methods
		protected void CheckInitialize()
		{
			const string Declaration = "CheckInitialize";

			try
			{
				InitializeConnection(Connection);

				//StringBuilder connectionString = new StringBuilder();
				//if (string.IsNullOrEmpty(connectionM.ConnectionString))
				//{
				//    bool hasUserPassword = false;
				//    bool requiresScheme = false;

				//    // format: mongodb://<dbuser>:<dbpassword>@<address>:<port>/<database>
				//    if (!string.IsNullOrEmpty(connectionM.User) && !string.IsNullOrEmpty(connectionM.Password))
				//    {
				//        requiresScheme = true;
				//        hasUserPassword = true;
				//        connectionString.Append(connectionM.User).Append(":").Append(connectionM.Password);
				//    }

				//    if (!string.IsNullOrEmpty(connectionM.Address))
				//    {
				//        requiresScheme = true;
				//        if (hasUserPassword)
				//            connectionString.Append("@");

				//        connectionString.Append(connectionM.Address);
				//        if (!string.IsNullOrEmpty(connectionM.Port))
				//            connectionString.Append(":").Append(connectionM.Port);
				//    }

				//    if (!string.IsNullOrEmpty(connectionM.Database))
				//    {
				//        if ((connectionString.Length > 0) && (connectionString[connectionString.Length - 1] != '/'))
				//            connectionString.Append("/");

				//        connectionString.Append(connectionM.Database);
				//    }

				//    if (connectionString.Length == 0)
				//        throw new MongoDbContextInvalidConnectionStringException();

				//    if (requiresScheme)
				//        connectionString.Append(scheme, 0, scheme.Length);
				//}
				//else
				//    connectionString.Append(connectionM.ConnectionString);

				//if (connectionString.Length == 0)
				//    throw new MongoDbContextInvalidConnectionStringException();

				if (Client == null)
				{
					if (!_clients.ContainsKey(Connection.Key))
					{
						lock (LockConnection)
						{
							if (Client == null)
							{
								if (!_clients.ContainsKey(Connection.Key))
								{
									Client = new MongoClient(Connection.ConnectionString);
									Client = InitializeClient(Client, Connection);
									_clients.Add(Connection.Key, Client);
								}
								else
									Client = _clients[Connection.Key];
							}
						}
					}
					else
						Client = _clients[Connection.Key];
				}

				if (Database == null)
				{
					lock (LockDatabase)
					{
						if (Database == null)
							Database = Client.GetDatabase(Connection.Database);
					}
				}
			}
			catch (Exception ex)
			{
				log.Error(Declaration, ex);
				throw;
			}
		}

		protected async Task<bool> DropCollectionAsync(string collectionName)
		{
			Enforce.AgainstNullOrEmpty(() => collectionName);

			CheckInitialize();

			await Database.DropCollectionAsync(collectionName);
			return true;
		}

		protected async Task<bool> DropCollectionAsync<T>()
		{
			CheckInitialize();

			await Database.DropCollectionAsync(typeof(T).Name.ToLower());
			return true;
		}

        protected bool DropCollection(string collectionName)
        {
            Enforce.AgainstNullOrEmpty(() => collectionName);

			CheckInitialize();

			Database.DropCollection(collectionName);
            return true;
        }

        protected bool DropCollection<T>()
		{
			CheckInitialize();

			Database.DropCollection(typeof(T).Name.ToLower());
            return true;
        }

        protected IMongoCollection<BsonDocument> GetCollection(string collectionName)
        {
            return GetCollection<BsonDocument>(collectionName);
        }

        protected IMongoCollection<T> GetCollection<T>()
		{
			CheckInitialize();

			CollectionNameAttribute attribute = Utilities.Attributes.GetCustomAttribute<CollectionNameAttribute>(typeof(T), true);
            if ((attribute == null) || !string.IsNullOrEmpty(attribute.Name))
				return GetCollection<T>(typeof(T).Name.ToLower());

            return GetCollection<T>(attribute.Name.ToLower());
        }

        protected virtual IMongoCollection<T> GetCollection<T>(string collectionName)
        {
            Enforce.AgainstNullOrEmpty(() => collectionName);

			CheckInitialize();

			return Database.GetCollection<T>(collectionName);
		}

		protected virtual void InitializeConnection(IMongoDbRepositoryConnectionConfiguration connection)
        {
        }

        protected virtual IMongoClient InitializeClient(IMongoClient client, IMongoDbRepositoryConnectionConfiguration connectio)
        {
			return client;
        }
        #endregion

        #region Proetcted Properties
        public IMongoClient Client { get; private set; }
		private IMongoDbRepositoryConnectionConfiguration Connection { get; set; }
		public IMongoDatabase Database { get; private set; }
		#endregion

		#region Fields
		private IDictionary<string, IMongoClient> _clients = new Dictionary<string, IMongoClient>();
		private static readonly object LockConnection = new object();
		private static readonly object LockDatabase = new object();
		#endregion

		#region Constants
		private const string scheme = "mongodb://";
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