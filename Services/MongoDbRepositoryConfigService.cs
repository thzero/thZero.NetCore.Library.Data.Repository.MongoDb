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

namespace thZero.Data.Services.Repository.MongoDb
{
    public class MongoDbRepositoryConfigService : IMongoDbRepositoryConfigService
    {
        #region Public Methods
        public virtual void Initialize()
        {
			if (_initialized)
				return;

			lock (LockInitialized)
			{
				if (_initialized)
					return;

				InitializeConventions();
				InitializeDataMappings();

				InitializeI();

				_initialized = true;
			}
		}
		#endregion

		#region Protected Methods
		protected virtual void InitializeI()
        {
        }

		protected virtual void InitializeConventions()
        {
            MongoDB.Bson.Serialization.Conventions.ConventionPack pack = new();
            InitializeConventions(pack);
            MongoDB.Bson.Serialization.Conventions.ConventionRegistry.Register("additional", pack, t => true);
		}

		protected virtual void InitializeConventions(MongoDB.Bson.Serialization.Conventions.ConventionPack pack)
		{
			pack.Add(new MongoDB.Bson.Serialization.Conventions.DataAnnotationAttributeConvention());
			pack.Add(new MongoDB.Bson.Serialization.Conventions.IgnoreExtraElementsConvention(true));
			pack.Add(new MongoDB.Bson.Serialization.Conventions.IgnoreIfNullConvention(true));
			pack.Add(new MongoDB.Bson.Serialization.Conventions.LowerCaseElementNameConvention());
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
        #endregion

        #region Fields
        private static bool _initialized = false;

		private static readonly object LockInitialized = new();
		#endregion
	}
}
