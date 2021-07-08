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
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;

using thZero.Data.Repository.MongoDb;

namespace MongoDB.Bson.Serialization.Conventions
{
    // http://stackoverflow.com/questions/19521626/mongodb-convention-packs
    // https://github.com/mongodb/mongo-csharp-driver/blob/c6ebb182517e128d1bd37da8a6970f3c7d4f6168/src/MongoDB.Bson/Serialization/Conventions/AttributeConventionPack.cs
    // https://github.com/mongodb/mongo-csharp-driver/blob/c6ebb182517e128d1bd37da8a6970f3c7d4f6168/src/MongoDB.Bson/Serialization/Conventions/ConventionRegistry.cs
    public class DataAnnotationAttributeConvention : ConventionBase, IClassMapConvention
    {
        #region Public Methods
        public void Apply(BsonClassMap classMap)
        {
            IgnoreMembersWithNotMappedAttribute(classMap);
        }
        #endregion

        #region Private Methods
        private static void IgnoreMembersWithNotMappedAttribute(BsonClassMap classMap)
        {
            foreach (var memberMap in classMap.DeclaredMemberMaps.ToList())
            {
                var ignoreMappingAttribute = (IgnoreMappingAttribute)memberMap.MemberInfo.GetCustomAttributes(typeof(IgnoreMappingAttribute), false).FirstOrDefault();
                var notMappedAttribute = (NotMappedAttribute)memberMap.MemberInfo.GetCustomAttributes(typeof(NotMappedAttribute), false).FirstOrDefault();
                if ((notMappedAttribute != null) || (ignoreMappingAttribute != null))
                    classMap.UnmapMember(memberMap.MemberInfo);
            }
        }
        #endregion
    }
}
