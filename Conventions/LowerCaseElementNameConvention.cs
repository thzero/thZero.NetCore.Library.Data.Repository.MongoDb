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

namespace MongoDB.Bson.Serialization.Conventions
{
    public class LowerCaseElementNameConvention : IMemberMapConvention
    {
        #region Public Methods
        public void Apply(BsonMemberMap memberMap)
        {
            memberMap.SetElementName(Char.ToLowerInvariant(memberMap.MemberName[0]) + memberMap.MemberName[1..]);
        }
        #endregion

        #region Public Properties
        public string Name
        {
            get { return "LowerCaseElementNameConvention"; }
        }
        #endregion
    }
}
