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

using MongoDB.Driver;

namespace thZero.Data.Repository.MongoDb
{
    public class SearchFilterMongoDb<TEntity> : SearchFilterEntity<TEntity>
         where TEntity : class
    {
        #region Public Properties
        public FilterDefinition<TEntity> FilterEx { get; set; }
        public FilterDefinitionBuilder<TEntity> FilterBuilder => (FilterDefinitionBuilder<TEntity>)Filter;
        public SortDefinitionBuilder<TEntity> SortBuilder => (SortDefinitionBuilder<TEntity>)Sort;
        public SortDefinition<TEntity> SortEx { get; set; }
        #endregion
    }

    public static class UtilitySearchFilterMongoDb
    {
        #region Public Methods
        public static TSearch InitializeSearchCriteria<TSearch, TEntity>()
            where TSearch : SearchFilterMongoDb<TEntity>
            where TEntity : class
        {
            var filter = Builders<TEntity>.Filter;
            var sort = Builders<TEntity>.Sort;

            //SearchCriteria<TEntity> criteria = new SearchCriteria<TEntity>();
            TSearch criteria = thZero.Utilities.Activator.CreateInstanceEx<TSearch>();
            criteria.Filter = filter;
            criteria.Sort = sort;
            return criteria;
        }
        #endregion
    }
}
