package com.hpcloud.mraas.db;

import com.hpcloud.mraas.core.Cluster;
import com.google.common.collect.ImmutableList;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;
import org.skife.jdbi.v2.sqlobject.stringtemplate.ExternalizedSqlViaStringTemplate3;
import org.skife.jdbi.v2.tweak.BeanMapperFactory;

@ExternalizedSqlViaStringTemplate3
@RegisterMapperFactory(BeanMapperFactory.class)
public interface ClusterDAO {

    @SqlUpdate
    void createClustersTable();

    @SqlQuery
    Cluster findById(@Bind("id") long id);

    @SqlUpdate
    @GetGeneratedKeys
    long create(@BindBean Cluster cluster);

    @SqlQuery
    ImmutableList<Cluster> findAll();
}

