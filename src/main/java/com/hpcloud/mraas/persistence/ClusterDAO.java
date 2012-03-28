package com.hpcloud.mraas.persistence;

import com.hpcloud.mraas.domain.Cluster;
import com.google.common.collect.ImmutableList;
import java.util.Map;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;
import org.skife.jdbi.v2.tweak.BeanMapperFactory;

@RegisterMapperFactory(BeanMapperFactory.class)
public interface ClusterDAO {

    @SqlUpdate("create table clusters (id Serial primary key, numNodes int, tenant varchar(100), accessKey varchar(100), secretKey varchar(100), nodeIds clob)")
    void createClustersTable();

    @SqlQuery("select * from clusters where id = :id")
    Cluster findById(@Bind("id") long id);

    @SqlUpdate("insert into clusters(id, numNodes, tenant, accessKey, secretKey) values (:id, :numNodes, :tenant, :accessKey, :secretKey)")
    @GetGeneratedKeys
    long create(@BindBean Cluster cluster);

    @SqlUpdate("delete from clusters where id = :id")
    void deleteById(@Bind("id") long id);

    @SqlUpdate("update clusters set nodeIds = :nodeIds where id = :id")
    void updateNodes(@Bind("id") long id, @Bind("nodeIds") Map<String, String> nodeIds);

    @SqlQuery("select * from clusters")
    ImmutableList<Cluster> findAll();
}

