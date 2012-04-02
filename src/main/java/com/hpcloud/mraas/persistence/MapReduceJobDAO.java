package com.hpcloud.mraas.persistence;

import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.BindBean;
import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;
import org.skife.jdbi.v2.tweak.BeanMapperFactory;

import com.google.common.collect.ImmutableList;
import com.hpcloud.mraas.domain.ClusterJob;

@RegisterMapperFactory(BeanMapperFactory.class)
public interface MapReduceJobDAO {

    @SqlUpdate("create table jobs (id Serial primary key, nameNodeIP varchar(32), hdfsPort int, jobTrackPort int, state int)")
    void createJobsTable();
    
    @SqlQuery("select * from jobs where id = :id")
    ClusterJob findById(@Bind("id") long id);
    
    @SqlUpdate("insert into jobs(id, nameNodeIP, hdfsPort, jobTrackerPort, state) values (:id, :nameNodeIP, :hdfsPort, :jobTrackerPort, :state)")
    @GetGeneratedKeys
    long create(@BindBean ClusterJob clusterJob);
    
    @SqlUpdate("update jobs set state = :state where id = :id")
    void updateState(@Bind("id") long id, @Bind("state") int state);
    
    @SqlQuery("select * from jobs")
    ImmutableList<ClusterJob> findAll();
}
