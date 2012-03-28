package com.hpcloud.mraas;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.db.Database;
import com.yammer.dropwizard.db.DatabaseFactory;

import com.hpcloud.mraas.cli.SetupDatabaseCommand;
import com.hpcloud.mraas.persistence.ClusterDAO;
import com.hpcloud.mraas.health.TemplateHealthCheck;
import com.hpcloud.mraas.resources.ClusterResource;
import com.hpcloud.mraas.resources.ClustersResource;
import com.hpcloud.mraas.app.Provisioner;
import com.hpcloud.mraas.app.Destroyer;

public class MraasService extends Service<MraasConfiguration> {
    public static void main(String[] args) throws Exception {
        new MraasService().run(args);
    }

    private MraasService() {
        super("mraas");
        addCommand(new SetupDatabaseCommand());
    }

    @Override
    protected void initialize(MraasConfiguration configuration,
                          Environment environment) throws ClassNotFoundException {
        final DatabaseFactory factory = new DatabaseFactory(environment);
        final Database db = factory.build(configuration.getDatabaseConfiguration(), "h2");
        final ClusterDAO clusterDAO = db.onDemand(ClusterDAO.class);

        environment.addResource(new ClusterResource(clusterDAO, new Destroyer()));
        environment.addResource(new ClustersResource(clusterDAO, new Provisioner()));
        environment.addHealthCheck(new TemplateHealthCheck("Hello, %s!"));
    }

}
