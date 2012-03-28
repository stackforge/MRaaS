package com.hpcloud.mraas.cli;

import com.hpcloud.mraas.MraasConfiguration;
import com.hpcloud.mraas.persistence.ClusterDAO;
import com.yammer.dropwizard.AbstractService;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.db.Database;
import com.yammer.dropwizard.db.DatabaseFactory;
import com.yammer.dropwizard.logging.Log;
import org.apache.commons.cli.CommandLine;

public class SetupDatabaseCommand extends ConfiguredCommand<MraasConfiguration> {

    public SetupDatabaseCommand() {
        super("setup", "Setup the database.");
    }

    @Override
    protected void run(AbstractService<MraasConfiguration> service, MraasConfiguration configuration, CommandLine params) throws Exception {

        final Log log = Log.forClass(SetupDatabaseCommand.class);
        final Environment environment = new Environment(configuration, service);
        final DatabaseFactory factory = new DatabaseFactory(environment);
        final Database db = factory.build(configuration.getDatabaseConfiguration(), "h2");
        final ClusterDAO clusterDAO = db.onDemand(ClusterDAO.class);

        log.info("creating tables.");
        clusterDAO.createClustersTable();
    }
}
