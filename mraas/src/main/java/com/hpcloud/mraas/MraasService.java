package com.hpcloud.mraas;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Environment;

import com.hpcloud.mraas.health.TemplateHealthCheck;
import com.hpcloud.mraas.resources.HelloWorldResource;

public class MraasService extends Service<MraasConfiguration> {
    public static void main(String[] args) throws Exception {
        new MraasService().run(args);
    }

    private MraasService() {
        super("mraas");
    }

    @Override
    protected void initialize(MraasConfiguration configuration,
                          Environment environment) {
        final String template = configuration.getTemplate();
        final String defaultName = configuration.getDefaultName();
        environment.addResource(new HelloWorldResource(template, defaultName));
        environment.addHealthCheck(new TemplateHealthCheck(template));
    }

}
