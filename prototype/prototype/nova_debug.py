#!/usr/bin/python -u
import novaclient.v1_1 as nc
import config

nova = nc.client.Client(config.name, config.password, config.project_id, config.auth_url, service_type='compute')

kpmgr = kpmgr = nc.keypairs.KeypairManager(nova)

import pdb; pdb.set_trace()
