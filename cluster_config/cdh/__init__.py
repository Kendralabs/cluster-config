
def json(cluster):
    configs = {}
    for service in cluster.services:
        configs[service] = {}
        for role in cluster.services[service].roles:
            configs[service][role] = {}
            for config_group in cluster.services[service].roles[role].config_groups:
                configs[service][role][config_group] = {}
                for config in cluster.services[service].roles[role].config_groups[config_group].configs:
                    configs[service][role][config_group][config] = cluster.services[service].roles[role].config_groups[config_group].configs[config].value
    return configs