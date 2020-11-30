#!/opt/rh/rh-python36/root/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import sys
import json
import PyU4V
import argparse
import traceback
import logging
import logging.handlers
from pyzabbix import ZabbixMetric, ZabbixSender

log_level = logging.DEBUG
key_base = 'dellemc.pmax.'
host_base = 'PowerMax {arrayid}'
metric_recency = 15


def log_exception_handler(type, value, tb):
    logger = logging.getLogger('discovery')
    # Dump the traceback
    logger.exception("Uncaught exception: {0}".format(str(value)))

    for i in traceback.format_tb(tb):
        logger.debug(i)


def setup_logging(log_file):
    """ Sets up our file logging with rotation """
    my_logger = logging.getLogger('discovery')
    my_logger.setLevel(log_level)

    handler = logging.handlers.RotatingFileHandler(
                          log_file, maxBytes=5120000, backupCount=5)

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(process)d %(message)s')
    handler.setFormatter(formatter)

    my_logger.addHandler(handler)

    sys.excepthook = log_exception_handler

    return


def generate_metric_key(base, category, metric, identifier):
    metric_key = f'{base}perf.{category}.{metric}[{identifier}]'
    return metric_key


def zabbix_safe_output(data):
    """ Generate JSON output for zabbix from a passed in list of dicts """
    logger = logging.getLogger('discovery')
    logger.info("Generating output")
    output = json.dumps({"data": data}, indent=4, separators=(',', ': '))

    logger.debug(json.dumps({"data": data}))

    return output


def fix_ts(timestamp):
    """ Remove milliseconds from timestamps """
    s, ms = divmod(int(timestamp), 1000)
    return s


def gather_array_health(configpath, arrayid):
    """ Collects Array Health Scores """
    logger = logging.getLogger('discovery')
    logger.debug("Starting Health Score Gathering")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    logger.debug("Collecting Health")
    health = conn.system.get_system_health(array_id=arrayid)
    logger.debug(health)

    # Loop through the collected health stats and send to
    # Zabbix via the sender
    for i in health['health_score_metric']:
        host = host_base.format(arrayid=arrayid)

        metric_key = '{base}health.{metric}[{arrayid}]'.format(
                      base=key_base, metric=i['metric'],
                      arrayid=arrayid)
        score = i['health_score']
        timestamp = fix_ts(i['data_date'])

        logger.debug("Sending Metric {host} - {met} - {score} - {ts}".format(
                     host=host, met=metric_key, score=score,
                     ts=timestamp))
        health_metric = ZabbixMetric(host, metric_key, score, timestamp)
        ZabbixSender(use_config=True).send([health_metric])

    logger.debug("Completed Health Score Gathering")


# This dict maps the category to the identifiers in the result set
# that are used in identifiers for Zabbix keys

category_map = {"Array": ["array_id"],
                "FEDirector": ["director_id"],
                "FEPort": ["director_id", "port_id"],
                "BEDirector": ["director_id"],
                "BEPort": ["director_id", "port_id"],
                "RDFDirector": ["director_id"],
                "RDFPort": ["director_id", "port_id"],
                "IMDirector": ["director_id"],
                "EDSDirector": ["director_id"],
                "StorageGroup": ["storage_group_id"],
                "SRP": ["srp_id"],
                "PortGroup": ["port_group_id"]}


def process_perf_results(metrics, category):

    logger = logging.getLogger('discovery')
    host = host_base.format(arrayid=metrics['array_id'])

    # Based on category, pull our our identifiers and format
    id_values = list()
    for i in category_map[category]:
        id_values.append(metrics[i])

    ident = "-".join(id_values)
    cat = category.lower()

    # Drop the ms from our timestamp, we've only got
    # 5 minute granularity at best here
    timestamp = fix_ts(metrics['timestamp'])

    metric_data = metrics['result'][0]

    send_metrics = list()
    for metric, score in metric_data.items():
        if 'timestamp' in metric:    # ignore the second timestamp
            continue

        key = generate_metric_key(key_base, cat, metric, ident)

        logger.debug(f"Built Metric: {key} for {host}")
        send_metrics.append(ZabbixMetric(host, key, score, timestamp))

    logger.debug("Sending Metrics")
    res = ZabbixSender(use_config=True).send(send_metrics)
    logger.debug(res)
    logger.debug("Completed sending Metrics")


def gather_dir_perf(configpath, arrayid, category):
    """ Collects FE Level Performance Statistics """
    logger = logging.getLogger('discovery')
    logger.debug(f"Starting {category} Perf Stats Collection")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    port_cat = category.replace('Director', 'Port')

    func_map = {'FEDirector':
                {'keys': conn.performance.get_frontend_director_keys,
                 'stats': conn.performance.get_frontend_director_stats},
                'BEDirector':
                {'keys': conn.performance.get_backend_director_keys,
                 'stats': conn.performance.get_backend_director_stats},
                'RDFDirector':
                {'keys': conn.performance.get_rdf_director_keys,
                 'stats': conn.performance.get_rdf_director_stats},
                'EDSDirector':
                {'keys': conn.performance.get_eds_director_keys,
                 'stats': conn.performance.get_eds_director_stats},
                'IMDirector':
                {'keys': conn.performance.get_im_director_keys,
                 'stats': conn.performance.get_im_director_stats},
                'FEPort':
                {'keys': conn.performance.get_frontend_port_keys,
                 'stats': conn.performance.get_frontend_port_stats},
                'BEPort':
                {'keys': conn.performance.get_backend_port_keys,
                 'stats': conn.performance.get_backend_port_stats},
                'RDFPort':
                {'keys': conn.performance.get_rdf_port_keys,
                 'stats': conn.performance.get_rdf_port_stats}}

    try:
        directors = func_map[category]['keys'](array_id=arrayid)
        logger.debug(directors)
    except PyU4V.utils.exception.ResourceNotFoundException:
        logger.debug(f"No {category} Directors found")

    for director in directors:
        dir_id = director['directorId']

        try:
            metrics = func_map[category]['stats'](array_id=arrayid,
                                                  director_id=dir_id,
                                                  metrics='KPI',
                                                  recency=metric_recency)
        except PyU4V.utils.exception.VolumeBackendAPIException:
            logger.info("Current metrics do not meet recency requirements")
            break

        logger.debug(metrics)

        process_perf_results(metrics, category)

        # Port Level Stats (if they exist)
        try:
            if port_cat in func_map:
                ports = func_map[port_cat]['keys'](array_id=arrayid,
                                                   director_id=dir_id)
                logger.debug(ports)
            else:
                ports = list()
        except PyU4V.utils.exception.ResourceNotFoundException:
            logger.debug(f"No ports found for dir: {dir_id} may be offline")
            continue

        for port in ports:
            port_id = port['portId']
            try:
                metrics = func_map[port_cat]['stats'](
                               director_id=dir_id,
                               array_id=arrayid,
                               port_id=port_id,
                               metrics='KPI',
                               recency=metric_recency)
            except PyU4V.utils.exception.VolumeBackendAPIException:
                logger.info("Metrics not read, recency not met")
                continue

            logger.debug(metrics)

            process_perf_results(metrics, port_cat)

    logger.debug("Completed Director Performance Gathering")


def gather_perf(configpath, arrayid, category):
    logger = logging.getLogger('discovery')
    logger.info(f"Starting {category} Stats Collection ")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    func_map = {'PortGroup':
                {'keys': conn.performance.get_port_group_keys,
                 'stats': conn.performance.get_port_group_stats,
                 'args': {'port_group_id': 'portGroupId'}},
                'SRP':
                {'keys': conn.performance.get_storage_resource_pool_keys,
                 'stats': conn.performance.get_storage_resource_pool_stats,
                 'args': {'srp_id': 'srpId'}},
                'StorageGroup':
                {'keys': conn.performance.get_storage_group_keys,
                 'stats': conn.performance.get_storage_group_stats,
                 'args': {'storage_group_id': 'storageGroupId'}},
                'Array':
                {'keys': conn.performance.get_array_keys,
                 'stats': conn.performance.get_array_stats,
                 'args': {}}
                }

    try:
        if 'Array' not in category:
            items = func_map[category]['keys'](array_id=arrayid)
        else:
            items = func_map[category]['keys']()
        logger.debug(items)
    except PyU4V.utils.exception.ResourceNotFoundException:
        logger.info(f"No {category} found")

    metric_params = {'recency': metric_recency,
                     'metrics': 'KPI'}

    if 'Array' not in category:
        metric_params['array_id'] = arrayid

    for item in items:
        # We need to dynamically update the dict we're using for kwargs
        # to include the appropriate parameters for this category item
        for m_key, i_key in func_map[category]['args'].items():
            metric_params[m_key] = item[i_key]

        logger.debug("Metric Parameters to be passed")
        logger.debug(metric_params)

        try:
            metrics = func_map[category]['stats'](**metric_params)
            logger.debug("Metrics returned")
            logger.debug(metrics)
        except PyU4V.utils.exception.VolumeBackendAPIException:
            logger.info(f"Metrics not read for {category}, recency not met")
            return

        process_perf_results(metrics, category)

    logger.info(f"Completed {category} Stats Collection")


def do_array_discovery(configpath, arrayid):
    """ Perform a discovery of all Arrays attached to U4V """
    logger = logging.getLogger('discovery')
    logger.debug("Starting discovery for Array")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    result = list()
    arrays_in_uni = conn.common.get_array_list()
    logger.debug(arrays_in_uni)

    if arrayid in conn.common.get_array_list():

        result.append({'{#ARRAYID}': arrayid})
        logger.debug(result)

    logger.debug("Completed discovery for Array")
    return result


def do_storagegroup_discovery(configpath, arrayid):
    """ Perform a discovery of all Arrays attached to U4V """
    logger = logging.getLogger('discovery')
    logger.debug("Starting Storage Group Discovery")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    result = list()
    groups = conn.performance.get_storage_group_keys(array_id=arrayid)
    logger.debug(groups)

    for group in groups:
        sg_id = group['storageGroupId']
        result.append({'{#ARRAYID}': arrayid,
                       '{#SGID}': sg_id})

    logger.debug(result)
    logger.debug("Completed Storage Group Discovery")
    return result


def do_director_discovery(configpath, arrayid, category):
    """ Perform a discovery of all the Directors in the array """
    logger = logging.getLogger('discovery')
    logger.debug(f"Starting discovery for {category}")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    func_map = {'FEDirector':
                {'id': '',
                 'keys': conn.performance.get_frontend_director_keys,
                 'ports': conn.performance.get_frontend_port_keys},
                'BEDirector':
                {'id': 'BE',
                 'keys': conn.performance.get_backend_director_keys,
                 'ports': conn.performance.get_backend_port_keys},
                'RDFDirector':
                {'id': 'RDF',
                 'keys': conn.performance.get_rdf_director_keys,
                 'ports': conn.performance.get_rdf_port_keys},
                'EDSDirector':
                {'id': 'EDS',
                 'keys': conn.performance.get_eds_director_keys},
                'IMDirector':
                {'id': 'IM',
                 'keys': conn.performance.get_im_director_keys}}

    result = list()
    directors = func_map[category]['keys'](array_id=arrayid)
    logger.debug(directors)

    for director in directors:
        dir_id = director['directorId']
        dir_key = f"{{#{func_map[category]['id']}DIRID}}"

        if 'ports' in func_map[category]:
            ports = list()
            try:
                ports = func_map[category]['ports'](array_id=arrayid,
                                                    director_id=dir_id)
                logger.debug(ports)
            except PyU4V.utils.exception.ResourceNotFoundException:
                logger.debug(f"No ports found for director {dir_id}")

            port_key = f"{{#{func_map[category]['id']}PORTID}}"
            if ports:
                for port in ports:
                    result.append({'{#ARRAYID}': arrayid,
                                   dir_key: dir_id,
                                   port_key: port['portId']})
        else:
            result.append({'{#ARRAYID}': arrayid,
                           dir_key: dir_id})

    logger.debug(result)
    logger.debug(f"Completed discovery for {category}")
    return result


def do_srp_discovery(configpath, arrayid):
    """ Perform discovery of all SRPs in the array """
    logger = logging.getLogger('discovery')
    logger.debug("Starting discovery for SRPs")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    result = list()
    srps = conn.performance.get_storage_resource_pool_keys(array_id=arrayid)
    logger.debug(srps)

    for pool in srps:
        result.append({'{#ARRAYID}': arrayid,
                       '{#SRPID}': pool['srpId']})

    logger.debug(result)
    logger.debug("Completed discovery for SRPs")
    return result


def do_portgroup_discovery(configpath, arrayid):
    """ Perform discovery of all Port Groups in the array """
    logger = logging.getLogger('discovery')
    logger.debug("Starting port group discovery")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    result = list()
    pgs = conn.performance.get_port_group_keys(array_id=arrayid)
    logger.debug(pgs)

    for pg in pgs:
        result.append({'{#ARRAYID}': arrayid,
                       '{#PGID}': pg['portGroupId']})

    logger.debug(result)
    logger.debug("COmpleted Port Group Discovery")
    return(result)


def main():

    log_file = '/tmp/zabbix_powermax.log'
    setup_logging(log_file)

    logger = logging.getLogger('discovery')

    logger.info("Started PowerMax Zabbix Integration")

    parser = argparse.ArgumentParser()

    parser.add_argument('--discovery', '-d', action='store_true',
                        help="Perform Discovery Operations")

    parser.add_argument('--configpath', '-c', action='store',
                        help="Path to U4V config file", required=True,
                        default=".")

    parser.add_argument('--array', '-a', action='store', required=True,
                        help="Perform array stat or array discovery")

    parser.add_argument('--director', action='store_true',
                        help="Perform director discovery")

    parser.add_argument('--srp', action='store_true',
                        help="Perform SRP discovery")

    parser.add_argument('--storagegroup', action='store_true',
                        help="Perform Storage Group discovery")

    parser.add_argument('--portgroup', action='store_true',
                        help="Perform Port Group discovery")

    args = parser.parse_args()

    logger.debug("Arguments parsed: %s" % str(args))

    result = None
    if args.discovery:
        if args.director:
            logger.info("Executing Director Discovery")
            result = list()
            for dir_cat in ['BEDirector', 'FEDirector', 'RDFDirector',
                            'EDSDirector', 'IMDirector']:
                result += do_director_discovery(args.configpath,
                                                args.array,
                                                category=dir_cat)

        elif args.srp:
            logger.info("Executing SRP Discovery")
            result = do_srp_discovery(args.configpath, args.array)

        elif args.storagegroup:
            logger.info("Executing StorageGroup Discovery")
            result = do_storagegroup_discovery(args.configpath, args.array)

        elif args.portgroup:
            logger.info("Executing POrt Group Discovery")
            result = do_portgroup_discovery(args.configpath, args.array)

        else:
            logger.info("Executing Array Discovery")
            result = do_array_discovery(args.configpath, args.array)

        print(zabbix_safe_output(result))

    else:
        if args.array:

            logger.info("Executing Stat collection")
            result = gather_array_health(args.configpath, args.array)

            # Get data for ALL director types
            for dir_cat in ['BEDirector', 'FEDirector', 'RDFDirector',
                            'EDSDirector', 'IMDirector']:
                result = gather_dir_perf(args.configpath,
                                         args.array,
                                         category=dir_cat)

            for perf_cat in ['SRP', 'PortGroup', 'StorageGroup', 'Array']:
                result = gather_perf(args.configpath, args.array,
                                     category=perf_cat)

    logger.info("Complete")


if __name__ == '__main__':
    main()
