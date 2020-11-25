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
                "StorageGroup": ["storage_group_id"],
                "SRP": ["srp_id"]}


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

    logger.debug(f"Sending Metrics for {host}")
    res = ZabbixSender(use_config=True).send(send_metrics)
    logger.debug(res)


def gather_array_perf(configpath, arrayid):
    """ Collects Array Level Performance Statistics """
    logger = logging.getLogger('discovery')
    logger.debug("Starting Array Perf Stats Collection ")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    logger.debug("Collecting Array Performance")

    try:
        metrics = conn.performance.get_array_stats(metrics='KPI',
                                                   array_id=arrayid,
                                                   recency=5)
    except PyU4V.utils.exception.VolumeBackendAPIException:
        logger.info("Metrics not read, recency not met")
        return

    logger.debug(metrics)

    process_perf_results(metrics, "Array")

    logger.debug("Completed Array Performance Gathering")


def gather_fe_perf(configpath, arrayid):
    """ Collects FE Level Performance Statistics """
    logger = logging.getLogger('discovery')
    logger.debug("Starting FE Perf Stats Collection ")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    logger.debug("Collecting FE Performance")

    # Gather our FE keys
    fe_adapters = conn.performance.get_frontend_director_keys(array_id=arrayid)
    logger.debug(fe_adapters)

    for adapter in fe_adapters:

        # Director Level Stats First
        dir_id = adapter['directorId']
        try:
            metrics = conn.performance.get_frontend_director_stats(
                           director_id=dir_id,
                           metrics='KPI',
                           array_id=arrayid,
                           recency=5)
        except PyU4V.utils.exception.VolumeBackendAPIException:
            logger.info("Metrics not read, recency not met")
            continue

        logger.debug(metrics)

        process_perf_results(metrics, "FEDirector")

        # Port Level Stats next
        try:
            ports = conn.performance.get_frontend_port_keys(dir_id,
                                                            array_id=arrayid)
        except PyU4V.utils.exception.ResourceNotFoundException:
            logger.debug(f"No ports found for dir: {dir_id} may be offline")
            continue

        for port in ports:
            port_id = port['portId']
            try:
                metrics = conn.performance.get_frontend_port_stats(
                               director_id=dir_id,
                               array_id=arrayid,
                               port_id=port_id,
                               metrics='KPI',
                               recency=5)
            except PyU4V.utils.exception.VolumeBackendAPIException:
                logger.info("Metrics not read, recency not met")
                continue

            logger.debug(metrics)

            process_perf_results(metrics, "FEPort")

    logger.debug("Completed FE Performance Gathering")


def gather_be_perf(configpath, arrayid):
    """ Collects BE Level Performance Statistics """
    logger = logging.getLogger('discovery')
    logger.debug("Starting BE Perf Stats Collection ")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    logger.debug("Collecting BE Performance")

    # Gather our BE keys
    be_directors = conn.performance.get_backend_director_keys(array_id=arrayid)
    logger.debug(be_directors)

    for director in be_directors:

        # Director Level Stats First
        dir_id = director['directorId']
        try:
            metrics = conn.performance.get_backend_director_stats(
                           director_id=dir_id,
                           metrics='KPI',
                           array_id=arrayid,
                           recency=5)
        except PyU4V.utils.exception.VolumeBackendAPIException:
            logger.info("Metrics not read, recency not met")
            continue

        logger.debug(metrics)

        process_perf_results(metrics, "BEDirector")

        # Port Level Stats next
        try:
            ports = conn.performance.get_backend_port_keys(dir_id,
                                                           array_id=arrayid)
        except PyU4V.utils.exception.ResourceNotFoundException:
            logger.debug(f"No ports found for dir: {dir_id} may be offline")
            continue

        for port in ports:
            port_id = port['portId']
            try:
                metrics = conn.performance.get_backend_port_stats(
                               director_id=dir_id,
                               array_id=arrayid,
                               port_id=port_id,
                               metrics='KPI',
                               recency=5)
            except PyU4V.utils.exception.VolumeBackendAPIException:
                logger.info("Metrics not read, recency not met")
                continue

            logger.debug(metrics)

            process_perf_results(metrics, "BEPort")

    logger.debug("Completed BE Performance Gathering")


def gather_srp_perf(configpath, arrayid):
    """ Collects SRP Performance Statistics """
    logger = logging.getLogger('discovery')
    logger.debug("Starting SRP Perf Stats Collection ")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    logger.debug("Collecting SRP Performance")

    # Gather our SRP keys
    srps = conn.performance.get_storage_resource_pool_keys(array_id=arrayid)
    logger.debug(srps)

    for pool in srps:

        srp_id = pool['srpId']
        try:
            metrics = conn.performance.get_storage_resource_pool_stats(
                           metrics='KPI',
                           array_id=arrayid,
                           srp_id=srp_id,
                           recency=5)
        except PyU4V.utils.exception.VolumeBackendAPIException:
            logger.info("Metrics not read, recency not met")
            continue

        logger.debug(metrics)

        process_perf_results(metrics, "SRP")

    logger.debug("Completed SRP Performance Gather")


def gather_storagegroup_perf(configpath, arrayid):
    """ Collects SRP Performance Statistics """
    logger = logging.getLogger('discovery')
    logger.debug("Starting Storage Group Perf Stats Collection ")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    logger.debug("Collecting Storage Group Performance")

    # Gather our Group keys
    groups = conn.performance.get_storage_group_keys(array_id=arrayid)
    logger.debug(groups)

    for group in groups:

        sg_id = group['storageGroupId']
        try:
            metrics = conn.performance.get_storage_group_stats(
                           metrics='KPI',
                           array_id=arrayid,
                           storage_group_id=sg_id,
                           recency=5)
        except PyU4V.utils.exception.VolumeBackendAPIException:
            logger.info(f"Metrics not read for {sg_id}, recency not met")
            continue

        logger.debug(metrics)

        process_perf_results(metrics, "StorageGroup")

    logger.debug("Completed Storage Group Performance Gather")


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


def do_be_discovery(configpath, arrayid):
    """ Perform a discovery of all the BEDirectors in the array """
    logger = logging.getLogger('discovery')
    logger.debug("Starting discovery for BE Directors")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    result = list()
    be_directors = conn.performance.get_backend_director_keys(array_id=arrayid)
    logger.debug(be_directors)

    for director in be_directors:
        dir_id = director['directorId']
        ports = None
        try:
            ports = conn.performance.get_backend_port_keys(dir_id)
        except PyU4V.utils.exception.ResourceNotFoundException:
            logger.debug(f"No ports found for director {dir_id}")

        if ports:
            for port in ports:
                result.append({'{#ARRAYID}': arrayid,
                               '{#BEDIRID}': dir_id,
                               '{#BEPORTID}': port['portId']})
        else:
            result.append({'{#ARRAYID}': arrayid,
                           '{#BEDIRID}': dir_id})

    logger.debug(result)
    logger.debug("Completed discovery for BE Directors")
    return result


def do_fe_discovery(configpath, arrayid):
    """ Perform a discovery of all the Directors in the array """
    logger = logging.getLogger('discovery')
    logger.debug("Starting discovery for FE Adapters")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    result = list()
    fe_adapters = conn.performance.get_frontend_director_keys(array_id=arrayid)
    logger.debug(fe_adapters)

    for adapter in fe_adapters:
        dir_id = adapter['directorId']
        ports = None
        try:
            ports = conn.performance.get_frontend_port_keys(dir_id)
        except PyU4V.utils.exception.ResourceNotFoundException:
            logger.debug(f"No ports found for director {dir_id}")

        if ports:
            for port in ports:
                result.append({'{#ARRAYID}': arrayid,
                               '{#DIRID}': dir_id,
                               '{#PORTID}': port['portId']})
        else:
            result.append({'{#ARRAYID}': arrayid,
                           '{#DIRID}': dir_id})

    logger.debug(result)
    logger.debug("Completed discovery for FE Adapters")
    return result


def do_srp_discovery(configpath, arrayid):
    """ Perform discovery of all SRPs in the array """
    logger = logging.getLogger('discovery')
    logger.debug("Starting discovery for FE Adapters")

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

    args = parser.parse_args()

    logger.debug("Arguments parsed: %s" % str(args))

    result = None
    if args.discovery:
        if args.director:
            logger.info("Executing Director Discovery")
            result = do_fe_discovery(args.configpath, args.array)
            result += do_be_discovery(args.configpath, args.array)

        elif args.srp:
            logger.info("Executing SRP Discovery")
            result = do_srp_discovery(args.configpath, args.array)

        elif args.storagegroup:
            logger.info("Executing StorageGroup Discovery")
            result = do_storagegroup_discovery(args.configpath, args.array)

        else:
            logger.info("Executing Array Discovery")
            result = do_array_discovery(args.configpath, args.array)

        print(zabbix_safe_output(result))

    else:
        if args.array:
            logger.info("Executing Array Stats")
            result = gather_array_health(args.configpath, args.array)
            result = gather_array_perf(args.configpath, args.array)
            result = gather_fe_perf(args.configpath, args.array)
            result = gather_be_perf(args.configpath, args.array)
            result = gather_srp_perf(args.configpath, args.array)
            result = gather_storagegroup_perf(args.configpath, args.array)

    logger.info("Complete")


if __name__ == '__main__':
    main()
