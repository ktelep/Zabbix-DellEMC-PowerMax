#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import sys
import json
import PyU4V
import argparse
import traceback
import logging
import logging.handlers
from pyzabbix import ZabbixMetric, ZabbixSender

# Update to include your Zabbix Server IP and Port
zabbix_ip = "192.168.1.64"
zabbix_port = 10051

# Logging Level INFO as default, change to DEBUG for more
# detailed info or troubleshooting
log_level = logging.DEBUG
log_file = "./zabbix_powermax.log"

# Host Base is the pattern used to define the Array in Zabbix
# it is case-sensitive
host_base = "PowerMax {arrayid}"

# Base name for the keys created, you can customize if you don't like
# the default
key_base = "dellemc.pmax."

# Metric recency is used to determine how "fresh" our stats must be
#  5 is the default (5 minutes), use 0 for testing.  Note this does
# not change how often diagnostic data is collected ON the array
metric_recency = 5


def log_exception_handler(type, value, tb):
    """ Handle all tracebacks and exceptions going to the logfile """
    logger = logging.getLogger('discovery')
    # Dump the traceback
    logger.exception("Uncaught exception: {0}".format(str(value)))

    for i in traceback.format_tb(tb):
        logger.debug(i)


def setup_logging(log_file):
    """ Sets up our file logging with rotation """
    my_logger = logging.getLogger('discovery')
    my_logger.setLevel(log_level)

    try:
        handler = logging.handlers.RotatingFileHandler(
                          log_file, maxBytes=5120000, backupCount=5)
    except PermissionError:
        print(f"ERROR: Error writing to or creating {log_file}")
        print("       Please verify permissions and path to file")
        sys.exit()

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(process)d %(message)s')
    handler.setFormatter(formatter)

    my_logger.addHandler(handler)

    sys.excepthook = log_exception_handler

    return


def generate_metric_key(base, category, metric, identifier):
    """ Generate a Zabbix formatted key """
    metric_key = f'{base}perf.{category}.{metric}[{identifier}]'
    return metric_key


def zabbix_safe_output(data):
    """ Generate JSON output for zabbix from a passed in list of dicts
        This is Zabbix 4.x and higher compatible """

    logger = logging.getLogger('discovery')
    logger.info("Generating output")
    output = json.dumps({"data": data}, indent=4, separators=(',', ': '))
    logger.debug(json.dumps({"data": data}))

    return output


def fix_ts(timestamp):
    """ Remove milliseconds from timestamps """
    s, ms = divmod(int(timestamp), 1000)
    # import time
    # s = int(time.time())    # Uncomment for testing
    return s


def gather_array_health(configpath, arrayid):
    """ Collects Array Health Scores """
    logger = logging.getLogger('discovery')
    logger.info("Starting Health Score Gathering")

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

        # Health Score may not be populated if we're between system checks
        # So valide it's there before we try to send it
        if 'health_score' in i:
            score = i['health_score']
            timestamp = fix_ts(i['data_date'])

            logger.debug(f"Sending Metric {host} - {metric_key} - "
                         f"{score} - {timestamp}")
            health_metric = ZabbixMetric(host, metric_key, score, timestamp)

            ZabbixSender(zabbix_server=zabbix_ip,
                         zabbix_port=zabbix_port).send([health_metric])
        else:
            logger.debug(f"No health score available for {i['metric']}")
    logger.info("Completed Health Score Gathering")


def process_perf_results(metrics, category):
    """ Process metrics collected from the _stats function by category """
    logger = logging.getLogger('discovery')
    host = host_base.format(arrayid=metrics['array_id'])

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
                    "Board": ["board_id"],
                    "DiskGroup": ["disk_group_id"],
                    "PortGroup": ["port_group_id"],
                    "BeEmulation": ["be_emulation_id"],
                    "FeEmulation": ["fe_emulation_id"],
                    "EDSEmulation": ["eds_emulation_id"],
                    "IMEmulation": ["im_emulation_id"],
                    "RDFEmulation": ["rdf_emulation_id"],
                    "Host": ["host_id"],
                    "Initiator": ["initiator_id"],
                    "RDFA": ["ra_group_id"],
                    "RDFS": ["rs_group_id"],
                    "ISCSITarget": ['iscsi_target_id']
                    }

    # Based on category, pull our our identifiers and format
    id_values = list()
    for i in category_map[category]:
        id_values.append(metrics[i])

    ident = "-".join(id_values)
    cat = category.lower()

    for metric_data in metrics['result']:

        # Drop the ms from our timestamp, we've only got
        # 5 minute granularity at best here
        timestamp = fix_ts(metric_data['timestamp'])

        send_metrics = list()
        # Bundle up all our metrics into a single list to send to Zabbix
        for metric, score in metric_data.items():
            if 'timestamp' in metric:    # ignore the second timestamp
                continue

            key = generate_metric_key(key_base, cat, metric, ident)

            logger.debug(f"Built Metric: {key} for {host} - ts: {timestamp}")
            send_metrics.append(ZabbixMetric(host, key, score, timestamp))

        logger.debug("Sending Metrics")

        # Send the actual metrics list
        res = ZabbixSender(zabbix_server=zabbix_ip,
                           zabbix_port=zabbix_port).send(send_metrics)
        logger.info(res)

    logger.debug("Completed sending Metrics")


def gather_dir_perf(configpath, arrayid, category, hours=None):
    """ Collects Director Level Performance Statistics """
    logger = logging.getLogger('discovery')
    logger.info(f"Starting {category} Perf Stats Collection")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    # Map our function to to it's matching ports
    # FEDirector = FEPorts, etc..
    port_cat = category.replace('Director', 'Port')

    # Function map to make this a generalized function vs. having
    # an individual one.   PyU4V provides a generalized function but
    # it seems to be troublesome.
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

    # Gather the keys for the director, this will throw an exception if
    # the box doesn't have a specific director type (like RDF)
    try:
        directors = func_map[category]['keys'](array_id=arrayid)
        logger.debug(directors)
    except PyU4V.utils.exception.ResourceNotFoundException:
        logger.info(f"No {category} Directors found")

    for director in directors:
        dir_id = director['directorId']
        logger.info(f"Collecting for {category} director {dir_id}")

        # this will be the kwargs passed to the stats function when called
        metric_params = {'recency': metric_recency,
                         'array_id': arrayid,
                         'metrics': 'KPI',
                         'director_id': dir_id}

        # Handle where we want multiple hours of data
        if hours:
            recent_time = conn.performance.get_last_available_timestamp()
            start_time, end_time = conn.performance.get_timestamp_by_hour(
                end_time=recent_time, hours_difference=hours)

            metric_params['start_time'] = start_time
            metric_params['end_time'] = end_time

        # Gather metrics, but gracefully handle if they're not recent enough
        try:
            metrics = func_map[category]['stats'](**metric_params)

        except PyU4V.utils.exception.VolumeBackendAPIException:
            logger.info("Current metrics do not meet recency requirements")
            break

        logger.debug(metrics)

        # Send them off to be processed and sent to Zabbix
        process_perf_results(metrics, category)

        # Port Level Stats (if they exist) follows the same pattern
        # but not all directors have ports (EDS and IM for ex.)
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
            logger.info(f"Collecting metrics for {category}"
                        f" {dir_id} port {port_id}")
            try:
                metric_params['port_id'] = port_id
                metrics = func_map[port_cat]['stats'](**metric_params)
            except PyU4V.utils.exception.VolumeBackendAPIException:
                logger.info("Metrics not read, recency not met")
                continue

            logger.debug(metrics)

            process_perf_results(metrics, port_cat)

    logger.info("Completed Director Performance Gathering")


def gather_perf(configpath, arrayid, category, hours=None):
    """ Generalized non-Director performance gathering """
    logger = logging.getLogger('discovery')
    logger.info(f"Starting {category} Stats Collection ")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    # Map our categories to functions and what arguments map to responses
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
                'DiskGroup':
                {'keys': conn.performance.get_disk_group_keys,
                 'stats': conn.performance.get_disk_group_stats,
                 'args': {'disk_group_id': 'diskGroupId'}},
                'Board':
                {'keys': conn.performance.get_board_keys,
                 'stats': conn.performance.get_board_stats,
                 'args': {'board_id': 'boardId'}},
                'BeEmulation':
                {'keys': conn.performance.get_backend_emulation_keys,
                 'stats': conn.performance.get_backend_emulation_stats,
                 'args': {'emulation_id': 'beEmulationId'}},
                'FeEmulation':
                {'keys': conn.performance.get_frontend_emulation_keys,
                 'stats': conn.performance.get_frontend_emulation_stats,
                 'args': {'emulation_id': 'feEmulationId'}},
                'EDSEmulation':
                {'keys': conn.performance.get_eds_emulation_keys,
                 'stats': conn.performance.get_eds_emulation_stats,
                 'args': {'emulation_id': 'edsEmulationId'}},
                'IMEmulation':
                {'keys': conn.performance.get_im_emulation_keys,
                 'stats': conn.performance.get_im_emulation_stats,
                 'args': {'emulation_id': 'imEmulationId'}},
                'RDFEmulation':
                {'keys': conn.performance.get_rdf_emulation_keys,
                 'stats': conn.performance.get_rdf_emulation_stats,
                 'args': {'emulation_id': 'rdfEmulationId'}},
                'Host':
                {'keys': conn.performance.get_host_keys,
                 'stats': conn.performance.get_host_stats,
                 'args': {'host_id': 'hostId'}},
                'Initiator':
                {'keys': conn.performance.get_initiator_perf_keys,
                 'stats': conn.performance.get_initiator_stats,
                 'args': {'initiator_id': 'initiatorId'}},
                'RDFS':
                {'keys': conn.performance.get_rdfs_keys,
                 'stats': conn.performance.get_rdfs_stats,
                 'args': {'rdfs_group_id': 'rsGroupId'}},
                'RDFA':
                {'keys': conn.performance.get_rdfa_keys,
                 'stats': conn.performance.get_rdfa_stats,
                 'args': {'rdfa_group_id': 'raGroupId'}},
                'ISCSITarget':
                {'keys': conn.performance.get_iscsi_target_keys,
                 'stats': conn.performance.get_iscsi_target_stats,
                 'args': {'iscsi_target_id': 'iscsiTargetId'}},
                'Array':
                {'keys': conn.performance.get_array_keys,
                 'stats': conn.performance.get_array_stats,
                 'args': {}}
                }

    try:
        if 'Array' not in category:
            items = func_map[category]['keys'](array_id=arrayid)
        else:
            # Special case, array object can't have array_id passed
            items = func_map[category]['keys']()
        logger.debug(items)
    except PyU4V.utils.exception.ResourceNotFoundException:
        logger.info(f"No {category} found")
        return

    # this will be the kwargs passed to the stats function when called
    metric_params = {'recency': metric_recency,
                     'metrics': 'KPI'}

    # Handle where we want multiple hours of data
    if hours:
        recent_time = conn.performance.get_last_available_timestamp()
        start_time, end_time = conn.performance.get_timestamp_by_hour(
            end_time=recent_time, hours_difference=hours)

        metric_params['start_time'] = start_time
        metric_params['end_time'] = end_time

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
    """ Perform a discovery of the array attached to U4V """
    logger = logging.getLogger('discovery')
    logger.info("Starting discovery for Array")

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    result = list()
    arrays_in_uni = conn.common.get_array_list()
    logger.debug(arrays_in_uni)

    if arrayid in conn.common.get_array_list():

        result.append({'{#ARRAYID}': arrayid})
        logger.debug(result)

    logger.info("Completed discovery for Array")
    return result


def do_director_discovery(configpath, arrayid, category, ports=False):
    """ Perform a discovery of all the Directors in the array """
    logger = logging.getLogger('discovery')
    logger.info(f"Starting discovery for {category}")

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
        logger.info(f"Discovering {category} {dir_id}")

        # Build the Zabbix formatted key for the director for LLD
        dir_key = f"{{#{func_map[category]['id']}DIRID}}"

        if not ports:
            result.append({'{#ARRAYID}': arrayid, dir_key: dir_id})
        else:
            # Now we find our director ports
            if 'ports' in func_map[category]:
                ports = list()
                try:
                    ports = func_map[category]['ports'](array_id=arrayid,
                                                        director_id=dir_id)
                    logger.debug(ports)
                except PyU4V.utils.exception.ResourceNotFoundException:
                    logger.info(f"No ports found for director {dir_id}")

                port_key = f"{{#{func_map[category]['id']}PORTID}}"
                if ports:
                    for port in ports:
                        port_id = f"{dir_id}-{port['portId']}"
                        result.append({'{#ARRAYID}': arrayid,
                                       port_key: port_id})

    logger.debug(result)
    logger.info(f"Completed discovery for {category}")

    return result


def do_item_discovery(configpath, arrayid, category):
    """ Perform discoveyr of items on the array """
    logger = logging.getLogger('discovery')
    logger.info(f"Starting item discovery for {category}")

    if 'Array' in category:  # Special case for array
        return do_array_discovery(configpath, arrayid)

    PyU4V.univmax_conn.file_path = configpath
    conn = PyU4V.U4VConn()

    result = list()

    func_map = {'PortGroup': {
                    'keys': conn.performance.get_port_group_keys,
                    'id': 'PGID',
                    'idparam': 'portGroupId'},
                'SRP': {
                    'keys': conn.performance.get_storage_resource_pool_keys,
                    'idparam': 'srpId',
                    'id': 'SRPID'},
                'DiskGroup': {
                    'keys': conn.performance.get_disk_group_keys,
                    'idparam': 'diskGroupId',
                    'id': 'DISKGID'},
                'StorageGroup': {
                    'keys': conn.performance.get_storage_group_keys,
                    'idparam': 'storageGroupId',
                    'id': 'SGID'},
                'BeEmulation': {
                    'keys': conn.performance.get_backend_emulation_keys,
                    'idparam': 'beEmulationId',
                    'id': 'BEEMUID'},
                'FeEmulation': {
                    'keys': conn.performance.get_frontend_emulation_keys,
                    'idparam': 'feEmulationId',
                    'id': 'FEEMUID'},
                'EDSEmulation': {
                    'keys': conn.performance.get_eds_emulation_keys,
                    'idparam': 'edsEmulationId',
                    'id': 'EDSEMUID'},
                'IMEmulation': {
                    'keys': conn.performance.get_im_emulation_keys,
                    'idparam': 'imEmulationId',
                    'id': 'IMEMUID'},
                'RDFEmulation': {
                    'keys': conn.performance.get_rdf_emulation_keys,
                    'idparam': 'rdfEmulationId',
                    'id': 'RDFEMUID'},
                'Host': {
                    'keys': conn.performance.get_host_keys,
                    'idparam': 'hostId',
                    'id': 'PMHOSTID'},
                'Initiator': {
                    'keys': conn.performance.get_initiator_perf_keys,
                    'idparam': 'initiatorId',
                    'id': 'INITID'},
                'RDFS': {
                    'keys': conn.performance.get_rdfs_keys,
                    'idparam': 'rdfsGroupId',
                    'id': 'RDFSGID'},
                'RDFA': {
                    'keys': conn.performance.get_rdfa_keys,
                    'idparam': 'rdfaGroupId',
                    'id': 'RDFAGID'},
                'ISCSITarget': {
                    'keys': conn.performance.get_iscsi_target_keys,
                    'idparam': 'iscsiTargetId',
                    'id': 'ISCSITID'},
                'Board': {
                    'keys': conn.performance.get_board_keys,
                    'idparam': 'boardId',
                    'id': 'BOARDID'}
                }

    try:
        items = func_map[category]['keys'](array_id=arrayid)
        logger.debug(items)
    except PyU4V.utils.exception.ResourceNotFoundException:
        logger.info(f"No {category} items found")
        return list()

    for item in items:
        item_key = f"{{#{func_map[category]['id']}}}"
        result.append({'{#ARRAYID}': arrayid,
                      item_key: item[func_map[category]['idparam']]})

    logger.debug(result)
    logger.info(f"Completed discovery for {category}")
    return result


def main():

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

    parser.add_argument('--hours', action='store', type=int, choices=range(25),
                        help="Preload hours of data into Zabbix (Up to 24)")

    dgroup = parser.add_mutually_exclusive_group()

    dgroup.add_argument('--FEPort', action='store_true',
                        help="Perform Frontend Port discovery")

    dgroup.add_argument('--BEPort', action='store_true',
                        help="Perform Backend Port discovery")

    dgroup.add_argument('--RDFPort', action='store_true',
                        help="Perform RDF Port discovery")

    dgroup.add_argument('--FEDirector', action='store_true',
                        help="Perform Frontend Director discovery")

    dgroup.add_argument('--BEDirector', action='store_true',
                        help="Perform Backend Director discovery")

    dgroup.add_argument('--RDFDirector', action='store_true',
                        help="Perform RDF Director discovery")

    dgroup.add_argument('--EDSDirector', action='store_true',
                        help="Perform EDS Director discovery")

    dgroup.add_argument('--IMDirector', action='store_true',
                        help="Perform IM Director discovery")

    dgroup.add_argument('--srp', action='store_true',
                        help="Perform SRP discovery")

    dgroup.add_argument('--board', action='store_true',
                        help="Perform Board discovery")

    dgroup.add_argument('--diskgroup', action='store_true',
                        help="Perform Disk Group discovery")

    dgroup.add_argument('--storagegroup', action='store_true',
                        help="Perform Storage Group discovery")

    dgroup.add_argument('--portgroup', action='store_true',
                        help="Perform Port Group discovery")

    dgroup.add_argument('--host', action='store_true',
                        help="Perform Host discovery")

    dgroup.add_argument('--initiator', action='store_true',
                        help="Perform Initiator discovery")

    dgroup.add_argument('--emulation', action='store_true',
                        help="Perform Emulation discovery")

    dgroup.add_argument('--iscsi', action='store_true',
                        help="Perform iSCSI Target discovery")

    dgroup.add_argument('--rdf', action='store_true',
                        help="Perform RDF discovery")

    args = parser.parse_args()

    logger.info("Arguments parsed: %s" % str(args))

    # Quick and dirty check for PyU4V.conf file existing
    try:
        f = open(args.configpath)
    except IOError:
        logger.error("Unable to access PyU4V.conf file, check path")
        sys.exit()
    finally:
        f.close()

    result = None
    if args.discovery:
        result = list()
        if args.FEDirector:
            logger.info("Executing FEDirector Discovery")
            result = do_director_discovery(args.configpath,
                                           args.array,
                                           category="FEDirector",
                                           ports=None)
        elif args.FEPort:
            logger.info("Executing FEPort Discovery")
            result = do_director_discovery(args.configpath,
                                           args.array,
                                           category="FEDirector",
                                           ports=True)
        elif args.BEDirector:
            logger.info("Executing BEDirector Discovery")
            result = do_director_discovery(args.configpath,
                                           args.array,
                                           category="BEDirector",
                                           ports=None)
        elif args.BEPort:
            logger.info("Executing BEPort Discovery")
            result = do_director_discovery(args.configpath,
                                           args.array,
                                           category="BEDirector",
                                           ports=True)
        elif args.RDFDirector:
            logger.info("Executing RDFDirector Discovery")
            result = do_director_discovery(args.configpath,
                                           args.array,
                                           category="RDFDirector",
                                           ports=None)
        elif args.RDFPort:
            logger.info("Executing RDFPort Discovery")
            result = do_director_discovery(args.configpath,
                                           args.array,
                                           category="RDFDirector",
                                           ports=True)
        elif args.EDSDirector:
            logger.info("Executing EDSDirector Discovery")
            result = do_director_discovery(args.configpath,
                                           args.array,
                                           category="EDSDirector",
                                           ports=None)
        elif args.IMDirector:
            logger.info("Executing IMDirector Discovery")
            result = do_director_discovery(args.configpath,
                                           args.array,
                                           category="IMDirector",
                                           ports=None)
        elif args.iscsi:
            logger.info("Executing iSCSI Target Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="ISCSITarget")
        elif args.srp:
            logger.info("Executing SRP Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="SRP")

        elif args.rdf:
            logger.info("Executing RDF Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="RDFS")
            result += do_item_discovery(args.configpath, args.array,
                                        category="RDFA")

        elif args.diskgroup:
            logger.info("Executing Disk Group Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="DiskGroup")

        elif args.storagegroup:
            logger.info("Executing StorageGroup Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="StorageGroup")

        elif args.portgroup:
            logger.info("Executing Port Group Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="PortGroup")

        elif args.board:
            logger.info("Executing Board Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="Board")

        elif args.initiator:
            logger.info("Executing Initiator Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="Initiator")

        elif args.host:
            logger.info("Executing Host Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="Host")

        elif args.emulation:
            logger.info("Executing Emulation Discovery")
            result = list()
            for emulation in ['BeEmulation', 'FeEmulation', 'EDSEmulation',
                              'IMEmulation', 'RDFEmulation']:
                result += do_item_discovery(args.configpath, args.array,
                                            category=emulation)

        else:
            logger.info("Executing Array Discovery")
            result = do_item_discovery(args.configpath, args.array,
                                       category="Array")

        # Dump our results to STDOUT
        print(zabbix_safe_output(result))

    else:
        if args.array:
            logger.info("Executing Stat collection")
            if args.hours:
                logger.info(f"Precollecting {args.hours} worth of statistics")

            result = gather_array_health(args.configpath, args.array)

            # Get data for ALL director types
            for dir_cat in ['BEDirector', 'FEDirector', 'RDFDirector',
                            'EDSDirector', 'IMDirector']:
                result = gather_dir_perf(args.configpath,
                                         args.array,
                                         category=dir_cat,
                                         hours=args.hours)

            # Get data for ALL other objects
            data_items = ['SRP', 'PortGroup', 'StorageGroup', 'Array',
                          'Board', 'DiskGroup', 'BeEmulation',
                          'FeEmulation', 'EDSEmulation', 'IMEmulation',
                          'RDFEmulation', 'Host', 'Initiator', 'RDFS',
                          'RDFA', 'ISCSITarget']

            for perf_cat in data_items:
                result = gather_perf(args.configpath, args.array,
                                     category=perf_cat, hours=args.hours)

    logger.info("Complete")


if __name__ == '__main__':
    main()
