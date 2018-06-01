#!/usr/bin/env python
#
#The MIT License (MIT)
#
# Copyright (c) 2015 Bit9 + Carbon Black
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# -----------------------------------------------------------------------------
#  <Short Description>
#
#  <Long Description>
#
#  last updated 2015-10-14 by Craig Cason ccason@bit9.com
#

from optparse import OptionParser, SUPPRESS_HELP
import sys
import datetime
import ConfigParser
import operator
import logging
from logging.config import fileConfig

import cbapi

sbl = fileConfig('/etc/cb/CbSensorBalancer-logger.conf')
logger = logging.getLogger(sbl)


def build_api_object(server_url, token, ssl_verify):
    #This should be an object that contains all of this information
    cb = cbapi.CbApi(server_url, token=token, ssl_verify=ssl_verify)

    return cb


def get_managed_clusters(confile):

    config = ConfigParser.ConfigParser()
    config.read(confile)

    name = config.get("Balancer", "Name")
    logger.info('Config: %s', name)

    return config


def get_cluster_sensor_stats(opts, managed_clusters):
    cluster_status = {}

    for cluster in managed_clusters.sections():
        if cluster != "Balancer":
            url = managed_clusters.get(cluster, "URL")
            token = managed_clusters.get(cluster, "Token")
            if managed_clusters.get(cluster, "SSLVerify") == "True":
                sslverify = True
            else:
                sslverify = False
            cb = build_api_object(url, token, sslverify)
            try:
                #gcss = cb.sensor_backlog()
                gcss = cb.license_status()
                #cluster_status[cluster] = gcss['active_sensor_count']
                cluster_status[cluster] = gcss['actual_sensor_count']
            except Exception, e:
                msg = 'Cluster %s: %s' % (cluster, str(e))
                logger.exception('%s' % msg)
                if opts.strict:
                    logger.error('Strict Mode - Managed cluster communication issue. Exiting...')
                    sys.exit(-1)
    return cluster_status


def get_sensor_list(opts, managed_clusters):
    sensor_ids = []
    query = {'groupid': managed_clusters.get("Balancer", "BaseSensorGroup")}

    url = managed_clusters.get("Balancer", "URL")
    token = managed_clusters.get("Balancer", "Token")
    if managed_clusters.get("Balancer", "SSLVerify") == "True":
        sslverify = True
    else:
        sslverify = False

    gsl = build_api_object(url, token, sslverify)

    try:
        sensors = gsl.sensors(query)
    except Exception, e:
        msg = 'Balancer query %s: %s' % (query, str(e))
        logger.exception('%s' % msg)
        sys.exit(-1)

    logger.debug('Balancer Base Sensor Group: %s', query)

    for sensor in sensors:
        sensor_ids.append(sensor['id'])

    return sensor_ids


def assign_sensors(opts, sensor_ids, cluster_status, managed_clusters):
    sensor_assignments = []

    if opts.sim:
        #SIMULATION TEST VALUE
        num = 3
        logger.warning('Adding Simulation Cluster; {ClusterSimTest: %s}', num)
        cluster_status['ClusterSimTest'] = int(num)

    for sid in sensor_ids:
        sorted_cs = sorted(cluster_status.items(), key=operator.itemgetter(1), reverse=False)
        logger.debug('Sorted Cluster Stats: %s', sorted_cs)

        for cluster, count in sorted_cs:
            # Assign the sensor id to the lowest cluster count migration sensor group
            assignment = {"sensor_id": sid, "migratesensorgroup": managed_clusters.get(cluster, "MigrateSensorGroup")}
            # Add assignment to list
            sensor_assignments.append(assignment)
            # Artificial increase the cluster count
            cluster_status[cluster] += 1
            # Break after first assignment
            break
    sorted_cs = sorted(cluster_status.items(), key=operator.itemgetter(1), reverse=False)
    logger.debug('Final Sorted Cluster Stats: %s', sorted_cs)
    # Return list of dict[ {sensor id: migration sensor group id} ]
    return sensor_assignments


def move_sensors(opts, sensor_assignment, managed_clusters):
    url = managed_clusters.get("Balancer", "URL")
    token = managed_clusters.get("Balancer", "Token")
    simulate = opts.sim
    if managed_clusters.get("Balancer", "SSLVerify") == "True":
        sslverify = True
    else:
        sslverify = False

    ms = build_api_object(url, token, sslverify)

    for assignment in sensor_assignment:
        if simulate:
            msg = 'Sim Moving Sensor ID: %s\tTo Group ID: %s' % (assignment['sensor_id'], assignment['migratesensorgroup'])
            logger.warning(msg)
            continue
        msg = 'Moving Sensor ID: %s\tTo Group ID: %s' % (assignment['sensor_id'], assignment['migratesensorgroup'])
        logger.debug(msg)
        sid = assignment['sensor_id']
        gid = assignment['migratesensorgroup']
        ms.move_sensor_to_group(sid, gid)

    return 0


def build_cli_parser():
    parser = OptionParser(usage="%prog [options]", description="Automatia")

    # for each supported output type, add an option
    parser.add_option("-c", "--config", action="store", default="/etc/cb/CbSensorBalancer.conf",
                      dest="cbsensorbalancer_configs", help="Runs in strict mode. If any managed cluster is"
                                                            " unaccessible, no balancing will occur.")
    parser.add_option("-s", "--simulate", action="store_true", default=False, dest="sim",
                      help="Runs in simulation mode. Simlutes the balancing of sensors to output only.")
    parser.add_option("-x", "--strict", action="store_true", default=False, dest="strict",
                      help="Runs in strict mode. If any managed cluster is unaccessible no balancing will occur.")

    return parser


def main(argv):
    start = datetime.datetime.now()

    parser = build_cli_parser()
    opts, args = parser.parse_args(argv)

    logger.info('Starting Sensor Migrations')

    # Get CbSensorBalancer.conf cluster configurations
    #
    managed_clusters = get_managed_clusters(opts.cbsensorbalancer_configs)

    # Get list of all sensors needing to be balanced
    #
    sensor_list = get_sensor_list(opts, managed_clusters)
    logger.info("Number of Sensors To Move: %s", len(sensor_list))
    logger.debug("Sensors To Move: %s", sensor_list)

    # Query each managed cluster's API for Sensor License Stats
    #
    cluster_sensor_stats = get_cluster_sensor_stats(opts, managed_clusters)
    logger.info("Cluster Stats: %s", cluster_sensor_stats)

    if opts.sim:
        logger.warning('Entering Simulation Mode')

    # Assign sensors to appropriate sensor group for balancing
    #
    sensor_assignment = assign_sensors(opts, sensor_list, cluster_sensor_stats, managed_clusters)
    logger.info("Sensor Assignments: %s", sensor_assignment)

    # Move sensors to assigned sensor groups for migration
    #
    move_sensors(opts, sensor_assignment, managed_clusters)

    if opts.sim:
        logger.warning('Exiting Simulation Mode')

    end = datetime.datetime.now()
    logger.info('Finished Sensor Migrations -- duration: %d sec', (end - start).seconds)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))