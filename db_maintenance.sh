#!/bin/bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate
if [ ! -f /tmp/lock_populate_xenia_maintenance ]; then
  touch /tmp/lock_populate_xenia_maintenance

    cd /home/xeniaprod/scripts/XeniaDBMaintenance
    python XeniaMaintenance.py --ConfigFile=/home/xeniaprod/config/xenia_database_connect_newdb.ini --LoggingConfig=/home/xeniaprod/scripts/XeniaDBMaintenance/db_maintenance_logging.conf

  rm -f /tmp/lock_populate_xenia_maintenance
fi