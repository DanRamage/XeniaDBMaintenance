import sys

sys.path.append('../commonfiles/python')
import optparse
import ConfigParser
import logging.config
from datetime import datetime, timedelta
import time
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, func
from xeniaSQLiteAlchemy import multi_obs, platform
from sqlalchemy import create_engine
from sqlalchemy import exc
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def main():
    parser = optparse.OptionParser()
    parser.add_option("--ConfigFile", dest="config_file",
                      help="INI Configuration file.")
    parser.add_option("--LoggingConfig", dest="logging_conf",
                      help="Logging Configuration file.")

    (options, args) = parser.parse_args()

    logging.config.fileConfig(options.logging_conf)
    logger = logging.getLogger(__name__)
    logger.info("Log file opened.")

    db_config_file = ConfigParser.RawConfigParser()
    db_config_file.read(options.config_file)
    db_user = db_config_file.get('database', 'user')
    db_pwd = db_config_file.get('database', 'password')
    db_host = db_config_file.get('database', 'host')
    db_name = db_config_file.get('database', 'name')
    db_connectionstring = db_config_file.get('database', 'connectionstring')
    db_obj = xeniaAlchemy()
    if (db_obj.connectDB(db_connectionstring, db_user, db_pwd, db_host, db_name, False) == True):
        logger.info("Succesfully connect to DB: %s at %s" % (db_name, db_host))

        default_prune = datetime.now() - timedelta(weeks=8)

        prune_start_time = time.time()

        # Get list of platforms to prune.
        platform_recs = db_obj.session.query(platform) \
            .order_by(platform.organization_id).all()
        platform_count = 0
        for platform_rec in platform_recs:
            delete_start_time = time.time()
            db_obj.session.query(multi_obs) \
                .filter(multi_obs.m_date < default_prune.strftime('%Y-%m-%dT%H:%M:%S')) \
                .filter(multi_obs.platform_handle == platform_rec.platform_handle)\
                .delete()
            logger.info("Platform: %s pruned records older than: %s in %f seconds" % (
            platform_rec.platform_handle, default_prune, time.time() - delete_start_time))
            platform_count += 1

        logger.info("Pruned %d platforms in %f seconds" % (platform_count, time.time() - prune_start_time))
        db_obj.disconnect()

        try:
            # Now create raw connection to database to do vacuum.
            connectionString = "%s://%s:%s@%s/%s" % (db_connectionstring, db_user, db_pwd, db_host, db_name)
            logger.info("Preparing to vacuum and reindex")
            db_engine = create_engine(connectionString, echo=False)
            connection = db_engine.raw_connection()
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        except (exc.OperationalError, Exception) as e:
            logger.exception(e)
        else:
            cursor = connection.cursor()
            vacuum_start_time = time.time()
            #cursor.execute("VACUUM ANALYSE multi_obs")
            cursor.execute("VACUUM FULL multi_obs")
            logger.info("VACUUMed multi_obs in %f seconds" % (time.time() - vacuum_start_time))
            reindex_start_time = time.time()
            cursor.execute("REINDEX TABLE multi_obs")
            logger.info("Reindexed multi_obs in %f seconds" % (time.time() - reindex_start_time))
            cursor.close()

            connection.close()
            db_engine.dispose()

    else:
        logger.error("Unable to connect to DB: %s at %s. Terminating script." % (db_name, db_host))

    return

if __name__ == "__main__":
    main()
