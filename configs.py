from datetime import datetime, timedelta
import yaml
import logging
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)


def find_today():
    """
    Find today date time
    :return: String, date time in Y-m-d format
    """
    return datetime.now().strftime("%Y-%m-%d")


def find_yesterday():
    """
    Find yesterday date time
    :return: String, date time in Y-m-d format
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def build_params(file):
    """
    Read arguments from ymal file
    :param file: Directory to the ymal file
    :return: Dictionary, all Dag argument
    """
    with open(file, "rt") as f:
        try:
            data = yaml.safe_load(f)
        except Exception as e:
            LOGGER.error(e)
            exit()
    return {
        "dir1_yesterday": data["dir_1"] + find_yesterday(),
        "dir1_today": data["dir_1"] + find_today(),
        "dir2_yesterday": data["dir_2"] + find_yesterday(),
        "dir2_today": data["dir_2"] + find_today()
    }

