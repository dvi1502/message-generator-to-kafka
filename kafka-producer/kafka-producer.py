import logging
import uuid
from time import sleep
from json import dumps
from kafka import KafkaProducer
from config_reader import config
from datetime import datetime
from time import sleep

from call import PhoneCallBuilder
from call import PhoneCall
from call import KeyBuilder


logging.basicConfig(
    level=logging.INFO,
    filename="/tmp/{0}.log".format(__name__),
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s"
)
# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)



def get_random_time(duration: int,wait_time: int):
    """
    тут возвращается генератор с последовательностью случайных задержек
    :param args: dict input arguments
    :return: generator
    """
    from random import randrange
    i = int(wait_time + wait_time / 2)
    # return randrange(i);
    return (randrange(i) for _ in range(duration * 10000))

def stop_time(current_time_ms , duration: int):
    """
    :param args: dict input arguments
    :return: значение последнего временного отсчета в текущем цикле
    """
    return current_time_ms + duration * 1000


def start(kafka_producer,  const) :

    log = logging.getLogger(__name__)
#    start_time_ms = datetime.now().microsecond
    current_time_ms = datetime.now().microsecond
    stop_time_ms  = stop_time(current_time_ms, int(const["duration"]))
    session_id = uuid.uuid4().hex[:8]
    log.warning("Range: {0} ms --> {1} ms, Total: {2} min, SessionId: {3}".format(
        current_time_ms,
        stop_time_ms,
        (stop_time_ms - current_time_ms) / 1000  / 60 ,
        session_id)
    )

    while ( current_time_ms < stop_time_ms ):
        ktopic = "input-calls"
        kkey = bytes(f"{session_id}-{current_time_ms}","utf-8")
        kvalue = next(PhoneCallBuilder(const))
        kafka_producer.send(topic=ktopic, key=kkey, value=kvalue.json())
        delay = next(get_random_time(int(const["duration"]),int(const["waittime"])))
        log.warning(f"{{topic: '{ktopic}', key: {kkey}, delay: {delay:3}, current_time_ms: {current_time_ms:7} stop_time_ms: {stop_time_ms:7}, countdown: {(stop_time_ms - current_time_ms) / 1000} sec }}")
        current_time_ms += (delay * 1000) + 3 # случайная задержка и 3 милисекунды в любом случае, даже если задержка равна 0
        sleep(delay)




if __name__ == '__main__':

    log = logging.getLogger(__name__)
    log.info("Application start ...")

    cluster = config(section="kafka")

    kafka_producer = None
    kafka_producer = KafkaProducer(
        bootstrap_servers=cluster.values(),
        value_serializer=lambda x: dumps(x).encode('utf-8'),
        security_protocol='PLAINTEXT'
    )

    values = config(section="common")
    start(kafka_producer, values)
    kafka_producer.flush(30)

    log.info("Application terminate ...")


