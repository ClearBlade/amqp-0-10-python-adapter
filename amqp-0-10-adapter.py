import logging, time, string
from clearblade_adapter_library import AdapterLibrary
import qpid.messaging

ADAPTER_NAME = "amqp-0-10-adapter"
ADAPTER_CONFIG = None
DEFAULT_ADAPTER_SETTINGS = {
    "broker_address": "localhost:5672",
    "anon_user": True,
    "queue": ""
}

amqp_send_session = None
amqp_connection = None

def cb_message_handler(message):
    global amqp_send_session
    if amqp_send_session == None:
        logging.error("Received outgoing message but AMQP not connected yet, ignoring")
        return
    address = string.split(message.topic, "/")[-1] # split message.topic on / and grab last element
    sender = amqp_send_session.sender(address)
    sender.send(qpid.messaging.Message(message.payload))

def connect_AMQP():
    global amqp_send_session, amqp_connection
    amqp_connection = qpid.messaging.Connection(ADAPTER_CONFIG["adapter_settings"]["broker_address"])
    try:
        amqp_connection.open() 
    except qpid.messaging.exceptions.ConnectError:
        logging.error("Failed to connect to AMQP broker, trying again in 30 seconds...")
        time.sleep(30)
        connect_AMQP()
    logging.info("Successfully connected to AMQP broker!")
    amqp_send_session = amqp_connection.session()
    return amqp_connection.session()

if __name__ == '__main__':
    adapter_lib = AdapterLibrary(ADAPTER_NAME)
    adapter_lib.parse_arguments()
    ADAPTER_CONFIG = adapter_lib.initialize_clearblade()

    # validate adapter settings within adapter config, can set defaults if possible/needed here
    if ADAPTER_CONFIG["adapter_settings"] == "":
         ADAPTER_CONFIG["adapter_settings"] = DEFAULT_ADAPTER_SETTINGS
         logging.info("Using default adapter settings")

    logging.debug("Using adapter config: " + str(ADAPTER_CONFIG))

    adapter_lib.connect_MQTT(topic=ADAPTER_CONFIG["topic_root"] + "/outgoing/#", cb_message_handler=cb_message_handler)

    #if a queue is in settings then we want to listen on it, otherwise we are in send only mode
    amqp_receive_session = connect_AMQP()
    receiver = None
    if "queue" in ADAPTER_CONFIG["adapter_settings"]:
        logging.info("A queue was provided via adapter_settings, listening for AMQP messages on " + ADAPTER_CONFIG["adapter_settings"]["queue"])
        receiver = amqp_receive_session.receiver(ADAPTER_CONFIG["adapter_settings"]["queue"])

    while True:
        try:
            if receiver != None:
                message = receiver.fetch()
                amqp_receive_session.acknowledge()
                logging.debug(str(message))
                adapter_lib.publish(ADAPTER_CONFIG["topic_root"]+ "/incoming/"+ADAPTER_CONFIG["adapter_settings"]["queue"], message.content)
            pass
        except KeyboardInterrupt:
            EXIT_APP = True
            adapter_lib.disconnect_MQTT()
            amqp_connection.close()
            exit(0)
        except Exception as e:
            logging.fatal("EXCEPTION:: %s", str(e))
            exit(-1)
