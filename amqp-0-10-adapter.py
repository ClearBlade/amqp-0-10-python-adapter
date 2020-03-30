import logging, time, string, json
from clearblade_adapter_library import AdapterLibrary
import qpid.messaging

ADAPTER_NAME = "amqp-0-10-adapter"
ADAPTER_CONFIG = None
DEFAULT_ADAPTER_SETTINGS = {
    "broker_address": "localhost:5672",
    "anon_user": True,
    "queue": "",
    "force_hex_content": False
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
    amqp_message = qpid.messaging.Message()
    parsed_message = json.loads(message.payload)
    parsed_message = byteify(parsed_message)
    if ADAPTER_CONFIG["adapter_settings"]["force_hex_content"]:
        amqp_message.content = parsed_message["content"].decode("hex")
    else:
        amqp_message.content = parsed_message["content"]
    if "content_typee" in parsed_message:
        amqp_message.content_type = parsed_message["content_type"]
    if "correlation_id" in parsed_message:
        amqp_message.correlation_id = parsed_message["correlation_id"]
    if "durable" in parsed_message:
        amqp_message.durable = parsed_message["durable"]
    if "id" in parsed_message:
        amqp_message.id = parsed_message["id"]
    if "priority" in parsed_message:
        amqp_message.priority = parsed_message["priority"]
    if "properties" in parsed_message:
        amqp_message.properties = parsed_message["properties"]
    if "reply_to" in parsed_message:
        amqp_message.reply_to = parsed_message["reply_to"]
    if "subject" in parsed_message:
        amqp_message.subject = parsed_message["subject"]
    if "ttl" in parsed_message:
        amqp_message.ttl = parsed_message["ttl"]
    if "user_id" in parsed_message:
        amqp_message.user_id = parsed_message["user_id"]
    sender.send(amqp_message)

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

def byteify(input):
    logging.info("in byteify")
    # helper function for python 2.7 to convert unicode to strings in a dict created with json.loads 
    # https://stackoverflow.com/a/13105359 
    if isinstance(input, dict):
        return {byteify(key): byteify(value)
                for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

if __name__ == '__main__':
    adapter_lib = AdapterLibrary(ADAPTER_NAME)
    adapter_lib.parse_arguments()
    ADAPTER_CONFIG = adapter_lib.initialize_clearblade()

    # validate adapter settings within adapter config, can set defaults if possible/needed here
    if ADAPTER_CONFIG["adapter_settings"] == "":
        ADAPTER_CONFIG["adapter_settings"] = DEFAULT_ADAPTER_SETTINGS
        logging.info("Using default adapter settings")
    elif "force_hex_content" not in ADAPTER_CONFIG["adapter_settings"]:
        ADAPTER_CONFIG["adapter_settings"]["force_hex_content"] = DEFAULT_ADAPTER_SETTINGS["force_hex_content"]

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
                full_message = {}
                if ADAPTER_CONFIG["adapter_settings"]["force_hex_content"]:
                    full_message["content"] = message.content.encode("utf-8").encode("hex")
                else:
                    full_message["content"] = str(message.content)
                # only send optional message properties that are set, if key is missing then we assume null/None
                if message.content_type != None:
                    full_message["content_type"] = message.content_type
                if message.durable != None:
                    full_message["durable"] = message.durable
                if message.durable != None:
                    full_message["id"] = message.id
                if message.priority != None:
                    full_message["priority"] = message.priority
                if message.properties != None and bool(message.properties):
                    full_message["properties"] = message.properties
                if message.reply_to != None:
                    full_message["reply_to"] = message.reply_to
                if message.subject != None:
                    full_message["subject"] = message.subject
                if message.ttl != None:
                    full_message["ttl"] = message.ttl
                if message.user_id != None:
                    full_message["user_id"] = message.user_id
                adapter_lib.publish(ADAPTER_CONFIG["topic_root"]+ "/incoming/"+ADAPTER_CONFIG["adapter_settings"]["queue"], json.dumps(full_message))
            pass
        except KeyboardInterrupt:
            EXIT_APP = True
            adapter_lib.disconnect_MQTT()
            amqp_connection.close()
            exit(0)
        except Exception as e:
            logging.fatal("Unexpected Exception:")
            logging.fatal(e)
            exit(-1)
