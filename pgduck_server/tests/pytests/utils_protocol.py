import struct


def pack_message(message_type, payload):
    """Pack a message with the given type and payload for PostgreSQL protocol."""
    return message_type.encode() + struct.pack("!I", len(payload) + 4) + payload


def send_message(sock, message_type, payload=b""):
    """Send a message over the socket."""
    message = pack_message(message_type, payload)
    sock.sendall(message)


def send_startup_message(sock):
    """Send startup message to PostgreSQL."""
    payload = struct.pack("!I", 196608)  # protocol version number
    send_message(sock, "", payload)


def send_termination(sock):
    """Send termination message."""
    send_message(sock, "X")


def send_parse_message(sock, query, stmt_name="", param_types=None):
    """Sends a Parse message to a PostgreSQL server."""
    if param_types is None:
        param_types = []

    # Message components
    stmt_name_bytes = stmt_name.encode("ascii") + b"\x00"
    query_bytes = query.encode("ascii") + b"\x00"
    param_type_count = len(param_types).to_bytes(2, byteorder="big")
    param_types_bytes = b"".join(pt.to_bytes(4, byteorder="big") for pt in param_types)

    # Construct the payload
    payload = stmt_name_bytes + query_bytes + param_type_count + param_types_bytes

    # Send the Parse message
    send_message(sock, "P", payload)


def send_bind_message(sock):
    """Sends a Bind message to a PostgreSQL server."""
    # Bind message components
    portal_name = b"\x00"  # Default portal (unnamed)
    prepared_stmt_name = b"\x00"  # Unnamed prepared statement
    parameter_format_code_count = (0).to_bytes(2, byteorder="big")
    parameter_values_count = (0).to_bytes(2, byteorder="big")
    result_column_format_code_count = (0).to_bytes(2, byteorder="big")

    # Construct the payload
    payload = (
        portal_name
        + prepared_stmt_name
        + parameter_format_code_count
        + parameter_values_count
        + result_column_format_code_count
    )

    # Send the Bind message
    send_message(sock, "B", payload)


def send_copy_data_message(sock, data):
    """Sends a CopyData message to a PostgreSQL server."""
    send_message(sock, "d", data)


# send_execute_message.py
def send_execute_message(sock, portal_name=b"", max_rows=0):
    """Sends an Execute message to a PostgreSQL server."""
    portal_name_bytes = portal_name + b"\x00"
    max_rows_bytes = max_rows.to_bytes(4, byteorder="big")
    payload = portal_name_bytes + max_rows_bytes
    send_message(sock, "E", payload)


# send_close_message.py
def send_close_message(sock, close_type="S", name=b""):
    """Sends a Close message to a PostgreSQL server."""
    close_type_byte = close_type.encode("ascii")
    name_bytes = name + b"\x00"
    payload = close_type_byte + name_bytes
    send_message(sock, "C", payload)


# send_describe_message.py
def send_describe_message(sock, describe_type="S", name=b""):
    """Sends a Describe message to a PostgreSQL server."""
    describe_type_byte = describe_type.encode("ascii")
    name_bytes = name + b"\x00"
    payload = describe_type_byte + name_bytes
    send_message(sock, "D", payload)


# send_sync_message.py
def send_sync_message(sock):
    """Sends a Sync message to a PostgreSQL server."""
    payload = b""
    send_message(sock, "S", payload)


def send_query_message(sock, query):
    """
    Sends a query message to the PostgreSQL server.

    :param sock: The socket connected to the server.
    :param query: The SQL query string to be sent.
    """
    # Message type for query
    message_type = b"Q"
    # Message length: 4 bytes for length itself + query length + null terminator
    message_length = 4 + len(query) + 1
    # Construct the message: message type + message length + query + null terminator
    message = (
        message_type
        + message_length.to_bytes(4, byteorder="big")
        + query.encode()
        + b"\x00"
    )
    # Send the message
    sock.sendall(message)
