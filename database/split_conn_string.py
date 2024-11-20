def split_conn_string(conn_string: str) -> dict:

    """
    Split a connection string into individual components for use in connecting to the database.
    
    Parameters:
        conn_string (str): The connection string.
    
    Returns:
        tuple: A dictionary containing the following keys: user, password, host, port, and db.
    
    Example:
        >>> conn_string = "postgresql://user:password@host:port/db"
        >>> split_conn_string(conn_string)
        {'user': 'user', 'password': 'password', 'host': 'host', 'port': 'port', 'db': 'db'}
    """

    
    elements = conn_string.split("://")
    conn_type, rest = elements[0], elements[1]

    if "mysql" in conn_type.lower():
        conn_type = "mysql"
    elif "postgresql" in conn_type.lower():
        conn_type = "postgresql"

    user_pass, host_port_db = rest.split("@")
    user, password = user_pass.split(":")
    host_port, db = host_port_db.split("/") if "/" in host_port_db else (host_port_db, None)
    host, port = host_port.split(":")

    return {
        "user":user,
        "password":password,
        "host":host,
        "port":port,
        "db":db
    }
