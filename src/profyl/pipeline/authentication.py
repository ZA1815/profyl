import jwt

def check_auth(token: str | None, secret_key: str, buffer: bytearray) -> int:
    if token is None:
        error = "[profyl] ERROR: JWT token not provided".encode("utf-8")
        buffer.extend(error)
        return -1
        
    try:
        payload = jwt.decode(token, secret_key, algorithms=["HS256"])
        # Hardcoded right now, figure out a more robust way to do this
        return int(payload["sub"])
    except jwt.ExpiredSignatureError:
        error = "[profyl] ERROR: JWT token is expired".encode("utf-8")
        buffer.extend(error)
        return -1
    except jwt.InvalidTokenError:
        error = "[profyl] ERROR: JWT token is invalid".encode("utf-8")
        buffer.extend(error)
        return -1