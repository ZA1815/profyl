import jwt

def check_auth(token: str, secret_key: str, buffer: bytearray) -> int:
    try:
        payload = jwt.decode(token, secret_key, algorithms=["HS256"])
        # Hardcoded right now, figure out a more robust way to do this
        return int(payload["sub"])
    except jwt.ExpiredSignatureError:
        error = "[profyl] ERROR: JWT token is expired".encode("utf-8")
        buffer.extend(error)
    except jwt.InvalidTokenError:
        error = "[profyl] ERROR: JWT token is invalid".encode("utf-8")
        buffer.extend(error)