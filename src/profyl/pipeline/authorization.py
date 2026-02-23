def check_authz(user_id: int, allowed_users: list[int], buffer: bytearray) -> bool:
    if user_id not in allowed_users:
        error = f"[profyl] ERROR: User with ID {user_id} is not allowed to access this project".encode("utf-8")
        buffer.extend(error)
        return False
    return True