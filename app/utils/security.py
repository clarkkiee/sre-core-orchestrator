import bcrypt


def hash_password(password: str) -> str:
    hashed: bytes = bcrypt.hashpw(password.encode(), bcrypt.gensalt())
    return hashed.decode()


def verify_password(password: str, hashed_password: str) -> bool:
    result: bool = bcrypt.checkpw(password.encode(), hashed_password.encode())
    return result
