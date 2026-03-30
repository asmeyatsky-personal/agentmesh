import os
from jose import jwt, JWTError
from datetime import UTC, datetime, timedelta
from typing import Optional, Dict, Any
from loguru import logger

_INSECURE_DEFAULT = "your-secret-key"
SECRET_KEY = os.environ.get("JWT_SECRET_KEY", _INSECURE_DEFAULT)
ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")

if SECRET_KEY == _INSECURE_DEFAULT:
    logger.warning(
        "JWT_SECRET_KEY is not set — using insecure default. "
        "Set the JWT_SECRET_KEY environment variable before deploying to production."
    )


def create_access_token(
    data: dict, tenant_id: str, expires_delta: Optional[timedelta] = None
) -> str:
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(UTC) + expires_delta
    else:
        expire = datetime.now(UTC) + timedelta(minutes=15)
    to_encode.update({"exp": expire, "tenant_id": tenant_id})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_access_token(token: str, credentials_exception) -> Dict[str, Any]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload is None or "tenant_id" not in payload:
            raise credentials_exception
        return payload
    except JWTError as e:
        logger.error(f"JWT Error: {e}")
        raise credentials_exception
