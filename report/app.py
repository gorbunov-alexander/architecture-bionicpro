import os
from datetime import datetime

from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import clickhouse_connect
import requests
from jose import jwt, JWTError
from jose.utils import base64url_decode
import logging

app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_jwks_cache = None

JWKS_URL = os.getenv("JWKS_URL", "http://localhost:8080/realms/reports-realm/protocol/openid-connect/certs")
ISSUER = "http://localhost:8080/realms/reports-realm"
ROLE = "prothetic_user"
ALGS = ["RS256"]


def _get_jwks():
    global _jwks_cache
    if _jwks_cache is None:
        resp = requests.get(JWKS_URL, timeout=5)
        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Cannot fetch JWKS")
        _jwks_cache = resp.json()["keys"]
    return _jwks_cache


def _get_signing_key(token):
    header = jwt.get_unverified_header(token)
    kid = header.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="No kid in token header")

    jwks = _get_jwks()
    for key in jwks:
        if key.get("kid") == kid:
            return key
    global _jwks_cache
    _jwks_cache = None
    jwks = _get_jwks()
    for key in jwks:
        if key.get("kid") == kid:
            return key

    raise HTTPException(status_code=401, detail="Signing key not found")


def _jwks_key_to_pem(jwk):
    if jwk.get("kty") != "RSA":
        raise HTTPException(status_code=500, detail="Unsupported key type")

    n = base64url_decode(jwk["n"].encode("utf-8"))
    e = base64url_decode(jwk["e"].encode("utf-8"))

    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

    pub_numbers = rsa.RSAPublicNumbers(
        int.from_bytes(e, "big"),
        int.from_bytes(n, "big"),
    )
    pub_key = pub_numbers.public_key(default_backend())
    pem = pub_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return pem


ch_client = clickhouse_connect.get_client(
    host=os.getenv("DB_HOST", "clickhouse-host"),
    username=os.getenv("DB_USER", "default"),
    password=os.getenv("DB_PASSWORD", ""),
    database=os.getenv("DB_NAME", "default"),
)


class Report(BaseModel):
    date: datetime
    user_id: int
    prosthesis_type: str
    muscle_group: str
    signals_count: int
    signal_frequency_avg: float
    signal_duration_avg: float
    signal_amplitude_avg: float
    signal_duration_total: float


def get_current_user_id(request: Request) -> str:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")

    token = auth_header[len("Bearer ") :].strip()
    try:
        jwk = _get_signing_key(token)
        public_key_pem = _jwks_key_to_pem(jwk)

        payload = jwt.decode(
            token,
            public_key_pem,
            algorithms=ALGS,
            issuer=ISSUER,
            audience=None,
        )
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    roles = payload.get("realm_access", {}).get("roles", [])
    if ROLE not in roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Required role {ROLE}")
    user_id = payload.get("system_id")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No user in token")
    return user_id


@app.get("/reports", response_model=List[Report])
def get_reports(
        user_id: str = Depends(get_current_user_id),
):
    query = """
        SELECT
            date,
            user_id,
            prosthesis_type,
            muscle_group,
            signals_count, 
            signal_frequency_avg,
            signal_duration_avg, 
            signal_amplitude_avg,
            signal_duration_total
        FROM prosthesis_reports
        WHERE user_id = %(user_id)s
        ORDER BY (prosthesis_type, signals_count)
        LIMIT 365
    """

    result = ch_client.query(query, parameters={"user_id": user_id})
    cols = result.column_names
    rows = [dict(zip(cols, row)) for row in result.result_rows]

    return [Report(**row) for row in rows]

