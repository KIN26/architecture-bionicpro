import os
import requests
import json
import httpx

from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from clickhouse_driver import Client
from datetime import date
from typing import List
from jose import jwt

app = FastAPI(title="BionicPRO Reports API")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


security = HTTPBearer(auto_error=True)

# Конфигурация
CLICKHOUSE_CONFIG = {
    'host': os.environ.get('CLICKHOUSE_HOST'),
    'port': 9000,
    'user': os.environ.get('CLICKHOUSE_USER'),
    'password': os.environ.get('CLICKHOUSE_PASSWORD'),
    'database': os.environ.get('CLICKHOUSE_DATABASE')
}

KEYCLOAK_URL = f"http://{os.environ.get('KEYCLOAK_HOST')}"
KEYCLOAK_REALM = os.environ.get('KEYCLOAK_REALM')


class DailyReport(BaseModel):
    report_date: date
    total_events: int
    total_steps: int
    avg_battery_level: float
    min_battery_level: float
    max_battery_level: float
    battery_usage: float
    avg_load: float
    max_load: float


class UserReport(BaseModel):
    customer_id: int
    full_name: str
    device_id: str
    reports: List[DailyReport]


async def get_jwks() -> dict:
    jwks_url = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
    async with httpx.AsyncClient() as client:
        response = await client.get(jwks_url)
        response.raise_for_status()
        jwks = response.json()
    return {k['kid']: k for k in jwks.get('keys', [])}


async def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    try:
        token = credentials.credentials
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get('kid')
        alg = unverified_header.get('alg', "RS256")
        jwks = await get_jwks()

        if kid not in jwks:
            raise HTTPException(status_code=401, detail="Unauthorized")

        decoded = jwt.decode(
            token,
            key=jwks[kid],
            algorithms=[alg],
            issuer=f"http://localhost:8080/realms/{KEYCLOAK_REALM}",
            audience=None,
            options={"verify_aud": False}  # Временно отключаем aud проверку
        )
        return {'customer_id': decoded.get('customer_id')}
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Token verification failed: {e}") from e


def get_clickhouse_client():
    """Получение клиента ClickHouse"""
    return Client(**CLICKHOUSE_CONFIG)


@app.get("/reports", response_model=UserReport)
async def get_user_report(
    current_user: dict = Depends(verify_token),
    ch_client: Client = Depends(get_clickhouse_client),
):
    customer_id = int(current_user['customer_id'])
    query = """
    SELECT report_date, \
           total_events, \
           total_steps, \
           avg_battery, \
           min_battery, \
           max_battery, \
           battery_usage, \
           avg_load, \
           max_load
    FROM prosthesis_reports_mart
    WHERE customer_id = %(customer_id)s
    ORDER BY report_date DESC \
    """

    try:
        results = ch_client.execute(query, {'customer_id': customer_id})

        if not results:
            raise HTTPException(status_code=404, detail="No data available")

        customer_query = """
            SELECT full_name, device_id
            FROM prosthesis_reports_mart
            WHERE customer_id = %(customer_id)s LIMIT 1 \
        """
        customer_info = ch_client.execute(customer_query, {'customer_id': customer_id})

        reports = []
        for row in results:
            reports.append(DailyReport(
                report_date=row[0],
                total_events=row[1],
                total_steps=row[2],
                avg_battery_level=row[3],
                min_battery_level=row[4],
                max_battery_level=row[5],
                battery_usage=row[6],
                avg_load=row[7],
                max_load=row[8]
            ))

        full_name = customer_info[0][0] if customer_info else current_user['full_name']
        device_id = customer_info[0][1] if customer_info else "Unknown"

        return UserReport(
            customer_id=customer_id,
            full_name=full_name,
            device_id=device_id,
            reports=reports,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail="Error generating report") from e


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)