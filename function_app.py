import azure.functions as func
import logging
import json
import os
import requests
import pyodbc
from datetime import datetime, timedelta

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


# =========================================================
# CONFIG
# =========================================================

def get_google_ads_config():
    config = {
        "developer_token": os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": os.environ.get("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": os.environ.get("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": os.environ.get("GOOGLE_ADS_REFRESH_TOKEN"),
    }

    missing = [k for k, v in config.items() if not v]
    if missing:
        raise ValueError(f"Missing required Google Ads config values: {missing}")

    return config


def get_sql_config():
    config = {
        "server": os.environ.get("SQL_SERVER"),
        "database": os.environ.get("SQL_DATABASE"),
        "username": os.environ.get("SQL_USERNAME"),
        "password": os.environ.get("SQL_PASSWORD"),
    }

    missing = [k for k, v in config.items() if not v]
    if missing:
        raise ValueError(f"Missing required SQL config values: {missing}")

    return config


def get_google_ads_customer_ids() -> list[str]:
    raw_value = os.environ.get("GOOGLE_ADS_CUSTOMER_IDS", "")

    customer_ids = [
        customer_id.strip()
        for customer_id in raw_value.split(",")
        if customer_id.strip()
    ]

    if not customer_ids:
        raise ValueError("Missing required config value: GOOGLE_ADS_CUSTOMER_IDS")

    return customer_ids


def build_sql_connection_string(sql_config: dict) -> str:
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )


# =========================================================
# GOOGLE AUTH
# =========================================================

def get_google_access_token(config: dict) -> str:
    response = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "refresh_token": config["refresh_token"],
            "grant_type": "refresh_token",
        },
        timeout=30,
    )

    response.raise_for_status()

    payload = response.json()
    access_token = payload.get("access_token")

    if not access_token:
        raise ValueError("OAuth token response did not include access_token.")

    return access_token


def build_google_ads_headers(config: dict, access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "developer-token": config["developer_token"],
        "Content-Type": "application/json",
    }


# =========================================================
# GOOGLE ADS EXTRACTS
# =========================================================

def extract_google_ads_accounts(
    config: dict,
    access_token: str,
    customer_id: str
) -> list[dict]:
    url = f"https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:search"

    query = """
        SELECT
          customer.id,
          customer.descriptive_name,
          customer.currency_code,
          customer.time_zone
        FROM customer
    """

    response = requests.post(
        url,
        headers=build_google_ads_headers(config, access_token),
        json={"query": query},
        timeout=60,
    )

    if not response.ok:
        raise ValueError(f"Google Ads account query failed: {response.text}")

    payload = response.json()
    results = payload.get("results", [])
    load_datetime = datetime.utcnow().replace(microsecond=0)

    rows = []
    for r in results:
        customer = r.get("customer", {})

        rows.append(
            {
                "LoadDateTime": load_datetime,
                "CustomerId": int(customer["id"]) if customer.get("id") else None,
                "AccountName": customer.get("descriptiveName"),
                "CurrencyCode": customer.get("currencyCode"),
                "TimeZone": customer.get("timeZone"),
            }
        )

    return rows


def extract_google_ads_campaigns(
    config: dict,
    access_token: str,
    customer_id: str
) -> list[dict]:
    url = f"https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:search"

    query = """
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.primary_status,
          campaign.advertising_channel_type,
          campaign.start_date_time,
          campaign.end_date_time,
          campaign.campaign_budget
        FROM campaign
        ORDER BY campaign.id
    """

    response = requests.post(
        url,
        headers=build_google_ads_headers(config, access_token),
        json={"query": query},
        timeout=60,
    )

    if not response.ok:
        raise ValueError(f"Google Ads campaign snapshot query failed: {response.text}")

    payload = response.json()
    results = payload.get("results", [])
    load_datetime = datetime.utcnow().replace(microsecond=0)

    rows = []
    for r in results:
        campaign = r.get("campaign", {})

        start_date = None
        if campaign.get("startDateTime"):
            start_date = datetime.fromisoformat(
                campaign["startDateTime"].replace("Z", "+00:00")
            ).date()

        end_date = None
        if campaign.get("endDateTime"):
            end_date = datetime.fromisoformat(
                campaign["endDateTime"].replace("Z", "+00:00")
            ).date()

        rows.append(
            {
                "LoadDateTime": load_datetime,
                "CustomerId": int(customer_id),
                "CampaignId": int(campaign["id"]) if campaign.get("id") else None,
                "CampaignName": campaign.get("name"),
                "CampaignStatus": campaign.get("status"),
                "AdvertisingChannelType": campaign.get("advertisingChannelType"),
                "ServingStatus": campaign.get("primaryStatus"),
                "StartDate": start_date,
                "EndDate": end_date,
                "BudgetResourceName": campaign.get("campaignBudget"),
            }
        )

    return rows


def extract_google_ads_campaign_daily(
    config: dict,
    access_token: str,
    customer_id: str,
    start_date: str,
    end_date: str
) -> list[dict]:
    url = f"https://googleads.googleapis.com/v23/customers/{customer_id}/googleAds:search"

    query = f"""
        SELECT
          campaign.id,
          campaign.name,
          campaign.status,
          segments.date,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY segments.date, campaign.id
    """

    response = requests.post(
        url,
        headers=build_google_ads_headers(config, access_token),
        json={"query": query},
        timeout=60,
    )

    if not response.ok:
        raise ValueError(f"Google Ads campaign daily query failed: {response.text}")

    payload = response.json()
    results = payload.get("results", [])
    load_datetime = datetime.utcnow().replace(microsecond=0)

    rows = []
    for r in results:
        campaign = r.get("campaign", {})
        metrics = r.get("metrics", {})
        segments = r.get("segments", {})

        report_date = None
        if segments.get("date"):
            report_date = datetime.strptime(segments["date"], "%Y-%m-%d").date()

        rows.append(
            {
                "LoadDateTime": load_datetime,
                "CustomerId": int(customer_id),
                "CampaignId": int(campaign["id"]) if campaign.get("id") else None,
                "ReportDate": report_date,
                "Impressions": int(metrics["impressions"]) if metrics.get("impressions") is not None else None,
                "Clicks": int(metrics["clicks"]) if metrics.get("clicks") is not None else None,
                "CostMicros": int(metrics["costMicros"]) if metrics.get("costMicros") is not None else None,
                "Conversions": float(metrics["conversions"]) if metrics.get("conversions") is not None else None,
                "ConversionValue": float(metrics["conversionsValue"]) if metrics.get("conversionsValue") is not None else None,
            }
        )

    return rows


# =========================================================
# SQL LOADS
# =========================================================

def merge_google_ads_accounts(rows: list[dict], sql_config: dict):
    conn = pyodbc.connect(build_sql_connection_string(sql_config))
    cursor = conn.cursor()

    try:
        merge_sql = """
        MERGE stg.GoogleAdsAccount AS tgt
        USING (VALUES (?, ?, ?, ?, ?)) AS src
            (LoadDateTime, CustomerId, AccountName, CurrencyCode, TimeZone)
        ON tgt.CustomerId = src.CustomerId

        WHEN MATCHED THEN
            UPDATE SET
                LoadDateTime = src.LoadDateTime,
                AccountName = src.AccountName,
                CurrencyCode = src.CurrencyCode,
                TimeZone = src.TimeZone

        WHEN NOT MATCHED THEN
            INSERT (LoadDateTime, CustomerId, AccountName, CurrencyCode, TimeZone)
            VALUES (src.LoadDateTime, src.CustomerId, src.AccountName, src.CurrencyCode, src.TimeZone);
        """

        for r in rows:
            cursor.execute(
                merge_sql,
                r["LoadDateTime"],
                r["CustomerId"],
                r["AccountName"],
                r["CurrencyCode"],
                r["TimeZone"],
            )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def replace_google_ads_campaigns(
    rows: list[dict],
    customer_id: str,
    sql_config: dict
):
    conn = pyodbc.connect(build_sql_connection_string(sql_config))
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            DELETE FROM stg.GoogleAdsCampaign
            WHERE CustomerId = ?
            """,
            int(customer_id)
        )

        insert_sql = """
            INSERT INTO stg.GoogleAdsCampaign
            (
                LoadDateTime,
                CustomerId,
                CampaignId,
                CampaignName,
                CampaignStatus,
                AdvertisingChannelType,
                ServingStatus,
                StartDate,
                EndDate,
                BudgetResourceName
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data = [
            (
                r["LoadDateTime"],
                r["CustomerId"],
                r["CampaignId"],
                r["CampaignName"],
                r["CampaignStatus"],
                r["AdvertisingChannelType"],
                r["ServingStatus"],
                r["StartDate"],
                r["EndDate"],
                r["BudgetResourceName"],
            )
            for r in rows
        ]

        cursor.fast_executemany = True
        cursor.executemany(insert_sql, data)
        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def delete_google_ads_campaign_daily_window(
    customer_id: str,
    start_date: str,
    end_date: str,
    sql_config: dict
):
    conn = pyodbc.connect(build_sql_connection_string(sql_config))
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            DELETE
            FROM stg.GoogleAdsCampaignDaily
            WHERE CustomerId = ?
              AND ReportDate BETWEEN ? AND ?
            """,
            int(customer_id),
            start_date,
            end_date,
        )
        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def insert_google_ads_campaign_daily(rows: list[dict], sql_config: dict):
    conn = pyodbc.connect(build_sql_connection_string(sql_config))
    cursor = conn.cursor()

    try:
        insert_sql = """
            INSERT INTO stg.GoogleAdsCampaignDaily
            (
                LoadDateTime,
                CustomerId,
                CampaignId,
                ReportDate,
                Impressions,
                Clicks,
                CostMicros,
                Conversions,
                ConversionValue
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        data = [
            (
                r["LoadDateTime"],
                r["CustomerId"],
                r["CampaignId"],
                r["ReportDate"],
                r["Impressions"],
                r["Clicks"],
                r["CostMicros"],
                r["Conversions"],
                r["ConversionValue"],
            )
            for r in rows
        ]

        cursor.fast_executemany = True
        cursor.executemany(insert_sql, data)
        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


# =========================================================
# SHARED LOAD ORCHESTRATOR
# =========================================================

def run_google_ads_campaign_load(
    customer_id: str,
    start_date,
    end_date,
) -> dict:
    config = get_google_ads_config()
    sql_config = get_sql_config()
    access_token = get_google_access_token(config)

    account_rows = extract_google_ads_accounts(
        config=config,
        access_token=access_token,
        customer_id=customer_id,
    )
    merge_google_ads_accounts(account_rows, sql_config)

    campaign_rows = extract_google_ads_campaigns(
        config=config,
        access_token=access_token,
        customer_id=customer_id,
    )
    replace_google_ads_campaigns(
        rows=campaign_rows,
        customer_id=customer_id,
        sql_config=sql_config,
    )

    daily_rows = extract_google_ads_campaign_daily(
        config=config,
        access_token=access_token,
        customer_id=customer_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )
    delete_google_ads_campaign_daily_window(
        customer_id=customer_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        sql_config=sql_config,
    )
    insert_google_ads_campaign_daily(daily_rows, sql_config)

    return {
        "customer_id": customer_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "account_row_count": len(account_rows),
        "campaign_snapshot_row_count": len(campaign_rows),
        "daily_row_count": len(daily_rows),
    }


# =========================================================
# HTTP FUNCTION
# =========================================================

@app.route(route="GoogleAdsCampaignDaily")
def GoogleAdsCampaignDaily(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("GoogleAdsCampaignDaily function processed a request.")

    customer_id = req.params.get("customer_id")
    start_date = req.params.get("start_date")
    end_date = req.params.get("end_date")

    if not customer_id or not start_date or not end_date:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        else:
            customer_id = customer_id or req_body.get("customer_id")
            start_date = start_date or req_body.get("start_date")
            end_date = end_date or req_body.get("end_date")

    missing = []
    if not customer_id:
        missing.append("customer_id")
    if not start_date:
        missing.append("start_date")
    if not end_date:
        missing.append("end_date")

    if missing:
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "message": "Missing required parameters.",
                    "missing_parameters": missing,
                }
            ),
            status_code=400,
            mimetype="application/json",
        )

    try:
        parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "message": "Dates must be in YYYY-MM-DD format."
                }
            ),
            status_code=400,
            mimetype="application/json",
        )

    if parsed_start_date > parsed_end_date:
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "message": "start_date cannot be greater than end_date."
                }
            ),
            status_code=400,
            mimetype="application/json",
        )

    try:
        load_result = run_google_ads_campaign_load(
            customer_id=customer_id,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "load_result": load_result
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps(
                {
                    "status": "error",
                    "message": str(e)
                }
            ),
            status_code=500,
            mimetype="application/json",
        )


# =========================================================
# TIMER FUNCTION
# NOTE: "0 0 6 * * *" = 6:00 AM UTC, not 6:00 AM Chicago
# =========================================================

@app.function_name(name="GoogleAdsCampaignDailyTimer")
@app.schedule(
    schedule="0 0 6 * * *",
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True
)
def GoogleAdsCampaignDailyTimer(mytimer: func.TimerRequest) -> None:
    logging.info("GoogleAdsCampaignDailyTimer function started.")

    end_date = datetime.utcnow().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=13)

    customer_ids = get_google_ads_customer_ids()
    overall_results = []

    for customer_id in customer_ids:
        try:
            load_result = run_google_ads_campaign_load(
                customer_id=customer_id,
                start_date=start_date,
                end_date=end_date,
            )

            overall_results.append(
                {
                    "customer_id": customer_id,
                    "status": "ok",
                    "load_result": load_result,
                }
            )

            logging.info(
                "Google Ads timer load completed successfully for customer_id %s: %s",
                customer_id,
                json.dumps(load_result)
            )

        except Exception as e:
            overall_results.append(
                {
                    "customer_id": customer_id,
                    "status": "error",
                    "message": str(e),
                }
            )

            logging.exception(
                "Google Ads timer load failed for customer_id %s: %s",
                customer_id,
                str(e)
            )

    logging.info("Google Ads timer load summary: %s", json.dumps(overall_results))

    failed_customers = [r for r in overall_results if r["status"] == "error"]
    if failed_customers:
        raise RuntimeError(f"One or more customer loads failed: {json.dumps(failed_customers)}")