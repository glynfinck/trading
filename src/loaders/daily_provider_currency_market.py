import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from datetime import date
from prefect import flow, task, get_run_logger
from modules.database import utils

@flow()
def daily_provider_currency_market(date: date):
    logger = get_run_logger()

    # 1. Run query to aggregate daily data.
    query = """
    SELECT DISTINCT
        DATE,
        PROVIDER,
        FROM_CURRENCY,
        TO_CURRENCY,
        FIRST_VALUE(OPEN) OVER (
            PARTITION BY
                DATE,
                PROVIDER,
                FROM_CURRENCY,
                TO_CURRENCY
            ORDER BY
                DATE ASC
        ) AS OPEN,
        LAST_VALUE(
            CLOSE
        ) OVER (
            PARTITION BY
                DATE,
                PROVIDER,
                FROM_CURRENCY,
                TO_CURRENCY
            ORDER BY
                DATE ASC
        ) AS "close",
        MAX(HIGH) OVER (
            PARTITION BY
                DATE,
                PROVIDER,
                FROM_CURRENCY,
                TO_CURRENCY
            ORDER BY
                DATE ASC
        ) AS "high",
        MIN(LOW) OVER (
            PARTITION BY
                DATE,
                PROVIDER,
                FROM_CURRENCY,
                TO_CURRENCY
            ORDER BY
                DATE ASC
        ) AS "low",
        SUM(VOLUME) OVER (
            PARTITION BY
                DATE,
                PROVIDER,
                FROM_CURRENCY,
                TO_CURRENCY
            ORDER BY
                DATE ASC
        ) AS "volume",
        SUM(TRADES) OVER (
            PARTITION BY
                DATE,
                PROVIDER,
                FROM_CURRENCY,
                TO_CURRENCY
            ORDER BY
                DATE ASC
        ) AS "trades"
    FROM
        (
            SELECT
                TIMESTAMP::DATE AS DATE,
                *
            FROM
                "ProviderCurrencyMarket"
        )
    WHERE
        DATE = '{date}'""".format(date=date.strftime("%Y-%m-%d"))
    logger.info(f"Running query: {query}")
    toset = utils.query_table(query)
    
    # 2. Add daily aggregates.
    utils.set_data("DailyProviderCurrencyMarket", toset)

if __name__ == "__main__":
    daily_provider_currency_market(date(2024,9,27))