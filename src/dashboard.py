from __future__ import annotations

import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st

from common import env


st.set_page_config(page_title="Kafka Retail Analytics", page_icon="⚡", layout="wide")


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=env("POSTGRES_HOST", "localhost"),
        port=env("POSTGRES_PORT", "5432"),
        dbname=env("POSTGRES_DB", "retail"),
        user=env("POSTGRES_USER", "kafka_user"),
        password=env("POSTGRES_PASSWORD", "kafka_password"),
    )


def query_frame(sql: str) -> pd.DataFrame:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [column.name for column in cursor.description]
    return pd.DataFrame(rows, columns=columns)


st.title("⚡ Real-Time Retail Analytics")
st.caption("Apache Kafka → Python stream processing → PostgreSQL • refreshes every 5 seconds")


@st.fragment(run_every="5s")
def live_dashboard() -> None:
    try:
        metrics = query_frame(
            """
            SELECT
                COUNT(*) AS total_orders,
                COALESCE(SUM(net_amount) FILTER (WHERE status <> 'cancelled'), 0) AS revenue,
                COALESCE(AVG(net_amount), 0) AS avg_order_value,
                COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_orders
            FROM orders
            """
        ).iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Events processed", f"{int(metrics['total_orders']):,}")
        col2.metric("Revenue", f"${float(metrics['revenue']):,.2f}")
        col3.metric("Average order", f"${float(metrics['avg_order_value']):,.2f}")
        col4.metric("Cancelled", f"{int(metrics['cancelled_orders']):,}")

        by_category = query_frame(
            """
            SELECT category, COUNT(*) AS orders,
                   SUM(net_amount) FILTER (WHERE status <> 'cancelled') AS revenue
            FROM orders
            GROUP BY category
            ORDER BY revenue DESC NULLS LAST
            """
        )
        by_region = query_frame(
            """
            SELECT region, COUNT(*) AS orders,
                   SUM(net_amount) FILTER (WHERE status <> 'cancelled') AS revenue
            FROM orders
            GROUP BY region
            ORDER BY revenue DESC NULLS LAST
            """
        )

        left, right = st.columns(2)
        with left:
            st.subheader("Revenue by category")
            if by_category.empty:
                st.info("Waiting for the first events...")
            else:
                st.plotly_chart(
                    px.bar(by_category, x="category", y="revenue", color="category"),
                    use_container_width=True,
                )
        with right:
            st.subheader("Orders by region")
            if by_region.empty:
                st.info("Waiting for the first events...")
            else:
                st.plotly_chart(
                    px.pie(by_region, names="region", values="orders", hole=0.45),
                    use_container_width=True,
                )

        recent = query_frame(
            """
            SELECT order_id, customer_id, product_name, category, region,
                   quantity, net_amount, status, event_time
            FROM orders
            ORDER BY event_time DESC
            LIMIT 15
            """
        )
        st.subheader("Latest events")
        st.dataframe(recent, use_container_width=True, hide_index=True)
    except psycopg.OperationalError as error:
        st.warning(f"Waiting for PostgreSQL: {error}")


live_dashboard()

