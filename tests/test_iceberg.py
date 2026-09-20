"""Unit tests for Iceberg catalog naming (no Spark)."""

import pytest

from payments_lake.iceberg import catalog_table, iceberg_catalog_type


def test_catalog_table_default_catalog():
    assert (
        catalog_table("rtmsp_dev_payments", "payment_events")
        == "glue_catalog.rtmsp_dev_payments.payment_events"
    )


def test_catalog_table_custom_catalog():
    assert (
        catalog_table("rtmsp_dev_payments", "fact_payment", catalog="glue_catalog")
        == "glue_catalog.rtmsp_dev_payments.fact_payment"
    )


def test_iceberg_catalog_type_defaults_to_glue(monkeypatch):
    monkeypatch.delenv("ICEBERG_CATALOG_TYPE", raising=False)
    assert iceberg_catalog_type() == "glue"


def test_iceberg_catalog_type_hadoop(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "hadoop")
    assert iceberg_catalog_type() == "hadoop"


def test_iceberg_catalog_type_rejects_unknown(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "hive")
    with pytest.raises(ValueError, match="must be 'glue' or 'hadoop'"):
        iceberg_catalog_type()
