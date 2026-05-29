import pytest


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("TABLE_NAME", "test-table")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


class _FakeTable:
    def __init__(self):
        self.items = {}

    def _key(self, key):
        return (key["PK"], key["SK"])

    def get_item(self, Key):
        k = self._key(Key)
        if k in self.items:
            return {"Item": self.items[k]}
        return {}

    def put_item(self, Item):
        k = (Item["PK"], Item["SK"])
        self.items[k] = Item
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def update_item(self, Key, UpdateExpression=None, ExpressionAttributeValues=None, **kwargs):
        item = self.get_item(Key).get("Item", {"PK": Key["PK"], "SK": Key["SK"]})
        if "METRICS_FAILURE_KEYS" in (UpdateExpression or ""):
            item["METRICS_FAILURE_KEYS"] = ExpressionAttributeValues[":fkeys"]
            item["METRICS_FAILED_RULES"] = ExpressionAttributeValues[":rules"]
            item["METRICS_STATUS"] = ExpressionAttributeValues[":status"]
            item["METRICS_DATE"] = ExpressionAttributeValues[":date"]
            item["METRICS_PROCESSING_TIME"] = ExpressionAttributeValues[":proc_time"]
            item["METRICS_IS_PRENOTA"] = ExpressionAttributeValues[":prenota"]
        self.put_item(item)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


def test_build_failure_keys_uses_outros():
    from update_metrics.handler import _build_failure_keys

    keys = _build_failure_keys("NF27", "07467822000126", [], "FAILED")
    assert keys == ["NF27|07467822000126|Outros"]


def test_daily_metrics_dedup_same_nf_cnpj_rule(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    key = "NF27|07467822000126|validar_produtos"
    h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        10,
        "VALIDATION_FAILED",
        11,
        "USOCONSUMO",
        ["validar_produtos"],
        [key],
        "p1",
        False,
    )
    h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        12,
        "VALIDATION_FAILED",
        11,
        "USOCONSUMO",
        ["validar_produtos"],
        [key],
        "p2",
        False,
    )

    item = fake.get_item(Key={"PK": "METRICS#2026-05-28", "SK": "SUMMARY"})["Item"]
    assert int(item["failed_count"]) == 1
    assert int(item["failed_rules"]["validar_produtos"]) == 1
    assert len(item["failure_dedup_registry"]) == 1


def test_daily_metrics_dedup_distinct_rules_count_separately(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    keys = [
        "NF27|07467822000126|validar_produtos",
        "NF27|07467822000126|validar_cnpj_fornecedor",
    ]
    h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        10,
        "VALIDATION_FAILED",
        11,
        "USOCONSUMO",
        ["validar_produtos", "validar_cnpj_fornecedor"],
        keys,
        "p1",
        False,
    )

    item = fake.get_item(Key={"PK": "METRICS#2026-05-28", "SK": "SUMMARY"})["Item"]
    assert int(item["failed_count"]) == 2
    assert int(item["failed_rules"]["validar_produtos"]) == 1
    assert int(item["failed_rules"]["validar_cnpj_fornecedor"]) == 1


def test_decrement_daily_metrics_removes_failure_keys(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    key = "NF27|07467822000126|Outros"
    h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        10,
        "LAMBDA_ERROR",
        11,
        "USOCONSUMO",
        [],
        [key],
        "p1",
        False,
    )
    h.decrement_daily_metrics(
        "2026-05-28",
        "FAILED",
        "USOCONSUMO",
        [],
        [key],
        10,
        False,
    )
    item = fake.get_item(Key={"PK": "METRICS#2026-05-28", "SK": "SUMMARY"})["Item"]
    assert int(item["failed_count"]) == 0
    assert int(item["failed_rules"].get("Outros", 0)) == 0
