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


def test_nf_cnpj_from_parsed_xml_example():
    from update_metrics.handler import _build_failure_keys, _nf_cnpj_from_parsed_xml

    xml_doc = {
        "numero_nota": "448528",
        "chave_acesso": "35260357600249000155552010004485281516323523",
        "emitente": {"cnpj": "57600249000155"},
    }
    nf, cnpj = _nf_cnpj_from_parsed_xml(xml_doc)
    assert nf == "448528"
    assert cnpj == "57600249000155"
    keys = _build_failure_keys(nf, cnpj, "A97057", ["validar_produtos"], "FAILED")
    assert keys == ["448528|57600249000155|validar_produtos|A97057"]


def test_pedido_numero_from_request_body_example():
    from update_metrics.handler import _pedido_numero_from_request_body

    pedido_doc = {
        "requestBody": {
            "itens": [
                {
                    "pedidoDeCompra": {"pedidoErp": "A97057", "itemPedidoErp": "0001"},
                }
            ],
            "cnpjEmitente": "57600249000155",
        }
    }
    assert _pedido_numero_from_request_body(pedido_doc["requestBody"]) == "A97057"


def test_build_failure_keys_uses_outros_and_pedido():
    from update_metrics.handler import _build_failure_keys

    keys = _build_failure_keys("NF27", "07467822000126", "AACBKV", [], "FAILED")
    assert keys == ["NF27|07467822000126|Outros|AACBKV"]


def test_failure_keys_after_protheus_schema_rules_not_outros():
    """Regressão: failure_keys deve usar regra Protheus/API, não Outros."""
    import json

    from utils.protheus_regras import build_failed_rules_for_metrics, load_api_regras_catalog

    from update_metrics.handler import _build_failure_keys

    meta = {
        "STATUS": "FAILED",
        "protheus_request_info": json.dumps(
            {
                "response_status_code": 400,
                "response_body": {
                    "errorCode": "SCHEMA_ITEM_002",
                    "message": "O campo 'codigoOperacao' é obrigatório no item 1.",
                },
            }
        ),
    }
    ocr_rules, _, skip, _ = build_failed_rules_for_metrics(
        None,
        meta,
        validation_failed_rules=[],
        api_catalog=load_api_regras_catalog(),
    )
    assert ocr_rules == ["SCHEMA_ITEM_002"]
    keys = _build_failure_keys(
        "04475055", "53113791000122", "AACHUY", ocr_rules, "FAILED"
    )
    assert keys == ["04475055|53113791000122|SCHEMA_ITEM_002|AACHUY"]
    assert "Outros" not in keys[0]


def test_daily_metrics_dedup_same_nf_cnpj_rule_pedido(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    key = "NF27|07467822000126|validar_produtos|PED001"
    h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        10,
        "VALIDATION_FAILED",
        11,
        "AGROQUIMICOS",
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
        "AGROQUIMICOS",
        ["validar_produtos"],
        [key],
        "p2",
        False,
    )

    item = fake.get_item(Key={"PK": "METRICS#2026-05-28", "SK": "SUMMARY"})["Item"]
    assert int(item["failed_count"]) == 1
    assert int(item["failed_rules"]["validar_produtos"]) == 1
    assert len(item["failure_dedup_registry"]) == 1


def test_daily_metrics_dedup_distinct_pedido_counts_separately(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    keys = [
        "NF27|07467822000126|validar_produtos|PED001",
        "NF27|07467822000126|validar_produtos|PED002",
    ]
    h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        10,
        "VALIDATION_FAILED",
        11,
        "USOCONSUMO",
        ["validar_produtos"],
        [keys[0]],
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
        [keys[1]],
        "p2",
        False,
    )

    item = fake.get_item(Key={"PK": "METRICS#2026-05-28", "SK": "SUMMARY"})["Item"]
    assert int(item["failed_count"]) == 2
    assert int(item["failed_rules"]["validar_produtos"]) == 2


def test_daily_metrics_dedup_distinct_rules_count_separately(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    keys = [
        "NF27|07467822000126|validar_produtos|PED001",
        "NF27|07467822000126|validar_cnpj_fornecedor|PED001",
    ]
    h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        10,
        "VALIDATION_FAILED",
        11,
        "AGROQUIMICOS",
        ["validar_produtos", "validar_cnpj_fornecedor"],
        keys,
        "p1",
        False,
    )

    item = fake.get_item(Key={"PK": "METRICS#2026-05-28", "SK": "SUMMARY"})["Item"]
    assert int(item["failed_count"]) == 2
    assert int(item["failed_rules"]["validar_produtos"]) == 1
    assert int(item["failed_rules"]["validar_cnpj_fornecedor"]) == 1


def test_missing_identity_uses_nao_identificado_all_types():
    from utils.failure_dedup import failure_identity_fallback as fb

    assert fb("pedido") == "NAO_IDENTIFICADO_PEDIDO"
    assert fb("cnpj") == "NAO_IDENTIFICADO_CNPJ"
    assert fb("nf") == "NAO_IDENTIFICADO_NF"


def test_update_daily_metrics_returns_dedup_role(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    key = "NF27|07467822000126|validar_produtos|PED001"
    r1 = h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        10,
        "VALIDATION_FAILED",
        11,
        "AGROQUIMICOS",
        ["validar_produtos"],
        [key],
        "p1",
        False,
    )
    r2 = h.update_daily_metrics(
        "2026-05-28",
        "FAILED",
        12,
        "VALIDATION_FAILED",
        11,
        "AGROQUIMICOS",
        ["validar_produtos"],
        [key],
        "p2",
        False,
    )
    assert r1["dedup_role"] == "primary"
    assert r2["dedup_role"] == "duplicate"
    assert r2["primary_process_id"] == "p1"


def test_usoconsumo_dedup_with_nao_identificado_pedido(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    key = "NF27|07467822000126|validar_produtos|NAO_IDENTIFICADO_PEDIDO"
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


def test_decrement_daily_metrics_removes_failure_keys(monkeypatch):
    import update_metrics.handler as h

    fake = _FakeTable()
    monkeypatch.setattr(h, "table", fake)

    key = "NF27|07467822000126|Outros|PED001"
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
