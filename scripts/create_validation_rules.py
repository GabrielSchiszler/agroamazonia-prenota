import boto3
import os

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('DocumentProcessorTable')

rules = [
    {
        'PK': 'RULES=SEMENTES',
        'SK': 'RULE=validar_numero_nota',
        'rule_name': 'validar_numero_nota',
        'description': 'Valida número da nota fiscal',
        'enabled': True,
        'order': 1
    },
    {
        'PK': 'RULES=SEMENTES',
        'SK': 'RULE=validar_serie',
        'rule_name': 'validar_serie',
        'description': 'Valida série da nota fiscal',
        'enabled': True,
        'order': 2
    },
    {
        'PK': 'RULES=SEMENTES',
        'SK': 'RULE=validar_data_emissao',
        'rule_name': 'validar_data_emissao',
        'description': 'Valida data de emissão',
        'enabled': True,
        'order': 3
    },
    {
        'PK': 'RULES=SEMENTES',
        'SK': 'RULE=validar_cnpj_fornecedor',
        'rule_name': 'validar_cnpj_fornecedor',
        'description': 'Valida CNPJ do fornecedor',
        'enabled': True,
        'order': 4
    },
    {
        'PK': 'RULES=SEMENTES',
        'SK': 'RULE=validar_produtos',
        'rule_name': 'validar_produtos',
        'description': 'Valida produtos (descrição, quantidade, valores)',
        'enabled': True,
        'order': 5
    },
    {
        'PK': 'RULES=SEMENTES',
        'SK': 'RULE=validar_rastreabilidade',
        'rule_name': 'validar_rastreabilidade',
        'description': 'Valida rastreabilidade (lote, validade, fabricação)',
        'enabled': True,
        'order': 6
    },
    {
        'PK': 'RULES=SEMENTES',
        'SK': 'RULE=validar_icms',
        'rule_name': 'validar_icms',
        'description': 'Valida ICMS (interno zerado, interestadual com base)',
        'enabled': True,
        'order': 7
    }
]

print("Criando regras de validação...")
for rule in rules:
    table.put_item(Item=rule)
    print(f"✓ Regra criada: {rule['rule_name']}")

print("\n✅ Todas as regras foram criadas com sucesso!")
