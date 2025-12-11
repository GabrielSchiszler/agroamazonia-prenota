import importlib
import logging

logger = logging.getLogger()

def load_rule(rule_name):
    """Carrega regra dinamicamente"""
    try:
        module = importlib.import_module(f'rules.{rule_name}')
        return module.validate
    except Exception as e:
        logger.error(f"Failed to load rule {rule_name}: {str(e)}")
        return None
