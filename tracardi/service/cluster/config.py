import os

from tracardi.service.cluster.settings import GlobalSettings

global_settings = GlobalSettings()
_default_save_logs = os.environ.get('SAVE_LOGS', 'yes').lower() == 'yes'


async def is_save_logs_on() -> bool:
    return await global_settings.get("SAVE_LOGS", _default_save_logs) is True
