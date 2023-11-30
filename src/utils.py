import os
import logging


def init_logging(
        logger,
        log_file: str,
        console_level=logging.INFO,
        file_level=logging.INFO,
        format: str = '%(asctime)s:%(levelname)s: %(message)s'):
    """
        Simple basic file/console logging.
    """
    base_log_dir = os.path.dirname(log_file)
    os.makedirs(base_log_dir, exist_ok=True)

    formatter = logging.Formatter(format)
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_level)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.setLevel(logging.DEBUG)


def update_settings(main_env: dict, update_with: dict) -> dict:
    """
        Update `main_env` with `update_with`,
        if `update_with` value is a dict, update only keys which are in `main_env`
    """
    env = main_env.copy()
    for k, v in update_with.items():
        if isinstance(v, dict) and k in env:
            env[k].update(v)
            continue
        env[k] = v
    return env


def exists_key(special_format_key_str, dict_inst, return_val=False):
    """ Checks whether a recursive key exists defined in dot format."""
    parts = special_format_key_str.split(".")
    d = dict_inst
    for part in parts:
        if part is None or part not in d:
            return (False, None) if return_val else False
        d = d[part]
    return (True, d) if return_val else True


def set_key(special_format_key_str, value, dict_inst):
    """ Checks whether a recursive key exists defined in dot format."""
    parts = special_format_key_str.split(".")
    d = dict_inst
    for i, part in enumerate(parts):
        if part is None or part not in d:
            return False
        if i != len(parts) - 1:
            d = d[part]
        else:
            d[part] = value
    return True
