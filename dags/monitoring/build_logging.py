import logging

def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        handlers=[
            # logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )