import logging


def setup_logging(LOGLEVEL: str, LOGFILE: str):
    """
    Configure the root logger with console and file handlers.
    Call this once at application startup.
    """
    # Prevent duplicate handlers if called twice (e.g., during hot reload)
    root = logging.getLogger()
    while root.handlers:
        h = root.handlers.pop()
        h.close()

    logging.basicConfig(
        level=getattr(logging, LOGLEVEL),
        filename=LOGFILE,
        format="%(asctime)s - %(levelname)s - %(name)s - %(funcName)s() - %(message)s",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, LOGLEVEL))
    console_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(funcName)s() - %(message)s"
        )
    )
    logging.getLogger().addHandler(console_handler)


def get_logger(name: str):
    return logging.getLogger(name)
