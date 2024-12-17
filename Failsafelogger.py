import logging

class SafeLogger(logging.Logger):
    """
    Custom Logger that ensures all handlers are invoked safely.
    If a handler fails, it logs the failure and continues with the next handler.
    """

    def handle(self, record):
        """
        Overridden handle method to safely emit records to each handler.
        If one handler fails, it logs the failure and continues with others.
        """
        for handler in self.handlers:
            try:
                handler.emit(record)
            except Exception as e:
                # Log handler failure to stderr
                print(f"Handler {handler} failed: {e}")

def setup_logging():
    # Use the custom SafeLogger class
    logging.setLoggerClass(SafeLogger)
    logger = logging.getLogger("safe_logger")
    logger.setLevel(logging.DEBUG)

    # Create multiple handlers
    handler_1 = logging.FileHandler("log1.log")
    handler_1.setLevel(logging.DEBUG)
    handler_2 = logging.FileHandler("log2.log")
    handler_2.setLevel(logging.DEBUG)

    # Example of a handler that will raise an exception
    class FailingHandler(logging.Handler):
        def emit(self, record):
            raise ValueError("Simulated handler failure")

    handler_3 = FailingHandler()

    # Add handlers to the logger
    logger.addHandler(handler_1)
    logger.addHandler(handler_2)
    logger.addHandler(handler_3)

    return logger

if __name__ == "__main__":
    logger = setup_logging()

    # Log some messages
    logger.info("This is a test log message.")
    logger.warning("This is a warning message.")
