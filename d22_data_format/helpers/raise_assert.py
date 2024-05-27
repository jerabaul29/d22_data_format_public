"""A few helpers, similar to assert, but guaranteed to raise."""

def ras(assertion,  message=""):
    """Raise on failed assertion.
    Input:
        - assertion: the assertion to check for.
        - message: the message to display as the Exception
            content if assertion fails.
    If assertion is True, do nothing. If False, raise an
    Exception with the custom message.
    """
    if not assertion:
        raise Exception(message)
