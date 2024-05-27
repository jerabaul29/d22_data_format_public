class GeneratorOneback():
    """A generator that usually behaves nicely with the .next method, but that
    also can provide one call to the .back method, in which case, the value that
    was yield latest will be yielded one more time."""

    def __init__(self, generator):
        self.generator = generator
        self.use_last = False
        self.last = None

    def use_last_value(self):
        if self.use_last:
            raise ValueError("cannot provide last last, i.e. two previous!")
        if not self.last:
            raise ValueError("self.last is None; need to have at least called once!")
        else:
            self.use_last = True

    def __next__(self):
        if not self.use_last:
            to_yield = next(self.generator)
            self.last = to_yield
        else:
            to_yield = self.last
            self.use_last = False

        return to_yield

    def __iter__(self):
        return self
