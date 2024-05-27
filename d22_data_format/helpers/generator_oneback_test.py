from d22_data_format.helpers.generator_oneback import GeneratorOneback

def test_oneback():
    generator_oneback = GeneratorOneback(iter(range(5)))
    stop_iteration = False

    res = next(generator_oneback)
    assert res == 0

    res = next(generator_oneback)
    assert res == 1

    generator_oneback.use_last_value()

    res = next(generator_oneback)
    assert res == 1

    res = next(generator_oneback)
    assert res == 2

    res = next(generator_oneback)
    assert res == 3

    res = next(generator_oneback)
    assert res == 4

    try:
        res = next(generator_oneback)
        assert res == 5
    except StopIteration:
        stop_iteration = True

    assert stop_iteration

def test_oneback_2():
    generator_oneback = GeneratorOneback(iter(range(5)))
    stop_iteration = False
    missing_last = False

    try:
        generator_oneback.use_last_value()
    except ValueError as e:
        assert str(e) == "self.last is None; need to have at least called once!"
        missing_last = True
    
    assert missing_last
    missing_last = False

    res = next(generator_oneback)
    assert res == 0

    res = next(generator_oneback)
    assert res == 1

    generator_oneback.use_last_value()

    res = next(generator_oneback)
    assert res == 1

    res = next(generator_oneback)
    assert res == 2

    res = next(generator_oneback)
    assert res == 3

    res = next(generator_oneback)
    assert res == 4

    try:
        res = next(generator_oneback)
        assert res == 5
    except StopIteration:
        stop_iteration = True

    assert stop_iteration

def test_oneback_3():
    generator_oneback = GeneratorOneback(iter(range(5)))
    stop_iteration = False
    missing_last = False
    double_last_error = False

    try:
        generator_oneback.use_last_value()
    except ValueError as e:
        assert str(e) == "self.last is None; need to have at least called once!"
        missing_last = True
    
    assert missing_last
    missing_last = False

    res = next(generator_oneback)
    assert res == 0

    res = next(generator_oneback)
    assert res == 1

    generator_oneback.use_last_value()

    res = next(generator_oneback)
    assert res == 1

    generator_oneback.use_last_value()

    try:
        generator_oneback.use_last_value()
    except ValueError as e:
        assert str(e) == "cannot provide last last, i.e. two previous!"
        double_last_error = True

    assert double_last_error

    res = next(generator_oneback)
    assert res == 1

    res = next(generator_oneback)
    assert res == 2

    res = next(generator_oneback)
    assert res == 3

    res = next(generator_oneback)
    assert res == 4

    try:
        res = next(generator_oneback)
        assert res == 5
    except StopIteration:
        stop_iteration = True

    assert stop_iteration
