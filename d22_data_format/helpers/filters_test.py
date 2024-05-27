import numpy as np

from d22_data_format.helpers import filters

def test_hampel_1():
    array_in = np.random.randn(128)
    _ = filters.hampel(array_in, 12, t0=3)


def test_interpolate_short_dropouts_1():
    array_in = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
    array_right = np.copy(array_in)
    array_in[2] = np.nan

    array_out = filters.interpolate_short_dropouts(array_in, max_nbr_dropout_points=1)

    assert np.allclose(array_out, array_right)

def test_interpolate_short_dropouts_2():
    array_in = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0])
    array_in[3] = np.nan
    array_in[4] = np.nan
    array_in[5] = np.nan
    array_in[6] = np.nan

    array_out = filters.interpolate_short_dropouts(array_in, max_nbr_dropout_points=1)

    assert np.allclose(array_out, array_in, equal_nan=True)

def test_interpolate_short_dropouts_3():
    array_in = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0])
    array_in[3] = np.nan
    array_in[4] = np.nan
    array_in[5] = np.nan
    array_in[6] = np.nan

    array_out = filters.interpolate_short_dropouts(array_in, max_nbr_dropout_points=2)

    assert np.allclose(array_out, array_in, equal_nan=True)

def test_interpolate_short_dropouts_4():
    array_in = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0])
    array_right = np.copy(array_in)
    array_right[3] = np.nan
    array_right[6] = np.nan
    array_in[3] = np.nan
    array_in[4] = np.nan
    array_in[5] = np.nan
    array_in[6] = np.nan

    array_out = filters.interpolate_short_dropouts(array_in, max_nbr_dropout_points=3)

    assert np.allclose(array_out, array_right, equal_nan=True)


if __name__ == "__main__":
    test_interpolate_short_dropouts_1()
