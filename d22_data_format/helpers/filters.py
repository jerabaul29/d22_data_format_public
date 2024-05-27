import numpy as np
import tqdm

import datetime

from scipy import interpolate
from scipy.signal import butter, sosfiltfilt
from scipy.ndimage.filters import uniform_filter1d

import logging

from d22_data_format.helpers.raise_assert import ras


def hampel(x, k, t0=3, use_tqdm=False):
    '''adapted from hampel function in R package pracma
    x= 1-d numpy array of numbers to be filtered
    k= number of items in (window-1)/2 (# forward and backward wanted to capture in median filter)
    t0= number of standard deviations to use; 3 is default
    '''
    # NOTE: this is adapted from: https://stackoverflow.com/questions/46819260/
    # filtering-outliers-how-to-make-median-based-hampel-function-faster

    ras(isinstance(x, np.ndarray))
    ras(isinstance(k, int))

    y = np.copy(x)  # y is the corrected series

    y = np.squeeze(y)
    ras(len(y.shape) == 1)

    mask_modified = np.zeros((y.shape[0]), dtype=bool)

    n = y.shape[0]

    L = 1.4826

    if use_tqdm:
        wrapper = tqdm.tqdm
    else:
        def nop(it, *a, **k):
            return it
        wrapper = nop

    for i in wrapper(range((k + 1), (n - k))):
        excluding_crrt_point = np.concatenate((
            y[(i - k):(i)],
            y[(i + 1):(i + k + 1)]
        ))

        if np.isnan(excluding_crrt_point).all():
            y[i] = np.nan
            continue

        if np.isnan(y[i]):
            y[i] = np.nan
            continue

        x0 = np.nanmedian(excluding_crrt_point)
        S0 = L * np.nanmedian(np.abs(excluding_crrt_point - x0))

        if (np.abs(y[i] - x0) > t0 * S0):
            y[i] = np.nan
            mask_modified[i] = True

    return (y, mask_modified)


def three_stages_hampel(array_in, datetime_resolution, use_tqdm=True):
    ras(isinstance(array_in, np.ndarray))

    crrt_array = np.copy(array_in)
    crrt_array = np.squeeze(crrt_array)
    ras(len(crrt_array.shape) == 1)

    nbr_points_1_days = int(datetime.timedelta(days=1.0) / datetime_resolution)
    nbr_points_1_hours = int(datetime.timedelta(hours=1.0) / datetime_resolution)

    # perform hampel filtering, 3 steps
    mask_modified = np.zeros((crrt_array.shape[0]), dtype=bool)
    crrt_array, crrt_mask_modified = hampel(crrt_array, k=nbr_points_1_days, t0=5, use_tqdm=use_tqdm)
    mask_modified = np.logical_or(mask_modified, crrt_mask_modified)
    crrt_array, crrt_mask_modified = hampel(crrt_array, k=nbr_points_1_hours, t0=4, use_tqdm=use_tqdm)
    mask_modified = np.logical_or(mask_modified, crrt_mask_modified)
    crrt_array, crrt_mask_modified = hampel(crrt_array, k=nbr_points_1_hours, t0=3, use_tqdm=use_tqdm)
    mask_modified = np.logical_or(mask_modified, crrt_mask_modified)

    return(crrt_array, mask_modified)


def interpolate_short_dropouts(array_in, max_nbr_dropout_points=3, use_tqdm=False):
    crrt_array = np.copy(array_in)
    crrt_array_interpolated = np.copy(array_in)

    # find the points where dropout
    indexes_nan = np.where(np.isnan(crrt_array))[0]

    if max_nbr_dropout_points < 4:
        kind_interp = "linear"
    else:
        kind_interp = "quadratic"

    if use_tqdm:
        wrapper = tqdm.tqdm
    else:
        def nop(it, *a, **k):
            return it
        wrapper = nop

    # if nbr of consecutive points with dropout < max_nbr_dropout_points in both
    # directions, interpolate using 
    for crrt_index in wrapper(indexes_nan):
        if crrt_index >= max_nbr_dropout_points + 1 and crrt_index <= crrt_array.shape[0] - max_nbr_dropout_points - 2:
            if (not np.isnan(crrt_array[crrt_index - max_nbr_dropout_points: crrt_index]).all()) and \
                    (not np.isnan(crrt_array[crrt_index: crrt_index + max_nbr_dropout_points + 1]).all()):
                # build array not nan values and indexes
                list_valid_indexes = []
                list_valid_values = []
                list_invalid_indexes = []

                for crrt_interp_index in range(crrt_index - 2 * max_nbr_dropout_points - 1,
                                               crrt_index - max_nbr_dropout_points - 1):
                    if crrt_interp_index >= 0 and not np.isnan(crrt_array[crrt_interp_index]):
                        list_valid_indexes.append(crrt_interp_index)
                        list_valid_values.append(crrt_array[crrt_interp_index])

                for crrt_interp_index in range(crrt_index - max_nbr_dropout_points - 1,
                                               crrt_index + max_nbr_dropout_points + 1):
                    if not np.isnan(crrt_array[crrt_interp_index]):
                        list_valid_indexes.append(crrt_interp_index)
                        list_valid_values.append(crrt_array[crrt_interp_index])
                    else:
                        list_invalid_indexes.append(crrt_interp_index)

                for crrt_interp_index in range(crrt_index + max_nbr_dropout_points + 1,
                                               crrt_index + 2 * max_nbr_dropout_points + 1):
                    if crrt_interp_index < crrt_array.shape[0] and not np.isnan(crrt_array[crrt_interp_index]):
                        list_valid_indexes.append(crrt_interp_index)
                        list_valid_values.append(crrt_array[crrt_interp_index])

                try:
                    # interpolate
                    interpolator = interpolate.interp1d(np.array(list_valid_indexes),
                                                        np.array(list_valid_values),
                                                        kind=kind_interp)

                    crrt_array_interpolated[crrt_index] = interpolator(np.array([crrt_index]))

                except Exception as e:
                    logging.warning("encountered a problem interpolating index {} of current array".format(crrt_index))
                    logging.warning("exception content:")
                    logging.warning(str(e))

    return crrt_array_interpolated


def butter_bandpass(lowcut, highcut, fs, order=5):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    sos = butter(order, [low, high], analog=False, btype='band', output='sos')

    return sos


def butter_bandpass_filter(data, lowcut, highcut, fs, order=5):
    locations_nan = np.where(np.isnan(data))
    crrt_array = np.copy(data)
    crrt_array[locations_nan] = 0

    sos = butter_bandpass(lowcut, highcut, fs, order=order)
    y = sosfiltfilt(sos, crrt_array)

    y[locations_nan] = np.nan

    return y


def outlier_dropout_processing(array_in, datetime_resolution, use_tqdm=True):
    ras(isinstance(array_in, np.ndarray))
    ras(isinstance(datetime_resolution, datetime.timedelta))
    ras(len(array_in.shape) == 1)

    crrt_array = np.copy(array_in)

    # perform filtering with Hampel three stages
    (filtered, mask_1) = three_stages_hampel(crrt_array, datetime_resolution)

    # NOTE: the steps under are dangerous, as they really modify the data....
    # perform interpolation
    interpolated = interpolate_short_dropouts(filtered, max_nbr_dropout_points=4, use_tqdm=use_tqdm)
    interpolated = interpolate_short_dropouts(interpolated, max_nbr_dropout_points=4, use_tqdm=use_tqdm)

    # perform 1 hampel filtering to make sure interpolation went fine
    nbr_points_1_hours = int(datetime.timedelta(hours=1.0) / datetime_resolution)
    (final_array, mask_2) = hampel(interpolated, k=nbr_points_1_hours, t0=3, use_tqdm=use_tqdm)

    mask_modified = np.logical_or(mask_1, mask_2)

    return (final_array, mask_modified)


def nan_helper(y):
    """Helper to handle indices and logical indices of NaNs.

    Input:
        - y, 1d numpy array with possible NaNs
    Output:
        - nans, logical indices of NaNs
        - index, a function, with signature indices= index(logical_indices),
          to convert logical indices of NaNs to 'equivalent' indices
    Example:
        >>> # linear interpolation of NaNs
        >>> nans, x= nan_helper(y)
        >>> y[nans]= np.interp(x(nans), x(~nans), y[~nans])
    """

    return np.isnan(y), lambda z: z.nonzero()[0]


def running_average(array_in, datetime_resolution, timedelta_total_averaging_width):
    ras(isinstance(array_in, np.ndarray))
    ras(isinstance(datetime_resolution, datetime.timedelta))
    ras(isinstance(timedelta_total_averaging_width, datetime.timedelta))

    crrt_array = np.copy(array_in)

    if np.isnan(crrt_array).any():
        logging.warning("call running_average with an array containing some NaNs;")
        logging.warning("replace them by interpolation to avoid nan output")
        nans, x = nan_helper(crrt_array)
        crrt_array[nans] = np.interp(x(nans), x(~nans), crrt_array[~nans])

    nbr_points_width = int(timedelta_total_averaging_width / datetime_resolution)
    result = uniform_filter1d(crrt_array, size=nbr_points_width)

    return(result)
