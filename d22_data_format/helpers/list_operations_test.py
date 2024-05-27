from d22_data_format.helpers.list_operations import permutation_to_sort, slice_list_by_index_list


def test_1():
    list_in = [3, 1, 2]
    right_permutation = [1, 2, 0]
    assert permutation_to_sort(list_in) == right_permutation

def test_2():
    list_in = [0, 1, 2]
    right_permutation = [0, 1, 2]
    assert permutation_to_sort(list_in) == right_permutation

def test_3():
    list_in = [3, 1, 2]
    right_permutation = [1, 2, 0]
    list_out = [1, 2, 3]
    assert slice_list_by_index_list(list_in, right_permutation) == list_out
