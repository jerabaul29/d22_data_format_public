from d22_data_format.helpers.raise_assert import ras


def permutation_to_sort(list_in):
    """return the permutation (list of indexes) that should be used to sort list_in."""

    ras(list_in)

    for crrt_elem in list_in:
        ras(not isinstance(crrt_elem, tuple))

    to_sort = [(list_in[i], i) for i in range(len(list_in))]
    to_sort.sort(key=lambda tup: tup[0])
    permutation = [elem[1] for elem in to_sort]

    return permutation

def slice_list_by_index_list(list_in, index_list):
    list_out = [list_in[ind] for ind in index_list]
    return(list_out)
