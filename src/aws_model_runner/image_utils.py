from typing import Iterator, Tuple

# TODO: Define a Point type so there is no confusion over the meaning of BBox.
#       (i.e. a two corner box would be (Point, Point) while a UL width height box
#       would be (Point, w, h)


# Pixel coordinate (row, column)
ImageCoord = Tuple[int, int]
# 2D shape (w, h)
ImageDimensions = Tuple[int, int]
# UL corner (row, column) , dimensions (w, h)
ImageRegion = Tuple[ImageCoord, ImageDimensions]
ImageKey = str


def ceildiv(a: int, b: int) -> int:
    """
    Integer ceiling division

    :param a: numerator
    :param b: denominator
    :return: ceil(a/b)
    """
    return -(-a // b)


def next_greater_multiple(n: int, m: int):
    """
    Return the minimum value that is greater than or equal to n that is evenly divisible by m.

    :param n: the input value
    :param m: the multiple
    :return: the minimum multiple of m greater than n
    """
    if n % m == 0:
        return n

    return n + (m - n % m)


def next_greater_power_of_two(n: int):
    """
    Returns the number that is both a power of 2 and greater than or equal to the input parameter.
    For example input 100 returns 128.

    :param n: the input integer
    :return: power of 2 greater than or equal to input
    """

    count = 0

    # First n in the below condition is for the case where n is 0
    # Second condition is only true if n is already a power of 2
    if n and not (n & (n - 1)):
        return n

    while n != 0:
        n >>= 1
        count += 1

    return 1 << count


def generate_crops_for_region(
    region: ImageRegion, chip_size: ImageDimensions, overlap: ImageDimensions
) -> Iterator[ImageRegion]:
    """
    Yields a list of overlapping chip bounding boxes for the given region. Chips will start
    in the upper left corner of the region (i.e. region[0][0], region[0][1]) and will be spaced
    such that they have the specified horizontal and vertical overlap.

    :param region: a tuple for the bounding box of the region ((ul_r, ul_c), (width, height))
    :param chip_size: a tuple for the chip dimensions (width, height)
    :param overlap:  a tuple for the overlap (width, height)
    :return: an iterable list of tuples for the chip bounding boxes [((ul_r, ul_c), (w, h)), ...]
    """
    if overlap[0] >= chip_size[0] or overlap[1] >= chip_size[1]:
        raise ValueError(
            "Overlap must be less than chip size! chip_size = "
            + str(chip_size)
            + " overlap = "
            + str(overlap)
        )

    # Calculate the spacing for the chips taking into account the horizontal and vertical overlap
    # and how many are needed to cover the region
    stride_x = chip_size[0] - overlap[0]
    stride_y = chip_size[1] - overlap[1]
    num_x = ceildiv(region[1][0], stride_x)
    num_y = ceildiv(region[1][1], stride_y)

    for r in range(0, num_y):
        for c in range(0, num_x):
            # Calculate the bounds of the chip ensuring that the chip does not extend
            # beyond the edge of the requested region
            ul_x = region[0][1] + c * stride_x
            ul_y = region[0][0] + r * stride_y
            w = min(chip_size[0], (region[0][1] + region[1][0]) - ul_x)
            h = min(chip_size[1], (region[0][0] + region[1][1]) - ul_y)
            if w > overlap[0] and h > overlap[1]:
                yield ((ul_y, ul_x), (w, h))
