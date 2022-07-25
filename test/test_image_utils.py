from aws_model_runner.image_utils import (
    generate_crops_for_region,
    next_greater_multiple,
    next_greater_power_of_two,
)


def test_chip_generator():
    chip_list = []
    for chip in generate_crops_for_region(((5, 10), (1024, 1024)), (300, 300), (44, 44)):
        chip_list.append(chip)

    assert len(chip_list) == 16
    assert chip_list[0] == ((5, 10), (300, 300))
    assert chip_list[1] == ((5, 266), (300, 300))
    assert chip_list[3] == ((5, 778), (256, 300))
    assert chip_list[12] == ((773, 10), (300, 256))
    assert chip_list[15] == ((773, 778), (256, 256))

    chip_list = []
    for chip in generate_crops_for_region(((0, 0), (5000, 2500)), (2048, 2048), (0, 0)):
        chip_list.append(chip)

    assert len(chip_list) == 6
    assert chip_list[0] == ((0, 0), (2048, 2048))
    assert chip_list[1] == ((0, 2048), (2048, 2048))
    assert chip_list[2] == ((0, 4096), (904, 2048))
    assert chip_list[3] == ((2048, 0), (2048, 452))
    assert chip_list[4] == ((2048, 2048), (2048, 452))
    assert chip_list[5] == ((2048, 4096), (904, 452))


def test_next_greater_multiple():
    assert 16 == next_greater_multiple(1, 16)
    assert 16 == next_greater_multiple(15, 16)
    assert 16 == next_greater_multiple(16, 16)
    assert 32 == next_greater_multiple(17, 16)
    assert 48 == next_greater_multiple(42, 16)
    assert 64 == next_greater_multiple(50, 16)
    assert 528 == next_greater_multiple(513, 16)


def test_next_greater_power_of_two():
    assert 1 == next_greater_power_of_two(1)
    assert 2 == next_greater_power_of_two(2)
    assert 4 == next_greater_power_of_two(3)
    assert 8 == next_greater_power_of_two(8)
    assert 64 == next_greater_power_of_two(42)
    assert 128 == next_greater_power_of_two(100)
    assert 256 == next_greater_power_of_two(255)
    assert 512 == next_greater_power_of_two(400)
