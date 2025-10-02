package broker

import "strconv"

func strSliceToUint32(str []string) ([]uint32, error) {
	nums := make([]uint32, 0, len(str))
	for i := range str {
		num, err := strconv.ParseUint(str[i], 10, 32)
		if err != nil {
			return nil, err
		}

		nums = append(nums, uint32(num))
	}

	return nums, nil
}

func strSliceToUint64(str []string) ([]uint64, error) {
	nums := make([]uint64, 0, len(str))
	for i := range str {
		num, err := strconv.ParseUint(str[i], 10, 64)
		if err != nil {
			return nil, err
		}

		nums = append(nums, uint64(num))
	}

	return nums, nil
}
