package pan

import (
	"testing"
	"testing/quick"
)

func TestSlave(t *testing.T) {
	db := &DB{}
	last := -1

	err := quick.Check(func(n int) bool {
		index := db.slave(n)
		if n <= 1 {
			return index == 0
		}

		result := index > 0 && index < n && index != last
		last = index

		return result
	}, nil)

	if err != nil {
		t.Error(err)
	}
}
