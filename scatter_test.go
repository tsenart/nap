package nap

import (
	"fmt"
	"runtime"
	"testing"
)

func TestScatter(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	seq := []int{1, 2, 3, 4, 5, 6, 7, 8}
	err := scatter(len(seq), func(i int) error {
		if seq[i]%2 == 0 {
			seq[i] *= seq[i]
			return nil
		}
		return fmt.Errorf("%d is an odd fellow", seq[i])
	})

	if err == nil {
		t.Fatal("Expected error, got nil")
	}

	want := []int{1, 4, 3, 16, 5, 36, 7, 64}
	for i := range want {
		if want[i] != seq[i] {
			t.Errorf("Wrong value at position %d. Want: %d, Got: %d", i, want[i], seq[i])
		}
	}
}
