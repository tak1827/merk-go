package merk

import (
	
)

func max(a []uint8) uint8 {
    max := a[0]
    for _, i := range a {
        if i > max {
            max = i
        }
    }
    return max
}
