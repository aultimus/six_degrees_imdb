package sixdegreesimdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGoalToArr(t *testing.T) {
	a := assert.New(t)

	n := NewNode(&Data{"ncat"})
	n.prev = &Edge{&Data{"tbar"}, nil, nil}
	n.prev.prev = NewNode(&Data{"nfoo"})

	arr := goalToArr(n)
	a.Equal([]string{"nfoo", "tbar", "ncat"}, arr)
}
