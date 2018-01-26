// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa_test

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/pilosa/pilosa"
)

// Ensure a bitmap query can be executed.
func TestCache_Rank(t *testing.T) {
	cacheSize := uint32(3)
	cache := pilosa.NewRankCache(cacheSize)
	for i := 1; i < int(2*cacheSize); i++ {
		cache.Add(uint64(i), 3)
	}
	cache.Recalculate()
	if cache.Len() != int(cacheSize) {
		t.Fatalf("unexpected cache Size: %d!=%d expected\n", cache.Len(), cacheSize)
	}
}

// Performs randomized blackbox testing to check for errors in rank cache invalidation.
func TestRankCache_Invalidate(t *testing.T) {
	if err := quick.Check(func(cacheSize uint32, ids, values []uint64) bool {
		cache := pilosa.NewRankCache(cacheSize)
		for i := range ids {
			cache.BulkAdd(ids[i], values[i]%1000)
		}
		cache.Invalidate()

		n := int(cacheSize)
		if len(ids) < n {
			n = len(ids)
		}

		return true
	}, &quick.Config{
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0] = reflect.ValueOf(uint32(rand.Intn(50000) + 25000))

			n := rand.Intn(100000)
			values[1] = reflect.ValueOf(GenerateUint64Slice(n, rand))
			values[2] = reflect.ValueOf(GenerateUint64Slice(n, rand))
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func GenerateUint64Slice(n int, rand *rand.Rand) []uint64 {
	a := make([]uint64, n)
	for i := range a {
		a[i] = rand.Uint64()
	}
	return a
}
