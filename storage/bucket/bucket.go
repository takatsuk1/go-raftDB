package bucket

import (
	"go-raft/storage"
)

var (
	keyBucketName = []byte("key")

	testBucketName = []byte("test")
)

var (
	Key = storage.Bucket(bucket{id: 1, name: keyBucketName, safeRangeBucket: true})

	Test = storage.Bucket(bucket{id: 100, name: testBucketName, safeRangeBucket: false})
)

type bucket struct {
	id              storage.BucketID
	name            []byte
	safeRangeBucket bool
}

func (b bucket) ID() storage.BucketID    { return b.id }
func (b bucket) Name() []byte            { return b.name }
func (b bucket) String() string          { return string(b.Name()) }
func (b bucket) IsSafeRangeBucket() bool { return b.safeRangeBucket }
