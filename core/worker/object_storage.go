package worker

import (
	"mini-ray/common"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type ObjectID string

type Object struct {
	Type common.ObjectType
	Data []byte
}

type objectStorage struct {
	lg      *zap.Logger
	pool    *ants.Pool
	objects map[ObjectID]*Object
}

func NewObjectStorage(lg *zap.Logger, pool *ants.Pool) *objectStorage {
	return &objectStorage{lg: lg, pool: pool, objects: make(map[ObjectID]*Object)}
}

func (o *objectStorage) GetObject(objectID ObjectID) *Object {
	if object, ok := o.objects[objectID]; ok {
		return object
	}
	return nil
}

func (o *objectStorage) SetObject(objectID ObjectID, object *Object) {
	o.objects[objectID] = object
}
