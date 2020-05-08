package avl

import (
	"container/list"
	"sync"
)

type nodeLRU struct {
	sync.Mutex

	size int

	elements map[[16]byte]*list.Element
	access   *list.List
}

type objectInfoNode struct {
	key [16]byte
	obj *node
}

func newNodeLRU(size int) *nodeLRU {
	return &nodeLRU{
		size:     size,
		elements: make(map[[16]byte]*list.Element, size),
		access:   list.New(),
	}
}

func (l *nodeLRU) Load(key [16]byte) (*node, bool) {
	l.Lock()
	defer l.Unlock()

	elem, ok := l.elements[key]
	if !ok {
		return nil, false
	}

	l.access.MoveToFront(elem)

	return elem.Value.(*objectInfoNode).obj, ok
}

func (l *nodeLRU) LoadOrPut(key [16]byte, val *node) (*node, bool) {
	l.Lock()
	defer l.Unlock()

	elem, ok := l.elements[key]

	if ok {
		val = elem.Value.(*objectInfoNode).obj
		l.access.MoveToFront(elem)
	} else {
		l.elements[key] = l.access.PushFront(&objectInfoNode{
			key: key,
			obj: val,
		})
		for len(l.elements) > l.size {
			back := l.access.Back()
			info := back.Value.(*objectInfoNode)
			delete(l.elements, info.key)
			l.access.Remove(back)
		}
	}

	return val, ok
}

func (l *nodeLRU) Put(key [16]byte, val *node) {
	l.Lock()
	defer l.Unlock()

	elem, ok := l.elements[key]

	if ok {
		elem.Value.(*objectInfoNode).obj = val
		l.access.MoveToFront(elem)
	} else {
		l.elements[key] = l.access.PushFront(&objectInfoNode{
			key: key,
			obj: val,
		})
		for len(l.elements) > l.size {
			back := l.access.Back()
			info := back.Value.(*objectInfoNode)
			delete(l.elements, info.key)
			l.access.Remove(back)
		}
	}
}

func (l *nodeLRU) Remove(key [16]byte) {
	l.Lock()
	defer l.Unlock()

	elem, ok := l.elements[key]
	if ok {
		delete(l.elements, key)
		l.access.Remove(elem)
	}
}
