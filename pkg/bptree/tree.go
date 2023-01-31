package bptree

import (
	"bytes"
	"sync"
)

type innerNode struct {
	pointers []Node
	keys [][]byte
	capacity int
}

func newNode(order int) *innerNode {
	return &innerNode{capacity: order}
}

func (n *innerNode) len() int {
	return len(n.pointers)
}

func (n *innerNode) isHalfFull() bool {
	return n.len() >= (n.capacity + 1) / 2
}

func (n *innerNode) lookup(key []byte) (int, Node) {
	for i, k := range n.keys {
		switch bytes.Compare(key, k) {
		case 0:
			return i + 1, n.pointers[i + 1]
		case -1:
			return i, n.pointers[i]
		}
	}
	return len(n.keys), n.pointers[len(n.keys)]
}

func (n *innerNode) split(splitKey []byte, child Node, insertAt int) ([]byte, Node, bool) {
	nn := newNode(n.capacity)
	splitAt := n.len()/2
	if insertAt > splitAt {
		splitAt += 1;
	}
	nn.pointers = append(nn.pointers, n.pointers[splitAt:]...)
	n.pointers = n.pointers[:splitAt]
	nn.keys = append(nn.keys, n.keys[splitAt-1:]...)
	n.keys = n.keys[:splitAt-1]

	if n.len() > nn.len() {
		nn.pointers = insert(nn.pointers, insertAt - n.len(), child)
		nn.keys = insert(nn.keys, insertAt - n.len(), splitKey)
	} else {
		n.keys = insert(n.keys, insertAt - 1, splitKey)
		n.pointers = insert(n.pointers, insertAt, child)
	}

	splitKey, nn.keys = nn.keys[0], nn.keys[1:]

	return splitKey, nn, true
} 

func (n *innerNode) set(key []byte, value []byte) ([]byte, Node, bool) {
	pos, child := n.lookup(key)
	splitKey, leaf, split := child.set(key, value)
	if split {
		insertAt := pos + 1
		if n.len() < n.capacity {
			n.keys = insert(n.keys, insertAt - 1, splitKey)
			n.pointers = insert(n.pointers, insertAt, leaf)
			return nil, nil, false
		} 
		return n.split(splitKey, leaf, insertAt)
	}
	return nil, nil, false
}

func (n *innerNode) delete(key []byte) {
	pointerPos, child := n.lookup(key)
	child.delete(key)

	if child.isHalfFull() || n.len() == 1 {
		return
	}
	
	// Get child siblings
	var lSib, rSib Node
	if pointerPos > 0 {
		lSib = n.pointers[pointerPos - 1]
	}
	if pointerPos < n.len() - 1 {
		rSib = n.pointers[pointerPos + 1]
	}

	// Rotate right
	if lSib != nil && lSib.len() > (n.capacity + 1) / 2 {
		n.rotateRight(child, lSib, pointerPos)
	// Rotate Left
	} else if rSib != nil && rSib.len() > (n.capacity + 1) / 2 {
		n.rotateLeft(child, rSib, pointerPos)
	// Merge Left
	} else if lSib != nil && lSib.len() + child.len() <= n.capacity {
		n.merge(lSib, child, pointerPos - 1)
	// Merge Right
	} else if rSib != nil && rSib.len() + child.len() <= n.capacity {
		n.merge(child, rSib, pointerPos )
	} else {
		panic("UNREACHABLE")
	}
}

func (n *innerNode) merge(child Node, sibling Node, keyPos int) {
	pointerPos := keyPos + 1

	switch s := sibling.(type) {
	case *leaf:
		c := child.(*leaf)
		
		n.keys = append(n.keys[:keyPos], n.keys[keyPos+1:]...)
		n.pointers = append(n.pointers[:pointerPos], n.pointers[pointerPos+1:]...)

		c.tuples = append(c.tuples, s.tuples...)
		c.rightLeaf = s.rightLeaf
	case *innerNode:
		c := child.(*innerNode)

		parentKey := n.keys[keyPos]
		n.keys = append(n.keys[:keyPos], n.keys[keyPos+1:]...)
		n.pointers = append(n.pointers[:pointerPos], n.pointers[pointerPos+1:]...)

		c.keys = append(c.keys, parentKey)
		c.keys = append(c.keys, s.keys...)
		c.pointers = append(c.pointers, s.pointers...)
	}
}

func (n *innerNode) rotateLeft(child Node, sibling Node, keyPos int) {
	var splitKey []byte

	switch s := sibling.(type) {
	case *leaf:
		c := child.(*leaf)
		c.tuples = append(c.tuples, s.tuples[0])
		s.tuples = s.tuples[1:]
		splitKey = s.tuples[0].key
		n.keys[keyPos] = splitKey
	case *innerNode:
		c := child.(*innerNode)
		splitKey = n.keys[keyPos]
		n.keys[keyPos] = s.keys[0]
		c.pointers = append(c.pointers, s.pointers[0])
		s.pointers = s.pointers[1:]
		c.keys = append(c.keys, splitKey)
		s.keys = s.keys[1:]
	}
}

func (n *innerNode) rotateRight(child Node, sibling Node, keyPos int) {
	var splitKey []byte

	switch s := sibling.(type) {
	case *leaf:
		c := child.(*leaf)
		c.tuples = insert(c.tuples, 0, s.tuples[s.len() - 1])
		s.tuples = s.tuples[:s.len() - 1]
		splitKey = c.tuples[0].key
		n.keys[keyPos - 1] = splitKey
	case *innerNode:
		c := child.(*innerNode)
		splitKey = n.keys[keyPos - 1]
		n.keys[keyPos - 1] = s.keys[0]
		c.pointers = insert(c.pointers, 0, s.pointers[s.len() - 1])
		s.pointers = s.pointers[:s.len() - 1]
		c.keys = insert(c.keys, 0, splitKey)
		s.keys = s.keys[:s.len() - 1]
	}
}

func (n *innerNode) get(key []byte) ([]byte, bool) {
	_, child := n.lookup(key)
	return child.get(key)
}

func (n *innerNode) scan(start []byte, end []byte) ([][]byte) {
	_, child := n.lookup(start)
	return child.scan(start, end)
}

type Node interface {
	len() (int)
	get(key []byte) ([]byte, bool)
	delete(key []byte)
	scan(start []byte, end []byte) ([][]byte)
	set(key []byte, value []byte) ([]byte, Node, bool)
	isHalfFull() bool
}

type leaf struct {
	tuples []tuple
	capacity int
	rightLeaf *leaf
}

type tuple struct {
	key []byte
	value []byte
}

func newLeaf(order int) *leaf {
	return &leaf{capacity: order}
}

func (l *leaf) len() int {
	return len(l.tuples)
}

func (l *leaf) isHalfFull() bool {
	return l.len() >= (l.capacity + 1) / 2
}

func (l *leaf) get(key []byte) ([]byte, bool) {
	for _, t := range l.tuples {
		if bytes.Compare(t.key, key) == 0 {
			return t.value, true
		}
	}
	return nil, false
}

func (l *leaf) delete(key []byte) {
	for i, t := range l.tuples {
		if bytes.Compare(t.key, key) == 0 {
			l.tuples = append(l.tuples[:i], l.tuples[i+1:]...)
			return
		}
	}
}

func (l *leaf) scan(start []byte, end []byte) ([][]byte) {
	// TODO: Consider TCO this func
	var result [][]byte
	for _, t := range l.tuples {
		switch bytes.Compare(t.key, start) {
		case 0,1:
			switch bytes.Compare(t.key, end) {
			case -1, 0: 
				result = append(result, t.value)
			case 1:
				return result
			}
		}
	}
	// Additional hop is made in case end key is the bigger key in the leaf, but we save a check
	// for all leaves.
	if l.rightLeaf != nil {
		result = append(result, l.rightLeaf.scan(start, end)...)
	}

	return result
}

func (l *leaf) set(key []byte, value []byte) ([]byte, Node, bool) {
	// TODO: Clean method
	record := tuple{key, value}

	// TODO: move insert logic to separate lookup method
	insertAt := l.len()
	for i, t := range l.tuples {
		switch bytes.Compare(t.key, key) {
			case 0: 
			l.tuples[i] = record 
			return nil, nil, false
			case 1: 
			insertAt = i
			break
		}
	}

	if l.len() < l.capacity {
		l.tuples = insert(l.tuples, insertAt, record)
		return nil, nil, false 
	}

	nl := newLeaf(l.capacity)
	splitAt := l.len()/2
	if insertAt >= splitAt {
		splitAt += 1;
	}
	nl.tuples = append(nl.tuples, l.tuples[splitAt:]...)
	l.tuples = l.tuples[:splitAt]

	if insertAt >= l.len() {
		nl.tuples = insert(nl.tuples, insertAt - l.len(), record)
	} else {
		l.tuples = insert(l.tuples, insertAt, record)
	}

	nl.rightLeaf = l.rightLeaf
	l.rightLeaf = nl

	return nl.tuples[0].key, nl, true
}

type Tree struct {
	root Node 
	order int
	mu sync.Mutex
}

func NewTree(order int) *Tree {
	rn := &leaf{
		capacity: order,
	}
	return &Tree{root: rn, order: order}
}

func (t *Tree) Set(key []byte, value []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var splitKey []byte
	var child Node 
	var split bool

	splitKey, child, split = t.root.set(key, value)

	if !split {
		return
	}

	rn := newNode(t.order)
	rn.keys = append(rn.keys, splitKey)
	rn.pointers = append(rn.pointers, t.root)
	rn.pointers = append(rn.pointers, child)
	t.root = rn
}

func (t *Tree) Get(key []byte) ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.root.get(key)
}

func (t *Tree) Delete(key []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.root.delete(key)
	if t.root.len() == 1 {
		n, ok := t.root.(*innerNode)
		if ok {
			t.root = n.pointers[0]
		}
	}
}

func (t *Tree) Scan(start []byte, end []byte) [][]byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.root.scan(start, end)
}

