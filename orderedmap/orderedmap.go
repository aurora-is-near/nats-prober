package orderedmap

type OrderedMap[K comparable, V any] struct {
	elements map[K]*item[K, V]
	first    *item[K, V]
	last     *item[K, V]
}

type item[K comparable, V any] struct {
	key   K
	value V
	prev  *item[K, V]
	next  *item[K, V]
}

func New[K comparable, V any]() *OrderedMap[K, V] {
	return &OrderedMap[K, V]{
		elements: make(map[K]*item[K, V]),
	}
}

func (m *OrderedMap[K, V]) Len() int {
	return len(m.elements)
}

func (m *OrderedMap[K, V]) Get(key K) (V, bool) {
	if item, ok := m.elements[key]; ok {
		return item.value, true
	}
	return *new(V), false
}

func (m *OrderedMap[K, V]) GetFirst() (V, bool) {
	if m.first == nil {
		return *new(V), false
	}
	return m.first.value, true
}

func (m *OrderedMap[K, V]) GetLast() (V, bool) {
	if m.last == nil {
		return *new(V), false
	}
	return m.last.value, true
}

func (m *OrderedMap[K, V]) Pop(key K) (V, bool) {
	item, ok := m.elements[key]
	if !ok {
		return *new(V), false
	}

	if item.prev == nil {
		m.first = item.next
	} else {
		item.prev.next = item.next
	}

	if item.next == nil {
		m.last = item.prev
	} else {
		item.next.prev = item.prev
	}

	delete(m.elements, key)
	return item.value, true
}

func (m *OrderedMap[K, V]) PopFirst() (V, bool) {
	if m.first == nil {
		return *new(V), false
	}
	return m.Pop(m.first.key)
}

func (m *OrderedMap[K, V]) PopLast() (V, bool) {
	if m.last == nil {
		return *new(V), false
	}
	return m.Pop(m.last.key)
}

func (m *OrderedMap[K, V]) PushFirst(key K, value V) {
	m.Pop(key)

	item := &item[K, V]{
		key:   key,
		value: value,
		next:  m.first,
	}
	if m.first != nil {
		m.first.prev = item
	}
	m.first = item

	m.elements[key] = item
}

func (m *OrderedMap[K, V]) PushLast(key K, value V) {
	m.Pop(key)

	item := &item[K, V]{
		key:   key,
		value: value,
		prev:  m.last,
	}
	if m.last != nil {
		m.last.next = item
	}
	m.last = item

	m.elements[key] = item
}
