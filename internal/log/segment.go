package log

type segment struct{
	store *store
	index *index
}

// segment whihch wraps index and store
func newSegment(dir string) (*segment, error){
	s := &segment{

	}

	s.store = newStore()
	s.index = newIndex()

	return s, nil
}