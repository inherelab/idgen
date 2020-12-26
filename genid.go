package genid


// IDGenerator interface
type ServerFace interface {
	Init() error
	Serve() error
}

// GeneratorFace interface
type GeneratorFace interface {
	Current() int64
	Next() (int64, error)
}

