package httpsrv

// ValueGet struct
type ValueGet struct {
	Name string `json:"name"`
}

// ValueSet struct
type ValueSet struct {
	Name  string `json:"name" validate:"required|min_len:2"`
	Value int64  `json:"value" validate:"required|min:1"`
	Force bool   `json:"force"`
}

// MultiSet struct
type MultiSet struct {
	Force  bool        `json:"force"`
	Values []*ValueSet `json:"values" validate:"required|min:1"`
}
